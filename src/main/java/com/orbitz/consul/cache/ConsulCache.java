package com.orbitz.consul.cache;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.orbitz.consul.ConsulException;
import com.orbitz.consul.async.ConsulResponseCallback;
import com.orbitz.consul.model.ConsulResponse;
import com.orbitz.consul.option.ImmutableQueryOptions;
import com.orbitz.consul.option.QueryOptions;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

/**
 * A cache structure that can provide an up-to-date read-only
 * map backed by consul data
 *
 * @param <V>
 */
public class ConsulCache<K, V> {

    enum State {latent, starting, started, stopped }

    private final static Logger LOGGER = LoggerFactory.getLogger(ConsulCache.class);

    @VisibleForTesting
    static final String BACKOFF_DELAY_PROPERTY = "com.orbitz.consul.cache.backOffDelay";
    private static final long BACKOFF_DELAY_QTY_IN_MS = getBackOffDelayInMs(System.getProperties());

    private final AtomicReference<BigInteger> latestIndex = new AtomicReference<BigInteger>(null);
    private final AtomicLong lastContact = new AtomicLong();
    private final AtomicBoolean isKnownLeader = new AtomicBoolean();
    private final AtomicReference<ImmutableMap<K, V>> lastResponse = new AtomicReference<ImmutableMap<K, V>>(null);
    private final AtomicReference<State> state = new AtomicReference<State>(State.latent);
    private final CountDownLatch initLatch = new CountDownLatch(1);
    private final ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder().setDaemon(true).build());
    private final CopyOnWriteArrayList<Listener<K, V>> listeners = new CopyOnWriteArrayList<Listener<K, V>>();
    private final ReentrantLock listenersStartingLock = new ReentrantLock();

    private final Function<V, K> keyConversion;
    private final CallbackConsumer<V> callBackConsumer;
    private final ConsulResponseCallback<List<V>> responseCallback;
    private String serviceName;
    private long lastUpdateIndexTimestamp = 0;
    private ScheduledExecutorService checkStateExecutorService = new ScheduledThreadPoolExecutor(1,
            new BasicThreadFactory.Builder().namingPattern("check-circle-status-schedule-pool-%d").daemon(true).build());

    ConsulCache(
            Function<V, K> keyConversion,
            CallbackConsumer<V> callbackConsumer) {

        this.keyConversion = keyConversion;
        this.callBackConsumer = callbackConsumer;

        this.responseCallback = new ConsulResponseCallback<List<V>>() {
            @Override
            public void onComplete(ConsulResponse<List<V>> consulResponse) {
                LOGGER.info("DEBUG_CONSUL_LOG service:{} callback onComplete", serviceName);
                if (consulResponse.isKnownLeader()) {
                    if (!isRunning()) {
                        LOGGER.error("DEBUG_CONSUL_LOG service:{} callback onComplete isNotRunning", serviceName);
                        return;
                    }
                    updateIndex(consulResponse);
                    LOGGER.info("DEBUG_CONSUL_LOG service:{} callback onComplete updated (index={})", serviceName, latestIndex);

                    ImmutableMap<K, V> full = convertToMap(consulResponse);

                    boolean changed = !full.equals(lastResponse.get());
                    LOGGER.info("DEBUG_CONSUL_LOG service:{} callback onComplete, changed:{}", serviceName, changed);
                    if (changed) {
                        LOGGER.info("DEBUG_CONSUL_LOG service:{} callback onComplete changed, set response:{}, lastContact:{}", serviceName, full, consulResponse.getLastContact());
                        // changes
                        lastResponse.set(full);
                        // metadata changes
                        lastContact.set(consulResponse.getLastContact());
                        isKnownLeader.set(consulResponse.isKnownLeader());
                    }

                    if (changed) {
                        LOGGER.info("DEBUG_CONSUL_LOG service:{} callback onComplete changed, notify listener", serviceName);
                        Boolean locked = false;
                        if (state.get() == State.starting) {
                            listenersStartingLock.lock();
                            locked = true;
                        }
                        try {
                            for (Listener<K, V> l : listeners) {
                                l.notify(full);
                            }
                        }
                        finally {
                            if (locked) {
                                listenersStartingLock.unlock();
                            }
                        }
                    }

                    if (state.compareAndSet(State.starting, State.started)) {
                        initLatch.countDown();
                    }
                    LOGGER.info("DEBUG_CONSUL_LOG service:{} callback onComplete success, start new runCallback", serviceName);
                    runCallback();
                } else {
                    LOGGER.error("DEBUG_CONSUL_LOG service:{} callback onComplete Consul cluster has no elected leader", serviceName);
                    onFailure(new ConsulException("Consul cluster has no elected leader"));
                }
            }

            @Override
            public void onFailure(Throwable throwable) {
                LOGGER.error("DEBUG_CONSUL_LOG service:{} callback onFailure", serviceName);
                if (!isRunning()) {
                    LOGGER.error("DEBUG_CONSUL_LOG service:{} callback onFailure isNotRuning", serviceName);
                    return;
                }
                LOGGER.error("DEBUG_CONSUL_LOG service:{} callback onFailure Error getting response from consul. will retry in {} {}", serviceName, BACKOFF_DELAY_QTY_IN_MS, TimeUnit.MILLISECONDS, throwable);

                executorService.schedule(new Runnable() {
                    @Override
                    public void run() {
                        LOGGER.info("DEBUG_CONSUL_LOG service:{} callback onFailure schedule runCallback", serviceName);
                        runCallback();
                    }
                }, BACKOFF_DELAY_QTY_IN_MS, TimeUnit.MILLISECONDS);
            }
        };
        checkStateExecutorService.scheduleWithFixedDelay(()->{
            if (lastUpdateIndexTimestamp > 0 && lastUpdateIndexTimestamp < System.currentTimeMillis() - 60000) {
                LOGGER.error("DEBUG_CONSUL_LOG service:{} stop update lastIndex for {}s", serviceName, (System.currentTimeMillis() - lastUpdateIndexTimestamp) / 1000);
            }
        }, 60, 60, TimeUnit.SECONDS);
    }

    @VisibleForTesting
    static long getBackOffDelayInMs(Properties properties) {
        String backOffDelay = null;
        try {
            backOffDelay = properties.getProperty(BACKOFF_DELAY_PROPERTY);
            if (!Strings.isNullOrEmpty(backOffDelay)) {
                return Long.parseLong(backOffDelay);
            }
        } catch (Exception ex) {
            LOGGER.warn(backOffDelay != null ?
                    String.format("Error parsing property variable %s: %s", BACKOFF_DELAY_PROPERTY, backOffDelay) :
                    String.format("Error extracting property variable %s", BACKOFF_DELAY_PROPERTY),
                    ex);
        }
        return TimeUnit.SECONDS.toMillis(10);
    }

    public void start() throws Exception {
        LOGGER.info("DEBUG_CONSUL_LOG service:{} start", serviceName);
        checkState(state.compareAndSet(State.latent, State.starting),"Cannot transition from state %s to %s", state.get(), State.starting);
        runCallback();
    }

    public void stop() throws Exception {
        LOGGER.info("DEBUG_CONSUL_LOG service:{} stop", serviceName);
        State previous = state.getAndSet(State.stopped);
        if (previous != State.stopped) {
            executorService.shutdownNow();
        }
    }

    private void runCallback() {
        if (isRunning()) {
            LOGGER.info("DEBUG_CONSUL_LOG service:{} runCallback running", serviceName);
            callBackConsumer.consume(latestIndex.get(), responseCallback);
        } else {
            LOGGER.error("DEBUG_CONSUL_LOG service:{} runCallback stopped", serviceName);
        }
    }

    private boolean isRunning() {
        return state.get() == State.started || state.get() == State.starting;
    }

    public boolean awaitInitialized(long timeout, TimeUnit unit) throws InterruptedException {
        return initLatch.await(timeout, unit);
    }

    public ImmutableMap<K, V> getMap() {
        return lastResponse.get();
    }

    public ConsulResponse<ImmutableMap<K,V>> getMapWithMetadata() {
        return new ConsulResponse<>(lastResponse.get(), lastContact.get(), isKnownLeader.get(), latestIndex.get());
    }

    @VisibleForTesting
    ImmutableMap<K, V> convertToMap(final ConsulResponse<List<V>> response) {
        if (response == null || response.getResponse() == null || response.getResponse().isEmpty()) {
            return ImmutableMap.of();
        }
        final ImmutableMap.Builder<K, V> builder = ImmutableMap.builder();
        final Set<K> keySet = new HashSet<>();
        for (final V v : response.getResponse()) {
            final K key = keyConversion.apply(v);
            if (key != null) {
                if (!keySet.contains(key)) {
                    builder.put(key, v);
                } else {
                    LOGGER.warn("Duplicate service encountered. May differ by tags. Try using more specific tags? " + key.toString());
                }
            }
            keySet.add(key);
        }
        return builder.build();
    }

    private void updateIndex(ConsulResponse<List<V>> consulResponse) {
        if (consulResponse == null) {
            LOGGER.error("DEBUG_CONSUL_LOG service:{} updateIndex consulResponse is null", serviceName);
        } else if (consulResponse.getIndex() == null) {
            LOGGER.error("DEBUG_CONSUL_LOG service:{} updateIndex consulResponse index is null", serviceName);
        }
        if (consulResponse != null && consulResponse.getIndex() != null) {
            this.latestIndex.set(consulResponse.getIndex());
            LOGGER.info("DEBUG_CONSUL_LOG service:{} updateIndex index:{}", serviceName, consulResponse.getIndex());
            lastUpdateIndexTimestamp = System.currentTimeMillis();
        }
    }

    protected static QueryOptions watchParams(final BigInteger index, final int blockSeconds,
                                              QueryOptions queryOptions) {
        checkArgument(!queryOptions.getIndex().isPresent() && !queryOptions.getWait().isPresent(),
                "Index and wait cannot be overridden");

        ImmutableQueryOptions.Builder builder =  ImmutableQueryOptions.builder()
                .from(watchDefaultParams(index, blockSeconds))
                .token(queryOptions.getToken())
                .consistencyMode(queryOptions.getConsistencyMode())
                .near(queryOptions.getNear())
                .datacenter(queryOptions.getDatacenter());
        for (String tag : queryOptions.getTag()) {
            builder.addTag(tag);
        }
        return builder.build();
    }

    private static QueryOptions watchDefaultParams(final BigInteger index, final int blockSeconds) {
        if (index == null) {
            return QueryOptions.BLANK;
        } else {
            return QueryOptions.blockSeconds(blockSeconds, index).build();
        }
    }

    /**
     * passed in by creators to vary the content of the cached values
     *
     * @param <V>
     */
    protected interface CallbackConsumer<V> {
        void consume(BigInteger index, ConsulResponseCallback<List<V>> callback);
    }

    /**
     * Implementers can register a listener to receive
     * a new map when it changes
     *
     * @param <V>
     */
    public interface Listener<K, V> {
        void notify(Map<K, V> newValues);
    }

    public boolean addListener(Listener<K, V> listener) {
        Boolean locked = false;
        boolean added;
        if (state.get() == State.starting) {
            listenersStartingLock.lock();
            locked = true;
        }
        try {
            added = listeners.add(listener);
            if (state.get() == State.started) {
                listener.notify(lastResponse.get());
            }
        }
        finally {
            if (locked) {
                listenersStartingLock.unlock();
            }
        }
        return added;
    }

    public List<Listener<K, V>> getListeners() {
        return Collections.unmodifiableList(listeners);
    }

    public boolean removeListener(Listener<K, V> listener) {
        return listeners.remove(listener);
    }

    @VisibleForTesting
    protected State getState() {
        return state.get();
    }

    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }

    public String getServiceName() {
        return this.serviceName;
    }
}
