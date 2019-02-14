package com.orbitz.consul.cache;

import com.google.common.base.Function;
import com.google.common.net.HostAndPort;
import com.orbitz.consul.HealthClient;
import com.orbitz.consul.async.ConsulResponseCallback;
import com.orbitz.consul.model.health.ServiceHealth;
import com.orbitz.consul.option.QueryOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.util.List;

public class ServiceHealthCache extends ConsulCache<ServiceHealthKey, ServiceHealth> {
    private final static Logger LOGGER = LoggerFactory.getLogger(ServiceHealthCache.class);

    private ServiceHealthCache(Function<ServiceHealth, ServiceHealthKey> keyConversion, CallbackConsumer<ServiceHealth> callbackConsumer) {
        super(keyConversion, callbackConsumer);
    }

    private ServiceHealthCache(Function<ServiceHealth, ServiceHealthKey> keyConversion, CallbackConsumer<ServiceHealth> callbackConsumer, String serviceName) {
        super(keyConversion, callbackConsumer);
        this.setServiceName(serviceName);
    }

    /**
     * Factory method to construct a string/{@link ServiceHealth} map for a particular service.
     * <p/>
     * Keys will be a {@link HostAndPort} object made up of the service's address/port combo
     *
     * @param healthClient the {@link HealthClient}
     * @param serviceName  the name of the service
     * @param passing      include only passing services?
     * @return a cache object
     */
    public static ServiceHealthCache newCache(
            final HealthClient healthClient,
            final String serviceName,
            final boolean passing,
            final int watchSeconds,
            final QueryOptions queryOptions,
            final Function<ServiceHealth, ServiceHealthKey> keyExtractor) {
        CallbackConsumer<ServiceHealth> callbackConsumer = new CallbackConsumer<ServiceHealth>() {
            @Override
            public void consume(BigInteger index, ConsulResponseCallback<List<ServiceHealth>> callback) {
                LOGGER.info("DEBUG_CONSUL_LOG ServiceHealthCache consume serviceName:{}, index:{}", serviceName, index);
                QueryOptions params = watchParams(index, watchSeconds, queryOptions);
                if (passing) {
                    healthClient.getHealthyServiceInstances(serviceName, params, callback);
                } else {
                    healthClient.getAllServiceInstances(serviceName, params, callback);
                }
            }
        };

        return new ServiceHealthCache(keyExtractor, callbackConsumer, serviceName);
    }

    public static ServiceHealthCache newCache(
            final HealthClient healthClient,
            final String serviceName,
            final boolean passing,
            final int watchSeconds,
            final QueryOptions queryOptions) {

        Function<ServiceHealth, ServiceHealthKey> keyExtractor = new Function<ServiceHealth, ServiceHealthKey>() {
            @Override
            public ServiceHealthKey apply(ServiceHealth input) {
                return ServiceHealthKey.fromServiceHealth(input);
            }
        };

        return newCache(healthClient, serviceName, passing, watchSeconds, queryOptions, keyExtractor);
    }
    
    public static ServiceHealthCache newCache(
            final HealthClient healthClient,
            final String serviceName,
            final boolean passing,
            final QueryOptions queryOptions,
            final int watchSeconds) {
        return newCache(healthClient, serviceName, passing, watchSeconds, queryOptions);
    }

    public static ServiceHealthCache newCache(final HealthClient healthClient, final String serviceName) {
        return newCache(healthClient, serviceName, true, QueryOptions.BLANK, 10);
    }
}
