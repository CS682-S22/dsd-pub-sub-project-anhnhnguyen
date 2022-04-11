package project2.zookeeper;

import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.curator.x.discovery.ServiceInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import project2.Constants;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

/**
 * Class to find broker(s).
 *
 * @author anhnguyen
 * <p>
 * Reference: http://blog.palominolabs.com/2012/08/14/using-netflix-curator-for-service-discovery/index.html
 */
public class BrokerFinder {
    /**
     * logger object.
     */
    private final Logger LOGGER = LoggerFactory.getLogger("operation");
    /**
     * service discovery.
     */
    private final ServiceDiscovery<BrokerMetadata> discovery;

    /**
     * Constructor.
     *
     * @param curatorFramework          curator framework
     * @param instanceSerializerFactory instance serializer factory
     */
    public BrokerFinder(CuratorFramework curatorFramework, InstanceSerializerFactory instanceSerializerFactory) {
        this.discovery = ServiceDiscoveryBuilder.builder(BrokerMetadata.class)
                .basePath(Constants.BASE_PATH)
                .client(curatorFramework)
                .serializer(instanceSerializerFactory.getInstanceSerializer(new TypeReference<>() {
                }))
                .build();
        try {
            this.discovery.start();
        } catch (Exception e) {
            LOGGER.error("BrokerFinder: " + e.getMessage());
        }
    }

    /**
     * Method to get list of brokers based on service name and key.
     *
     * @param serviceName service name
     * @return list of brokers
     */
    public Collection<BrokerMetadata> getBrokers(String serviceName) {
        Collection<BrokerMetadata> brokers = new ArrayList<>();
        try {
            for (ServiceInstance<BrokerMetadata> instance : discovery.queryForInstances(serviceName)) {
                brokers.add(instance.getPayload());
            }
        } catch (Exception e) {
            LOGGER.error("getBrokers(): " + e.getMessage());
        }
        return brokers;
    }

    /**
     * Method to close service discovery when done.
     */
    public void close() {
        try {
            discovery.close();
        } catch (IOException e) {
            LOGGER.error("close(): " + e.getMessage());
        }
    }
}
