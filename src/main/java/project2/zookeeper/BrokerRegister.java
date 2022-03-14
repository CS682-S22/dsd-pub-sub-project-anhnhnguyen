package project2.zookeeper;

import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.details.InstanceSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import project2.Constants;

/**
 * Class to register broker.
 *
 * @author anhnguyen
 * <p>
 * Reference: http://blog.palominolabs.com/2012/08/14/using-netflix-curator-for-service-discovery/index.html
 */
public class BrokerRegister {
    /**
     * logger object.
     */
    private final Logger LOGGER = LoggerFactory.getLogger(BrokerRegister.class);
    /**
     * curator framework.
     */
    private final CuratorFramework curatorFramework;
    /**
     * instance serializer.
     */
    private final InstanceSerializer<BrokerMetadata> jacksonInstanceSerializer;
    /**
     * service name.
     */
    private final String serviceName;
    /**
     * listening address.
     */
    private final String listenAddress;
    /**
     * listening port.
     */
    private final int listenPort;
    /**
     * partition number.
     */
    private final int partition;
    /**
     * service discovery.
     */
    private ServiceDiscovery<BrokerMetadata> discovery;

    /**
     * Constructor.
     *
     * @param curatorFramework          curator framework
     * @param instanceSerializerFactory instance serializer factory
     * @param serviceName               service name
     * @param listenAddress             listening address
     * @param listenPort                listening port
     * @param partition                 partition
     */
    public BrokerRegister(CuratorFramework curatorFramework, InstanceSerializerFactory instanceSerializerFactory,
                          String serviceName, String listenAddress, int listenPort, int partition) {
        this.curatorFramework = curatorFramework;
        this.jacksonInstanceSerializer = instanceSerializerFactory.getInstanceSerializer(new TypeReference<>() {
        });
        this.serviceName = serviceName;
        this.listenAddress = listenAddress;
        this.listenPort = listenPort;
        this.partition = partition;
    }

    /**
     * Register availability.
     */
    public void registerAvailability() {
        try {
            discovery = getDiscovery();
            discovery.start();
            discovery.registerService(getInstance());
        } catch (Exception e) {
            LOGGER.error("registerAvailability(): " + e.getMessage());
        }
    }

    /**
     * Unregister availability.
     */
    public void unregisterAvailability() {
        try {
            discovery = getDiscovery();
            discovery.start();
            discovery.unregisterService(getInstance());
            discovery.close();
        } catch (Exception e) {
            LOGGER.error("unregisterAvailability(): " + e.getMessage());
        }
    }

    /**
     * Helper method to get a service instance.
     *
     * @return Service instance
     * @throws Exception exception
     */
    private ServiceInstance<BrokerMetadata> getInstance() throws Exception {
        BrokerMetadata broker = new BrokerMetadata(listenAddress, listenPort, partition);
        return ServiceInstance.<BrokerMetadata>builder()
                .name(serviceName)
                .address(listenAddress)
                .port(listenPort)
                .payload(broker)
                .build();
    }

    /**
     * Helper method to get service discovery.
     *
     * @return service discovery
     */
    private ServiceDiscovery<BrokerMetadata> getDiscovery() {
        return ServiceDiscoveryBuilder.builder(BrokerMetadata.class)
                .basePath(Constants.BASE_PATH)
                .client(curatorFramework)
                .serializer(jacksonInstanceSerializer)
                .build();
    }
}
