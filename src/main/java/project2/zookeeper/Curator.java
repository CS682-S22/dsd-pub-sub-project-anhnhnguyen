package project2.zookeeper;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import project2.Constants;

import java.util.Collection;

/**
 * Class to start the curator framework and object mapper.
 *
 * @author anhnguyen
 */
public class Curator {
    /**
     * curator framework.
     */
    private final CuratorFramework curatorFramework;
    /**
     * object mapper.
     */
    private final ObjectMapper objectMapper;

    /**
     * Constructor.
     *
     * @param zkConnection zkConnection
     */
    public Curator(String zkConnection) {
        this.curatorFramework = CuratorFrameworkFactory.builder()
                .connectionTimeoutMs(1000)
                .retryPolicy(new RetryNTimes(10, 500))
                .connectString(zkConnection)
                .build();
        this.curatorFramework.start();
        this.objectMapper = new ObjectMapper();
        this.objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    /**
     * Method to find brokers.
     *
     * @return broker meta data
     */
    public Collection<BrokerMetadata> findBrokers() {
        BrokerFinder brokerFinder = new BrokerFinder(curatorFramework,
                new InstanceSerializerFactory(objectMapper.reader(), objectMapper.writer()));
        Collection<BrokerMetadata> brokers = brokerFinder.getBrokers(Constants.SERVICE_NAME);
        brokerFinder.close();
        return brokers;
    }

    /**
     * Getter for curator framework.
     *
     * @return curator framework
     */
    public CuratorFramework getCuratorFramework() {
        return curatorFramework;
    }

    /**
     * Getter for object mapper.
     *
     * @return object mapper
     */
    public ObjectMapper getObjectMapper() {
        return objectMapper;
    }

    /**
     * method to close curator framework.
     */
    public void close() {
        curatorFramework.close();
    }
}
