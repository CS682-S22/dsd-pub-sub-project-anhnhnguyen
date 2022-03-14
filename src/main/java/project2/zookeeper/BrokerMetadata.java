package project2.zookeeper;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Broker info.
 *
 * @author anhnguyen
 * <p>
 * Reference: http://blog.palominolabs.com/2012/08/14/using-netflix-curator-for-service-discovery/index.html
 */
public class BrokerMetadata {
    /**
     * listening address.
     */
    @JsonProperty("listenAddress")
    private final String listenAddress;

    /**
     * listening port.
     */
    @JsonProperty("listenPort")
    private final int listenPort;

    /**
     * partition number.
     */
    @JsonProperty("partition")
    private final int partition;

    /**
     * Constructor.
     *
     * @param listenAddress listening address
     * @param listenPort    listening port
     * @param partition     partition
     */
    @JsonCreator
    public BrokerMetadata(@JsonProperty("listenAddress") String listenAddress, @JsonProperty("listenPort") int listenPort,
                          @JsonProperty("partition") int partition) {
        this.listenAddress = listenAddress;
        this.listenPort = listenPort;
        this.partition = partition;
    }

    /**
     * Getter for listening address.
     *
     * @return listen address
     */
    public String getListenAddress() {
        return listenAddress;
    }

    /**
     * Getter for listening port.
     *
     * @return listen port
     */
    public int getListenPort() {
        return listenPort;
    }

    /**
     * Getter for partition.
     */
    public int getPartition() {
        return partition;
    }
}
