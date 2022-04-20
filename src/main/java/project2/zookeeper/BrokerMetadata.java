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
     * id.
     */
    @JsonProperty("id")
    private final int id;

    /**
     * isAsync.
     */
    @JsonProperty("isAsync")
    private final boolean isAsync;

    /**
     * Constructor.
     *
     * @param listenAddress listening address
     * @param listenPort    listening port
     * @param partition     partition
     */
    @JsonCreator
    public BrokerMetadata(@JsonProperty("listenAddress") String listenAddress, @JsonProperty("listenPort") int listenPort,
                          @JsonProperty("partition") int partition, @JsonProperty("id") int id, @JsonProperty("isAsync") boolean isAsync) {
        this.listenAddress = listenAddress;
        this.listenPort = listenPort;
        this.partition = partition;
        this.id = id;
        this.isAsync = isAsync;
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

    /**
     * Getter for id.
     *
     * @return id
     */
    public int getId() {
        return id;
    }

    /**
     * Getter for async status.
     *
     * @return async status
     */
    public boolean isAsync() {
        return isAsync;
    }
}
