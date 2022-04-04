package project2;

import java.io.File;
import java.util.Map;

/**
 * Class that extract configurations from JSON file.
 *
 * @author anhnguyen
 */
public class Config {
    /**
     * host.
     */
    private final String host;
    /**
     * port.
     */
    private final int port;
    /**
     * file to read and send.
     */
    private final String file;
    /**
     * topic to pull message.
     */
    private final String topic;
    /**
     * position to pull message from.
     */
    private final long position;
    /**
     * partition number.
     */
    private final int partition;
    /**
     * zkConnection.
     */
    private final String zkConnection;
    /**
     * topic map.
     */
    private final Map<String, Integer> topics;
    /**
     * number of partitions.
     */
    private final int numPartitions;
    /**
     * number of brokers.
     */
    private final int numBrokers;
    /**
     * leader status.
     */
    private final boolean isLeader;
    /**
     * broker id.
     */
    private final int id;
    /**
     * member info.
     */
    private final String members;

    /**
     * Constructor.
     *
     * @param host          host
     * @param port          port
     * @param file          file to read and send message
     * @param topic         topic of message
     * @param position      starting position to pull from
     * @param partition     partition
     * @param zkConnection  zkConnection
     * @param numPartitions number of total partitions
     * @param numBrokers    number of brokers
     * @param topics        map between topic and number of partitions for the topic
     * @param isLeader      leader status
     * @param id            id
     * @param members       members
     */
    public Config(String host, int port, String file, String topic, long position, int partition, boolean isLeader, String members,
                  String zkConnection, Map<String, Integer> topics, int numPartitions, int numBrokers, int id) {
        this.host = host;
        this.port = port;
        this.file = file;
        this.topic = topic;
        this.position = position;
        this.partition = partition;
        this.zkConnection = zkConnection;
        this.topics = topics;
        this.numPartitions = numPartitions;
        this.numBrokers = numBrokers;
        this.isLeader = isLeader;
        this.id = id;
        this.members = members;
    }

    /**
     * Getter for host.
     *
     * @return host
     */
    public String getHost() {
        return host;
    }

    /**
     * Getter for port.
     *
     * @return port
     */
    public int getPort() {
        return port;
    }

    /**
     * Getter for file.
     *
     * @return file
     */
    public String getFile() {
        return file;
    }

    /**
     * Getter for topic.
     *
     * @return topic
     */
    public String getTopic() {
        return topic;
    }

    /**
     * Getter for position.
     *
     * @return position
     */
    public long getPosition() {
        return position;
    }

    /**
     * Getter for partition.
     *
     * @return partition
     */
    public int getPartition() {
        return partition;
    }

    /**
     * Getter for zkConnection.
     *
     * @return zkConnection
     */
    public String getZkConnection() {
        return zkConnection;
    }

    /**
     * Getter for topics map of topic with the number of partitions for each topic.
     *
     * @return topics
     */
    public Map<String, Integer> getTopics() {
        return topics;
    }

    /**
     * Getter for number of partitions.
     *
     * @return numPartitions
     */
    public int getNumPartitions() {
        return numPartitions;
    }

    /**
     * Getter for number of brokers.
     *
     * @return numBrokers
     */
    public int getNumBrokers() {
        return numBrokers;
    }

    /**
     * Getter for leader status.
     *
     * @return isLeader
     */
    public boolean isLeader() {
        return isLeader;
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
     * Getter for members.
     *
     * @return member info
     */
    public String getMembers() {
        return members;
    }

    /**
     * Method to validate configs.
     */
    public void validate() {
        if (file != null) {
            File f = new File(file);
            if (!f.exists()) {
                System.err.println(file + " doesn't exist");
                System.exit(1);
            }
        }
    }
}
