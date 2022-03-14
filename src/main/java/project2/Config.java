package project2;

import java.io.File;

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
     * boolean for pull/push consumer.
     */
    private final boolean isPull;

    /**
     * Constructor.
     *
     * @param host         host
     * @param port         port
     * @param file         file to read and send message
     * @param topic        topic of message
     * @param position     starting position to pull from
     * @param partition    partition
     * @param zkConnection zkConnection
     * @param isPull       isPull
     */
    public Config(String host, int port, String file, String topic,
                  long position, int partition, String zkConnection, boolean isPull) {
        this.host = host;
        this.port = port;
        this.file = file;
        this.topic = topic;
        this.position = position;
        this.partition = partition;
        this.zkConnection = zkConnection;
        this.isPull = isPull;
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
     * Getter for boolean status for pull/push consumer.
     *
     * @return isPull
     */
    public boolean isPull() {
        return isPull;
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
