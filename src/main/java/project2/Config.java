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
     * Constructor.
     *
     * @param host     host
     * @param port     port
     * @param file     file to read and send message
     * @param topic    topic of message
     * @param position starting position to pull from
     */
    public Config(String host, int port, String file, String topic, long position) {
        this.host = host;
        this.port = port;
        this.file = file;
        this.topic = topic;
        this.position = position;
    }

    /**
     * Getter for host.
     * @return host
     */
    public String getHost() {
        return host;
    }

    /**
     * Getter for port.
     * @return port
     */
    public int getPort() {
        return port;
    }

    /**
     * Getter for file.
     * @return file
     */
    public String getFile() {
        return file;
    }

    /**
     * Getter for topic.
     * @return topic
     */
    public String getTopic() {
        return topic;
    }

    /**
     * Getter for position.
     * @return position
     */
    public long getPosition() {
        return position;
    }

    /**
     * Method to validate configs.
     */
    public void validate() {
        if (host == null || port < 1150 || port > 1174) {
            System.err.println("need to specify host name and port needs to be between 1150 and 1174");
            System.exit(1);
        }
        if (file != null) {
            File f = new File(file);
            if (!f.exists()) {
                System.err.println(file + " doesn't exist");
                System.exit(1);
            }
        }
    }
}
