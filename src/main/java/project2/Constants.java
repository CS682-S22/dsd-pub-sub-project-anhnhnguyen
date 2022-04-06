package project2;

/**
 * Class that maintains information about constants used in the program.
 *
 * @author anhnguyen
 */
public class Constants {
    /**
     * size of byte to read message from socket.
     */
    public static final int BYTE_ALLOCATION = 1024;
    /**
     * exit string.
     */
    public static final String EXIT = "exit";
    /**
     * publish request message type.
     */
    public static final int PUB_REQ = 0;
    /**
     * pull request message type.
     */
    public static final int PULL_REQ = 1;
    /**
     * membership request message type.
     */
    public static final int MEM_REQ = 2;
    /**
     * replication request message type.
     */
    public static final int REP_REQ = 3;
    /**
     * pull request response message type.
     */
    public static final int REQ_RES = 4;
    /**
     * ack response message type.
     */
    public static final int ACK_RES = 5;
    /**
     * catch up message type.
     */
    public static final int CAT_FIN = 6;
    /**
     * number of messages sent to consumer per each poll.
     */
    public static final int NUM_RESPONSE = 10;
    /**
     * position of the offset list in the list mapped to topic.
     */
    public static final int OFFSET_INDEX = 0;
    /**
     * position of the starting offset list in the list mapped to topic.
     */
    public static final int STARTING_OFFSET_INDEX = 1;
    /**
     * position of the list storing the number of partitions in the list mapped to topic.
     */
    public static final int NUM_PARTITIONS_INDEX = 2;
    /**
     * maximum segment file.
     */
    public static final int SEGMENT_SIZE = 1024;
    /**
     * persistent folder.
     */
    public static final String LOG_FOLDER = "log/";
    /**
     * log extension.
     */
    public static final String FILE_TYPE = ".log";
    /**
     * path string.
     */
    public static final String PATH_STRING = "/";
    /**
     * timeout interval.
     */
    public static final int TIME_OUT = 500;
    /**
     * polling interval.
     */
    public static final int INTERVAL = 500;
    /**
     * zk base path.
     */
    public static final String BASE_PATH = "/pubsub";
    /**
     * service name.
     */
    public static final String SERVICE_NAME = "pubsub";
    /**
     * default number of partitions.
     */
    public static final int NUM_PARTS = 3;
    /**
     * retry.
     */
    public static final int RETRY = 20;
    /**
     * no leader info.
     */
    public static final String NONE = "none";
}
