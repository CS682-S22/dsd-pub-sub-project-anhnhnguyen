package project2.consumer;

import project2.Utils;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;

/**
 * Class that extracts byte array from consumer to broker into object with topic and starting position.
 *
 * @author anhnguyen
 */
public class PullReq {
    /**
     * topic.
     */
    private final String topic;
    /**
     * starting position.
     */
    private final long startingPosition;
    /**
     * partition.
     */
    private final int partition;

    /**
     * Constructor.
     * <p>
     * Extracting byte array in the form of [1-byte message type] | [topic] | 0 | [8-byte offset] | [2-byte partition]
     *
     * @param message byte array
     */
    public PullReq(byte[] message) {
        int index = 1;
        byte[] topicBytes = Utils.extractBytes(index, message.length, message, true);
        this.topic = new String(topicBytes, StandardCharsets.UTF_8);

        index += topicBytes.length + 1;
        this.startingPosition = new BigInteger(Utils.extractBytes(index, index + 8, message, false)).longValue();
        this.partition = new BigInteger(Utils.extractBytes(index + 8, message.length, message, false)).intValue();
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
     * Getter for starting position.
     *
     * @return starting position
     */
    public long getStartingPosition() {
        return startingPosition;
    }

    /**
     * Getter for partition.
     * @return partition
     */
    public int getPartition() {
        return partition;
    }
}
