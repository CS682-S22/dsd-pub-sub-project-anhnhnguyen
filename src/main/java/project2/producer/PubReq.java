package project2.producer;

import project2.Utils;

import java.nio.charset.StandardCharsets;

/**
 * Class that extracts byte array from producer to broker into object with topic, key, and data.
 *
 * @author anhnguyen
 */
public class PubReq {
    /**
     * topic.
     */
    private final String topic;
    /**
     * key.
     */
    private final String key;
    /**
     * data.
     */
    private final byte[] data;

    /**
     * Constructor.
     * <p>
     * Extracting byte array in the form of [1-byte message type] | [topic] | 0 | [key] | 0 | [data]
     *
     * @param message byte array
     */
    public PubReq(byte[] message) {
        int index = 1;
        byte[] topicBytes = Utils.extractBytes(index, message.length, message, true);
        this.topic = new String(topicBytes, StandardCharsets.UTF_8);

        index += topicBytes.length + 1;
        byte[] keyBytes = Utils.extractBytes(index, message.length, message, true);
        this.key = new String(keyBytes, StandardCharsets.UTF_8);

        index += keyBytes.length + 1;
        this.data = Utils.extractBytes(index, message.length, message, false);
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
     * Getter for key.
     *
     * @return key
     */
    public String getKey() {
        return key;
    }

    /**
     * Getter for data.
     *
     * @return data
     */
    public byte[] getData() {
        return data;
    }
}
