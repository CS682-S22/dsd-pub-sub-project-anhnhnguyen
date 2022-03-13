package project2.producer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import project2.Client;
import project2.Constants;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Class that sends message to broker.
 *
 * @author anhnguyen
 */
public class Producer extends Client {
    /**
     * logger object.
     */
    private final Logger LOGGER = LoggerFactory.getLogger(Producer.class);

    /**
     * Constructor.
     *
     * @param host host
     * @param port port
     */
    public Producer(String host, int port) {
        super(host, port);
    }

    /**
     * method to publish topic, key, and data to broker.
     *
     * @param topic topic
     * @param key   key
     * @param data  data
     */
    public void send(String topic, String key, byte[] data) {
        int length = topic.getBytes(StandardCharsets.UTF_8).length
                + key.getBytes(StandardCharsets.UTF_8).length + data.length + 3;
        ByteBuffer byteBuffer = ByteBuffer.allocate(length);
        byteBuffer.put((byte) Constants.PUB_REQ);
        byteBuffer.put(topic.getBytes(StandardCharsets.UTF_8));
        byteBuffer.put((byte) 0);
        byteBuffer.put(key.getBytes(StandardCharsets.UTF_8));
        byteBuffer.put((byte) 0);
        byteBuffer.put(data);
        LOGGER.info("message sent. topic: " + topic + ", key: "
                + key + ", data: " + new String(data, StandardCharsets.UTF_8));
        connection.send(byteBuffer.array());
    }
}
