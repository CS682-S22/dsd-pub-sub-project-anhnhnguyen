package project2.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import project2.Client;
import project2.Constants;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Class that lets consumer be push-based instead of pull-based by first subscribing to a certain topic
 * then keep listening for new messages.
 *
 * @author anhnguyen
 */
public class PushConsumer extends Client {

    /**
     * Constructor.
     *
     * @param host host
     * @param port port
     */
    public PushConsumer(String host, int port, String topic) {
        super(host, port);
        connection.send(prepareSubRequest(topic));
        Logger logger = LoggerFactory.getLogger(PushConsumer.class);
        logger.info("subscribe request sent. topic: " + topic);
    }

    /**
     * Method to prepare request to broker to subscribe to the specified topic.
     *
     * @param topic topic
     * @return byte array in the form of [1-byte message type] | [topic]
     */
    private byte[] prepareSubRequest(String topic) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(topic.getBytes(StandardCharsets.UTF_8).length + 1);
        byteBuffer.put((byte) Constants.SUB_REQ);
        byteBuffer.put(topic.getBytes(StandardCharsets.UTF_8));
        return byteBuffer.array();
    }
}
