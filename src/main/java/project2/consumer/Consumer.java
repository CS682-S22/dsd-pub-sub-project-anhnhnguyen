package project2.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import project2.Client;
import project2.Constants;
import project2.broker.ReqRes;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Class that pulls message from the Broker by specifying topic and starting position to pull.
 *
 * @author anhnguyen
 */
public class Consumer extends Client {
    /**
     * logger object.
     */
    private final Logger LOGGER = LoggerFactory.getLogger(Consumer.class);
    /**
     * topic.
     */
    private final String topic;
    /**
     * starting position.
     */
    private long startingPosition;

    /**
     * Constructor.
     *
     * @param host             host
     * @param port             port
     * @param topic            topic
     * @param startingPosition starting position
     */
    public Consumer(String host, int port, String topic, long startingPosition) {
        super(host, port);
        this.topic = topic;
        this.startingPosition = startingPosition;
    }

    /**
     * Method to prepare request to broker to pull message for the specified topic and starting position.
     *
     * @param topic            topic
     * @param startingPosition starting position
     * @return byte array in the form of [1-byte message type] | [topic] | 0 | [8-byte offset]
     */
    private byte[] prepareRequest(String topic, long startingPosition) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(topic.getBytes(StandardCharsets.UTF_8).length + 10);
        byteBuffer.put((byte) Constants.PULL_REQ);
        byteBuffer.put(topic.getBytes(StandardCharsets.UTF_8));
        byteBuffer.put((byte) 0);
        byteBuffer.putLong(startingPosition);
        return byteBuffer.array();
    }

    /**
     * method to periodically send request to broker every specified milliseconds if no responses from broker.
     *
     * @param milliseconds interval
     * @return byte[] array of message received
     */
    @Override
    public byte[] poll(int milliseconds) {
        byte[] message = connection.receive(milliseconds);
        if (message != null) {
            ReqRes response = new ReqRes(message);
            startingPosition = response.getOffset() + response.getKey().getBytes(StandardCharsets.UTF_8).length
                    + response.getData().length + 1;
        } else {
            connection.send(prepareRequest(topic, startingPosition));
            LOGGER.info("pull request sent. topic: " + topic + ", starting position: " + startingPosition);
        }
        return message;
    }
}
