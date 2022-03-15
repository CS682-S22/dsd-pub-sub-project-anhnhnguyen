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
    protected final String topic;
    /**
     * starting position.
     */
    protected long startingPosition;

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
     * method to periodically send request to broker every specified milliseconds if no responses from broker.
     *
     * @param milliseconds interval
     * @return byte[] array of message received
     */
    @Override
    public byte[] poll(int milliseconds) {
        byte[] message = getMessage(milliseconds);
        if (message == null) {
            connection.send(prepareRequest(topic, startingPosition, (byte) Constants.PULL_REQ));
            LOGGER.info("pull request sent. topic: " + topic + ", starting position: " + startingPosition);
        }
        return message;
    }

    /**
     * Method to prepare request to broker to pull message for the specified topic and starting position.
     *
     * @param topic            topic
     * @param startingPosition starting position
     * @param messageType      message type (Pull request or subscribe request)
     * @return byte array in the form of [1-byte message type] | [topic] | 0 | [8-byte offset]
     */
    protected byte[] prepareRequest(String topic, long startingPosition, byte messageType) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(topic.getBytes(StandardCharsets.UTF_8).length + 10);
        byteBuffer.put(messageType);
        byteBuffer.put(topic.getBytes(StandardCharsets.UTF_8));
        byteBuffer.put((byte) 0);
        byteBuffer.putLong(startingPosition);
        return byteBuffer.array();
    }

    /**
     * Method to get message from the connection and verify message is not duplicate.
     *
     * @param milliseconds timeout interval
     * @return byte array
     */
    protected byte[] getMessage(int milliseconds) {
        byte[] message = connection.receive(milliseconds);
        if (message != null) {
            ReqRes response = new ReqRes(message);
            if (response.getOffset() < startingPosition) {
                return null;
            }
            startingPosition = response.getOffset() + response.getKey().getBytes(StandardCharsets.UTF_8).length
                    + response.getData().length + 1;
        }
        return message;
    }
}
