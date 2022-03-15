package project2.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class that lets consumer be push-based instead of pull-based by first subscribing to a certain topic
 * then keep listening for new messages.
 *
 * @author anhnguyen
 */
public class PushConsumer extends Consumer {

    /**
     * Constructor.
     *
     * @param host             host
     * @param port             port
     * @param topic            topic
     * @param startingPosition starting position
     */
    public PushConsumer(String host, int port, String topic, long startingPosition) {
        super(host, port, topic, startingPosition);
        connection.send(prepareRequest(topic, startingPosition));
        Logger logger = LoggerFactory.getLogger(PushConsumer.class);
        logger.info("subscribe request sent. topic: " + topic);
    }

    /**
     * method to listen for messages from broker.
     *
     * @param milliseconds interval to timeout, do nothing, listen for messages again
     * @return byte[] array of message received
     */
    @Override
    public byte[] poll(int milliseconds) {
        return getMessage(milliseconds);
    }
}
