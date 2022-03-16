package project2.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import project2.Constants;

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
     * @param partition        partition
     */
    public PushConsumer(String host, int port, String topic, long startingPosition, int partition) {
        super(host, port, topic, startingPosition, partition);
        connection.send(prepareRequest(topic, startingPosition, (byte) Constants.SUB_REQ, partition));
        Logger logger = LoggerFactory.getLogger(PushConsumer.class);
        logger.info("subscribe request sent. topic: " + topic + ", partition: " + partition + ", starting offset: " + startingPosition);
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
