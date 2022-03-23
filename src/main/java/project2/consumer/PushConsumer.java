package project2.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import project2.Constants;

import java.io.IOException;

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
        Logger logger = LoggerFactory.getLogger(PushConsumer.class);
        try {
            byte[] request = prepareRequest(topic, startingPosition, (byte) Constants.SUB_REQ, partition, 0);
            this.dos.writeShort(request.length);
            this.dos.write(request);
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
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
