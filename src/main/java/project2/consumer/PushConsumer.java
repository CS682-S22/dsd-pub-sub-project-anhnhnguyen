package project2.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import project2.Constants;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

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
        this.scheduler.shutdownNow(); // cancel the pulling task from the super class
        Logger logger = LoggerFactory.getLogger(PushConsumer.class);
        try {
            byte[] request = prepareRequest(topic, startingPosition, (byte) Constants.SUB_REQ, partition, 0);
            this.dos.writeShort(request.length);
            this.dos.write(request);
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
        logger.info("subscribe request sent. topic: " + topic + ", partition: " + partition + ", starting offset: " + startingPosition);
        this.scheduler = Executors.newSingleThreadScheduledExecutor();
        // thread to read message from broker and add to queue where application can poll from
        this.scheduler.scheduleWithFixedDelay(this::getMessage, 0, Constants.INTERVAL, TimeUnit.MILLISECONDS);
    }
}
