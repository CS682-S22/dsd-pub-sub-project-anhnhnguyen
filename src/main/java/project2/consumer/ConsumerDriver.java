package project2.consumer;

import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import project2.Config;
import project2.Constants;
import project2.Utils;
import project2.broker.ReqRes;
import project2.zookeeper.BrokerMetadata;
import project2.zookeeper.Curator;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Driver for consumer to connect with the broker and request to pull message for certain topic at a certain position.
 *
 * @author anhnguyen
 */
public class ConsumerDriver {
    /**
     * logger object.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerDriver.class);
    /**
     * state of the driver.
     */
    private static volatile boolean isRunning = true;

    /**
     * Main program for consumer to start a thread to pull message from consumer until user asks to exit.
     *
     * @param args program arguments
     */
    public static void main(String[] args) {
        Utils.validateArgs(args);
        try {
            Config config = new Gson().fromJson(new FileReader(args[0]), Config.class);
            config.validate();
            Curator curator = new Curator(config.getZkConnection());
            Collection<BrokerMetadata> brokers = curator.findBrokers();
            Map<Consumer, Integer> partitionMap = new HashMap<>();
            for (BrokerMetadata broker : brokers) {
                Consumer consumer = new Consumer(broker.getListenAddress(), broker.getListenPort(),
                        config.getTopic(), config.getPosition());
                partitionMap.put(consumer, broker.getPartition());
            }

            List<Thread> threads = new ArrayList<>();
            for (Consumer consumer : partitionMap.keySet()) {
                Thread t = new Thread(() -> request(consumer, config, partitionMap.get(consumer)));
                t.start();
                threads.add(t);
            }

            Scanner scanner = new Scanner(System.in);
            if (scanner.nextLine().equalsIgnoreCase(Constants.EXIT)) {
                isRunning = false;
                for (Thread t : threads) {
                    t.join();
                }
                for (Consumer consumer : partitionMap.keySet()) {
                    consumer.close();
                }
                curator.close();
            }
        } catch (FileNotFoundException | InterruptedException e) {
            LOGGER.error(e.getMessage());
            System.exit(1);
        }
    }

    /**
     * method to request consumer object to pull message and print to file.
     *
     * @param consumer consumer
     * @param config   config
     * @param suffix   suffix
     */
    private static void request(Consumer consumer, Config config, int suffix) {
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(config.getTopic() + suffix + Constants.FILE_TYPE))) {
            while (isRunning) {
                byte[] data = consumer.poll(Constants.TIME_OUT);
                if (data != null) {
                    ReqRes response = new ReqRes(data);
                    bw.write(response.getKey() + " "
                            + new String(response.getData(), StandardCharsets.UTF_8) + "\n");
                    LOGGER.info("write to file: " + config.getTopic() + suffix + Constants.FILE_TYPE);
                }
            }
        } catch (IOException e) {
            LOGGER.error(e.getMessage());
        }
    }
}
