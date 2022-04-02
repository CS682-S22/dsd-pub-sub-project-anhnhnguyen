package project2.producer;

import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import project2.Config;
import project2.Constants;
import project2.Utils;
import project2.zookeeper.BrokerMetadata;
import project2.zookeeper.Curator;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Driver for producer to connect with broker and publish message.
 *
 * @author anhnguyen
 */
public class ProducerDriver {
    /**
     * logger object.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(ProducerDriver.class);

    /**
     * map between partition number and producer that connects with the appropriate broker.
     */
    private static final Map<Integer, Producer> partitionMap = new HashMap<>();

    /**
     * main program to start producer and send messages to broker.
     *
     * @param args program arguments
     */
    public static void main(String[] args) {
        Utils.validateArgs(args);
        Config config = null;
        try {
            config = new Gson().fromJson(new FileReader(args[0]), Config.class);
        } catch (FileNotFoundException e) {
            LOGGER.error(e.getMessage());
            System.exit(1);
        }

        Curator curator = new Curator(config.getZkConnection());
        findBrokers(curator, null, 0);
        publish(config, curator);
    }

    /**
     * Method to find brokers and store them in a map for easy lookup based on partition number.
     *
     * @param curator curator
     * @param host host
     * @param port port
     */
    private static void findBrokers(Curator curator, String host, int port) {
        Collection<BrokerMetadata> brokers = curator.findBrokers();
        for (BrokerMetadata broker : brokers) {
            int partition = broker.getPartition();
            String brokerHost = broker.getListenAddress();
            int brokerPort = broker.getListenPort();
            // if broker fails, only replace the partition that that broker is in charge of with the new producer
            // connecting with that broker. Need to check that new broker is not the same because Zookeeper
            // may take some time to reflect the change.
            if (!partitionMap.containsKey(partition) && (!brokerHost.equals(host) || brokerPort != port)) {
                Producer producer = new Producer(broker.getListenAddress(), broker.getListenPort());
                partitionMap.put(partition, producer);
            }
        }
    }

    /**
     * Method to read from file line by line, and publish message to the appropriate broker based on key.
     *
     * @param config       config
     * @param curator      curator
     */
    private static void publish(Config config, Curator curator) {
        try (FileInputStream fis = new FileInputStream(config.getFile());
             BufferedReader br = new BufferedReader(new InputStreamReader(fis))) {
            LOGGER.info("reading from file: " + config.getFile());
            String line;
            while ((line = br.readLine()) != null) {
                Thread.sleep(5); // only for demo purpose or else publishing happens too fast, can't inject failure
                String topic = findTopic(line);
                String key = findKey(line);
                byte[] data = findData(line);
                int topicPartitions = config.getTopics().getOrDefault(topic, Constants.NUM_PARTS);
                int partition = (key.hashCode() % topicPartitions) % config.getNumBrokers();
                while (!partitionMap.containsKey(partition)) {
                    findBrokers(curator, null, 0);
                }
                Producer producer = partitionMap.get(partition);
                boolean success = producer.send(topic, key, data, topicPartitions);
                // success is false when IOException happens or broker fails
                // then remove that producer connecting with that broker from the partition map
                // and look for a new broker through Zookeeper
                while (!success) {
                    LOGGER.info("Looking for new broker");
                    String currentHost = producer.getHost();
                    int currentPort = producer.getPort();
                    partitionMap.remove(partition);
                    while (!partitionMap.containsKey(partition)) {
                        findBrokers(curator, currentHost, currentPort);
                    }
                    producer = partitionMap.get(partition);
                    success = producer.send(topic, key, data, topicPartitions);
                }
            }
            for (Producer producer : partitionMap.values()) {
                producer.close();
            }
            curator.close();
        } catch (IOException | InterruptedException e) {
            LOGGER.error(e.getMessage());
            System.exit(1);
        }
    }

    /**
     * Method to find the break between topic and data.
     *
     * @param line line
     * @return index of break
     */
    private static int findBreak(String line) {
        int count = 0;
        int index = 0;
        while (index < line.length()) {
            if (line.charAt(index) == ':') {
                count++;
            }
            if (count == 3) {
                break;
            }
            index++;
        }
        return index;
    }

    /**
     * method to find topic.
     *
     * @param line line
     * @return topic
     */
    private static String findTopic(String line) {
        int index = findBreak(line) - 1;
        StringBuilder sb = new StringBuilder();
        while (index >= 0) {
            if (line.charAt(index) == ' ') {
                break;
            }
            sb.append(line.charAt(index));
            index--;
        }
        return sb.reverse().toString();
    }

    /**
     * method to find key.
     *
     * @param line line
     * @return key
     */
    private static String findKey(String line) {
        String key = line.split(" ")[4];
        if (key.isEmpty()) {
            key = line.split(" ")[5];
        }
        return key;
    }

    /**
     * method to find data.
     *
     * @param line line
     * @return data
     */
    private static byte[] findData(String line) {
        int index = findBreak(line) + 2;
        return line.substring(index).getBytes(StandardCharsets.UTF_8);
    }
}
