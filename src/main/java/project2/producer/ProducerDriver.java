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
        Map<Integer, Producer> partitionMap = findBrokers(curator);
        publish(config, partitionMap, curator);
    }

    /**
     * Method to find brokers and store them in a map for easy lookup based on partition number.
     *
     * @return map that points partition to producer that connected with the brokers that store the partition
     */
    private static Map<Integer, Producer> findBrokers(Curator curator) {
        Collection<BrokerMetadata> brokers = curator.findBrokers();
        Map<Integer, Producer> partitionMap = new HashMap<>();
        for (BrokerMetadata broker : brokers) {
            Producer producer = new Producer(broker.getListenAddress(), broker.getListenPort());
            int partition = broker.getPartition();
            partitionMap.put(partition, producer);
        }
        return partitionMap;
    }

    /**
     * Method to read from file line by line, and publish message to the appropriate broker based on key.
     *
     * @param config       config
     * @param partitionMap partition map
     * @param curator      curator
     */
    private static void publish(Config config, Map<Integer, Producer> partitionMap, Curator curator) {
        try (FileInputStream fis = new FileInputStream(config.getFile());
             BufferedReader br = new BufferedReader(new InputStreamReader(fis))) {
            LOGGER.info("reading from file: " + config.getFile());
            String line;
            while ((line = br.readLine()) != null) {
                String topic = findTopic(line);
                String key = findKey(line);
                byte[] data = findData(line);
                int topicPartitions = config.getTopics().getOrDefault(topic, Constants.NUM_PARTS);
                int partition = (key.hashCode() % topicPartitions) % partitionMap.size();
                Producer producer = partitionMap.get(partition);
                producer.send(topic, key, data, topicPartitions);
            }
            for (Producer producer : partitionMap.values()) {
                producer.close();
            }
            curator.close();
        } catch (IOException e) {
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
