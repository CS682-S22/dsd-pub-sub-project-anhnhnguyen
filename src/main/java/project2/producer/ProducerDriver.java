package project2.producer;

import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import project2.Config;
import project2.Utils;

import java.io.*;
import java.nio.charset.StandardCharsets;

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
        Producer producer = new Producer(config.getHost(), config.getPort());
        try (FileInputStream fis = new FileInputStream(config.getFile());
             BufferedReader br = new BufferedReader(new InputStreamReader(fis))) {
            LOGGER.info("reading from file: " + config.getFile());
            String line;
            while ((line = br.readLine()) != null) {
                String topic = findTopic(line);
                String key = findKey(line);
                byte[] data = findData(line);
                producer.send(topic, key, data);
            }
        } catch (IOException e) {
            LOGGER.error(e.getMessage());
            System.exit(1);
        }
        producer.close();
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
