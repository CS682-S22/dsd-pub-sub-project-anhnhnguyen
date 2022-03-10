package project2.producer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;

public class ProducerDriver {
    private static final Logger LOGGER = LoggerFactory.getLogger(ProducerDriver.class);

    public static void main(String[] args) {
        validateArgs(args);
        Producer producer = new Producer(args[0], Integer.parseInt(args[1]));
        try (FileInputStream fis = new FileInputStream(args[2]);
             BufferedReader br = new BufferedReader(new InputStreamReader(fis))) {
            LOGGER.info("reading from file: " + args[2]);
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

    private static void validateArgs(String[] args) {
        if (args.length != 3) {
            LOGGER.error("incorrect number of arguments. " +
                    "usage: java -cp project2.jar project2.producer.ProducerDriver <host> <port> <file name>");
            System.exit(1);
        }
        try {
            Integer.parseInt(args[1]);
        } catch (NumberFormatException e) {
            LOGGER.error(args[1] + " is not an integer");
            System.exit(1);
        }
        if (!new File(args[2]).exists()) {
            LOGGER.error(args[2] + "doesn't exist");
            System.exit(1);
        }
    }

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

    private static String findKey(String line) {
        return line.split(" ")[4];
    }

    private static byte[] findData(String line) {
        int index = findBreak(line) + 2;
        return line.substring(index).getBytes(StandardCharsets.UTF_8);
    }
}
