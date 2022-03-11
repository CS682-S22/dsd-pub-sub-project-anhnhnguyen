package project2.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import project2.Constants;
import project2.broker.ReqRes;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;

public class ConsumerDriver {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerDriver.class);
    private static boolean isRunning = true;
    private static final int TIME_OUT = 100;

    public static void main(String[] args) {
        validateArgs(args);
        Consumer consumer = new Consumer(args[0], Integer.parseInt(args[1]), args[2], Integer.parseInt(args[3]));
        Thread t = new Thread(() -> {
            try {
                BufferedWriter bw = new BufferedWriter(new FileWriter(args[2] + ".log", true));
                while (isRunning) {
                    byte[] data = consumer.poll(TIME_OUT);
                    if (data != null) {
                        ReqRes response = new ReqRes(data);
                        String key = response.getKey();
                        String txt = new String(response.getData(), StandardCharsets.UTF_8);
                        bw.write(key + " " + txt + "\n");
                        LOGGER.info("write to file: " + args[2] + ".log");
                    }
                }
                bw.close();
            } catch (IOException e) {
                LOGGER.error(e.getMessage());
            }

        });
        t.start();
        Scanner scanner = new Scanner(System.in);
        if (scanner.nextLine().equalsIgnoreCase(Constants.EXIT)) {
            isRunning = false;
            try {
                t.join();
            } catch (InterruptedException e) {
                LOGGER.error(e.getMessage());
            }
            consumer.close();
        }
    }

    private static void validateArgs(String[] args) {
        if (args.length != 4) {
            LOGGER.error("incorrect number of arguments. " +
                    "usage: java -cp project2.jar project2.consumer.ConsumerDriver <host> <port> <topic> <starting position>");
            System.exit(1);
        }
        try {
            Integer.parseInt(args[1]);
        } catch (NumberFormatException e) {
            LOGGER.error(args[1] + " is not an integer");
            System.exit(1);
        }
        try {
            Integer.parseInt(args[3]);
        } catch (NumberFormatException e) {
            LOGGER.error(args[3] + " is not an integer");
            System.exit(1);
        }
    }
}
