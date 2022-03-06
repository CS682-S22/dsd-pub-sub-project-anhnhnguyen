package project2.broker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import project2.Constants;

import java.util.Scanner;

public class BrokerDriver {
    private static final Logger LOGGER = LoggerFactory.getLogger(BrokerDriver.class);

    public static void main(String[] args) {
        validateArgs(args);
        Broker broker = new Broker(args[0], Integer.parseInt(args[1]));
        broker.start();
        Scanner scanner = new Scanner(System.in);
        if (scanner.nextLine().equalsIgnoreCase(Constants.EXIT)) {
            broker.close();
        }
    }

    private static void validateArgs(String[] args) {
        if (args.length != 2) {
            LOGGER.error("incorrect number of arguments. " +
                    "usage: java -cp project2.jar project2.broker.BrokerDriver <host> <port>");
            System.exit(1);
        }
        try {
            Integer.parseInt(args[1]);
        } catch (NumberFormatException e) {
            LOGGER.error(args[1] + " is not an integer");
            System.exit(1);
        }
    }
}
