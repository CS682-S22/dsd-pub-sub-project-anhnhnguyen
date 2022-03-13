package project2.broker;

import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import project2.Config;
import project2.Constants;
import project2.Utils;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Scanner;

/**
 * Driver to start the broker on the config host and port and listen for incoming requests.
 *
 * @author anhnguyen
 */
public class BrokerDriver {
    /**
     * logger object.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(BrokerDriver.class);

    /**
     * main broker program to listen for incoming request until user asks broker to exit.
     *
     * @param args program arguments
     */
    public static void main(String[] args) {
        Utils.validateArgs(args);
        try {
            Config config = new Gson().fromJson(new FileReader(args[0]), Config.class);
            config.validate();
            Broker broker = new Broker(config.getHost(), config.getPort());
            broker.start();
            Scanner scanner = new Scanner(System.in);
            if (scanner.nextLine().equalsIgnoreCase(Constants.EXIT)) {
                broker.close();
            }
        } catch (FileNotFoundException e) {
            LOGGER.error(e.getMessage());
            System.exit(1);
        }
    }
}
