package project2.consumer;

import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import project2.Config;
import project2.Connection;
import project2.Constants;
import project2.Utils;
import project2.broker.ReqRes;
import project2.broker.protos.Membership;
import project2.zookeeper.BrokerMetadata;
import project2.zookeeper.Curator;

import java.io.*;
import java.net.InetSocketAddress;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Driver for consumer to connect with the broker and request to pull message for certain topic at a certain position.
 *
 * @author anhnguyen
 */
public class ConsumerDriver {
    /**
     * logger object.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger("operation");
    /**
     * state of the driver.
     */
    private static volatile boolean isRunning = true;
    /**
     * curator.
     */
    protected static Curator curator;
    /**
     * number of brokers.
     */
    private static int numBrokers;

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
            numBrokers = config.getNumBrokers();
            curator = new Curator(config.getZkConnection());
            Collection<BrokerMetadata> brokers = curator.findBrokers();
            while (brokers.size() != config.getNumBrokers()) {
                synchronized (brokers) {
                    brokers.wait(Constants.TIME_OUT);
                }
                LOGGER.info("Looking for broker");
                brokers = curator.findBrokers();
            }
            Map<Consumer, Integer> clients = createConsumers(brokers, config);

            List<Thread> threads = new ArrayList<>();
            for (Consumer consumer : clients.keySet()) {
                Thread t = new Thread(() -> request(consumer, config, clients.get(consumer)));
                t.start();
                threads.add(t);
            }

            Scanner scanner = new Scanner(System.in);
            if (scanner.nextLine().equalsIgnoreCase(Constants.EXIT)) {
                isRunning = false;
                for (Thread t : threads) {
                    t.join(100);
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
                    bw.write(response.getKey() + " " + new String(response.getData(), StandardCharsets.UTF_8));
                    bw.newLine();
                    bw.flush();
                    LOGGER.info("write to file: " + config.getTopic() + suffix + Constants.FILE_TYPE + ", offset: " + response.getOffset());
                }
            }
            bw.flush();
            consumer.close();
        } catch (IOException e) {
            LOGGER.error(e.getMessage());
        }
    }

    /**
     * Method to create consumers (pull/push) object linked with each broker.
     *
     * @param brokers brokers
     * @param config  config
     * @return map between consumer and the partition it's pulling from
     */
    private static Map<Consumer, Integer> createConsumers(Collection<BrokerMetadata> brokers, Config config) {
        Map<Consumer, Integer> clients = new HashMap<>();
        for (int i = 0; i < config.getNumPartitions(); i++) {
            BrokerMetadata broker = findBroker(brokers, i);
            if (broker != null) {
                Membership.Broker follower = findFollower(broker);
                if (follower != null) {
                    Consumer consumer;
                    consumer = new Consumer(follower.getAddress(), follower.getPort(), config.getTopic(),
                            config.getPosition(), i);
                    clients.put(consumer, i);
                }
            }
        }
        return clients;
    }

    /**
     * Method to connect with leader and find follower to read from.
     *
     * @param broker leader broker found through Zookeeper
     * @return follower
     */
    protected static Membership.Broker findFollower(BrokerMetadata broker) {
        try {
            AsynchronousSocketChannel socket = AsynchronousSocketChannel.open();
            Future<Void> future = socket.connect(new InetSocketAddress(broker.getListenAddress(), broker.getListenPort()));
            future.get();
            Connection connection = new Connection(socket);
            Membership.Broker follower = null;
            while (follower == null) {
                connection.send(new byte[]{(byte) Constants.MEM_REQ});
                byte[] resp = connection.receive();
                while (resp == null) {
                    resp = connection.receive();
                }
                Membership.MemberTable memberTable = Membership.MemberTable.parseFrom(resp);
                if (memberTable.getBrokersCount() != 0) {
                    follower = memberTable.getBrokers(memberTable.getBrokersCount() - 1);
                }
            }
            connection.close();
            return follower;
        } catch (IOException | ExecutionException | InterruptedException e) {
            LOGGER.error(e.getMessage());
            return null;
        }
    }

    /**
     * Method to find broker that stores the partition.
     *
     * @param brokers   list of brokers
     * @param partition partition
     * @return broker that store the partition
     */
    protected static BrokerMetadata findBroker(Collection<BrokerMetadata> brokers, int partition) {
        Collections.reverse((List<?>) brokers);
        for (BrokerMetadata broker : brokers) {
            if (broker.getPartition() == partition % numBrokers) {
                return broker;
            }
        }
        return null;
    }
}
