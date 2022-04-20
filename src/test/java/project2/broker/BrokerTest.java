package project2.broker;

import com.google.gson.Gson;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import project2.Config;
import project2.consumer.Consumer;
import project2.producer.Producer;
import project2.zookeeper.BrokerMetadata;
import project2.zookeeper.Curator;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class BrokerTest {
    private Broker broker;
    private String host;
    private int port;
    private final String TOPIC = "test";
    private final String KEY = "key";
    private final byte[] DATA = "this is a test".getBytes(StandardCharsets.UTF_8);
    private Curator curator;

    @BeforeEach
    void setUp() {
        curator = new Curator("127.0.0.1:2181");
        try {
            Config config = new Gson().fromJson(new FileReader("configs/broker1.json"), Config.class);
            broker = new Broker(config, curator.getCuratorFramework(), curator.getObjectMapper());
            Config config1a = new Gson().fromJson(new FileReader("configs/broker1a.json"), Config.class);
            Broker broker1a = new Broker(config1a, curator.getCuratorFramework(), curator.getObjectMapper());
            broker1a.start();
            host = config.getHost();
            port = config.getPort();
            broker.start();
        } catch (FileNotFoundException e) {
            fail(e.getMessage());
        }

    }

    @AfterEach
    void tearDown() {
        broker.close();
        curator.close();
    }

    @Test
    void testBrokerSimpleValidPubPullReq() {
        Map<Integer, List<Producer>> partitionMap = findBrokers(curator);
        for (int i = 0; i < 100; i++) {
            int partition = KEY.hashCode() % partitionMap.size();
            List<Producer> producers = partitionMap.get(partition);
            for (Producer producer : producers) {
                producer.send(TOPIC, KEY, DATA, 1);
            }
        }
        for (List<Producer> producers : partitionMap.values()) {
            for (Producer producer : producers) {
                producer.close();
            }
        }
        Consumer consumer = new Consumer(host, port, TOPIC, 0, 0);
        int count = 0;
        while (count < 56) {
            byte[] message = consumer.poll(100);
            if (message != null) {
                ReqRes response = new ReqRes(message);
                assertEquals(response.getKey(), KEY);
                assertArrayEquals(response.getData(), DATA);
                count++;
            }
        }
        assertEquals(count, 56);
        consumer.close();
    }

    @Test
    void testMultiplePubPullReq() {
        List<Thread> producerThreads = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            Thread p = new Thread(new SimpleProducer());
            producerThreads.add(p);
            p.start();
        }

        try {
            for (Thread p : producerThreads) {
                p.join();
            }
        } catch (InterruptedException e) {
            fail(e.getMessage());
        }

        List<Thread> consumerThreads = new ArrayList<>();
        int times = 0;
        for (int i = 0; i < 3; i++) {
            Thread c = new Thread(new SimpleConsumer(18 * times));
            consumerThreads.add(c);
            c.start();
            times += 10;
        }

        try {
            for (Thread c : consumerThreads) {
                c.join();
            }
        } catch (InterruptedException e) {
            fail(e.getMessage());
        }
    }

    @Test
    void testAsyncMultiplePubPullReq() {
        List<Thread> producerThreads = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            Thread p = new Thread(new SimpleProducer());
            producerThreads.add(p);
            p.start();
        }
        List<Thread> consumerThreads = new ArrayList<>();
        int times = 0;
        for (int i = 0; i < 3; i++) {
            Thread c = new Thread(new SimpleConsumer(18 * times));
            consumerThreads.add(c);
            c.start();
            times += 10;
        }

        try {
            for (Thread p : producerThreads) {
                p.join();
            }
            for (Thread c : consumerThreads) {
                c.join();
            }
        } catch (InterruptedException e) {
            fail(e.getMessage());
        }
    }

    private class SimpleProducer implements Runnable {
        @Override
        public void run() {
            Map<Integer, List<Producer>> partitionMap = findBrokers(curator);
            for (int i = 0; i < 100; i++) {
                int partition = KEY.hashCode() % partitionMap.size();
                List<Producer> producers = partitionMap.get(partition);
                for (Producer producer : producers) {
                    producer.send(TOPIC, KEY, DATA, 1);
                }
            }
            for (List<Producer> producers : partitionMap.values()) {
                for (Producer producer : producers) {
                    producer.close();
                }
            }
        }
    }

    private class SimpleConsumer implements Runnable {
        private final int startingPosition;

        private SimpleConsumer(int startingPosition) {
            this.startingPosition = startingPosition;
        }

        @Override
        public void run() {
            Consumer consumer = new Consumer(host, port, TOPIC, startingPosition, 0);
            int count = 0;
            while (count < 30 - startingPosition / 18) {
                byte[] message = consumer.poll(100);
                if (message != null) {
                    count++;
                }
            }
            assertEquals(count, 30 - startingPosition / 18);
            consumer.close();
        }
    }

    private Map<Integer, List<Producer>> findBrokers(Curator curator) {
        Collection<BrokerMetadata> brokers = curator.findBrokers();
        Map<Integer, List<Producer>> partitionMap = new HashMap<>();
        for (BrokerMetadata broker : brokers) {
            Producer producer = new Producer(broker.getListenAddress(), broker.getListenPort());
            int partition = broker.getPartition();
            if (partitionMap.containsKey(partition)) {
                partitionMap.get(partition).add(producer);
            } else {
                List<Producer> producers = new ArrayList<>();
                producers.add(producer);
                partitionMap.put(partition, producers);
            }
        }
        return partitionMap;
    }
}