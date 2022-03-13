package project2.broker;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import project2.consumer.Consumer;
import project2.producer.Producer;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

class BrokerTest {
    private final String HOST = "localhost";
    private final int PORT = 1024;
    private Broker broker;
    private final String TOPIC = "test";
    private final String KEY = "key";
    private final byte[] DATA = "this is a test".getBytes(StandardCharsets.UTF_8);

    @BeforeEach
    void setUp() {
        broker = new Broker(HOST, PORT);
        broker.start();
    }

    @AfterEach
    void tearDown() {
        broker.close();
    }

    @Test
    void testBrokerSimpleValidPubPullReq() {
        Producer producer = new Producer(HOST, PORT);
        for (int i = 0; i < 100; i++) {
            producer.send(TOPIC, KEY, DATA);
        }
        producer.close();
        Consumer consumer = new Consumer(HOST, PORT, TOPIC, 0);
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
        Thread p1 = new Thread(new SimpleProducer());
        Thread p2 = new Thread(new SimpleProducer());
        Thread p3 = new Thread(new SimpleProducer());
        p1.start();
        p2.start();
        p3.start();
        try {
            p1.join();
            p2.join();
            p3.join();
        } catch (InterruptedException e) {
            fail(e.getMessage());
        }
        p1 = new Thread(new SimpleConsumer(0));
        p2 = new Thread(new SimpleConsumer(18 * 10));
        p3 = new Thread(new SimpleConsumer(18 * 20));
        p1.start();
        p2.start();
        p3.start();
        try {
            p1.join();
            p2.join();
            p3.join();
        } catch (InterruptedException e) {
            fail(e.getMessage());
        }
    }

    @Test
    void testAsyncMultiplePubPullReq() {
        Thread p1 = new Thread(new SimpleProducer());
        Thread p2 = new Thread(new SimpleProducer());
        Thread p3 = new Thread(new SimpleProducer());
        Thread c1 = new Thread(new SimpleConsumer(0));
        Thread c2 = new Thread(new SimpleConsumer(18 * 10));
        Thread c3 = new Thread(new SimpleConsumer(18 * 20));
        p1.start();
        p2.start();
        p3.start();
        c1.start();
        c2.start();
        c3.start();
        try {
            p1.join();
            p2.join();
            p3.join();
            c1.join();
            c2.join();
            c3.join();
        } catch (InterruptedException e) {
            fail(e.getMessage());
        }
    }

    private class SimpleProducer implements Runnable {
        @Override
        public void run() {
            Producer producer = new Producer(HOST, PORT);
            for (int i = 0; i < 100; i++) {
                producer.send(TOPIC, KEY, DATA);
            }
            producer.close();
        }
    }

    private class SimpleConsumer implements Runnable {
        private final int startingPosition;

        private SimpleConsumer(int startingPosition) {
            this.startingPosition = startingPosition;
        }

        @Override
        public void run() {
            Consumer consumer = new Consumer(HOST, PORT, TOPIC, startingPosition);
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
}