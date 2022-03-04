package project2.producer;

import com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import project2.protos.Message;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

public class Producer {
    private final Logger logger = LoggerFactory.getLogger(Producer.class);
    private Socket socket;
    private DataOutputStream dos;

    public Producer(String host, int port) {
        try {
            this.socket = new Socket(host, port);
            logger.info("open connection with broker: " + host + ":" + port);
            this.dos = new DataOutputStream(socket.getOutputStream());
        } catch (IOException e) {
            logger.error("can't open connection with broker: " + host + ":" + port + " " + e.getMessage());
        }
    }

    public void send(String topic, byte[] data) {
        Message.PublishRequest pubReq = Message.PublishRequest.newBuilder()
                .setTopic(topic)
                .setData(ByteString.copyFrom(data))
                .build();
        Message.Wrapper wrapper = Message.Wrapper.newBuilder()
                .setPubReq(pubReq)
                .build();
        try {
            dos.writeInt(wrapper.toByteArray().length);
            dos.write(wrapper.toByteArray());
            logger.info("message sent. topic: " + topic + ", data: " + new String(data, StandardCharsets.UTF_8));
        } catch (IOException e) {
            logger.error("send(): " + e.getMessage());
        }
    }

    public void close() {
        try {
            dos.close();
            socket.close();
            logger.info("closing producer");
        } catch (IOException e) {
            logger.error("close(): " + e.getMessage());
        }
    }
}
