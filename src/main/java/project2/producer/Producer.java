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
    private final Logger LOGGER = LoggerFactory.getLogger(Producer.class);
    private Socket socket;
    private DataOutputStream dos;

    public Producer(String host, int port) {
        try {
            this.socket = new Socket(host, port);
            LOGGER.info("open connection with broker: " + host + ":" + port);
            this.dos = new DataOutputStream(socket.getOutputStream());
        } catch (IOException e) {
            LOGGER.error("can't open connection with broker: " + host + ":" + port + " " + e.getMessage());
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
            LOGGER.info("message sent. topic: " + topic + ", data: " + new String(data, StandardCharsets.UTF_8));
        } catch (IOException e) {
            LOGGER.error("send(): " + e.getMessage());
        }
    }

    public void close() {
        try {
            socket.shutdownInput();
            socket.shutdownOutput();
            socket.close();
            dos.close();
            LOGGER.info("closing producer");
        } catch (IOException e) {
            LOGGER.error("close(): " + e.getMessage());
        }
    }
}
