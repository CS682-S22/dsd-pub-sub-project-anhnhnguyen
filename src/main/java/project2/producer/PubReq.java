package project2.producer;

import project2.Constants;

import java.nio.charset.StandardCharsets;

public class PubReq {
    private final String topic;
    private final String key;
    private final byte[] data;

    public PubReq(byte[] message) {
        int i = 1;
        int j = 0;

        byte[] tmp = new byte[Constants.BYTE_ALLOCATION];
        while (i < message.length && message[i] != 0) {
            tmp[j] = message[i];
            i++;
            j++;
        }
        byte[] topicBytes = new byte[j];
        System.arraycopy(tmp, 0, topicBytes, 0, j);
        this.topic = new String(topicBytes, StandardCharsets.UTF_8);

        tmp = new byte[Constants.BYTE_ALLOCATION];
        i++;
        j = 0;
        while (i < message.length && message[i] != 0) {
            tmp[j] = message[i];
            i++;
            j++;
        }
        byte[] keyBytes = new byte[j];
        System.arraycopy(tmp, 0, keyBytes, 0, j);
        this.key = new String(keyBytes, StandardCharsets.UTF_8);

        tmp = new byte[Constants.BYTE_ALLOCATION];
        i++;
        j = 0;
        while (i < message.length) {
            tmp[j] = message[i];
            i++;
            j++;
        }
        this.data = new byte[j];
        System.arraycopy(tmp, 0, data, 0, j);
    }

    public String getTopic() {
        return topic;
    }

    public String getKey() {
        return key;
    }

    public byte[] getData() {
        return data;
    }
}
