package project2.broker;

import project2.Utils;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;

/**
 * Class that extracts byte array from broker to consumer into object with offset, key, and data.
 *
 * @author anhnguyen
 */
public class ReqRes {
    /**
     * data offset.
     */
    private final long offset;
    /**
     * key.
     */
    private final String key;
    /**
     * data.
     */
    private final byte[] data;

    /**
     * Constructor.
     * <p>
     * Extracting byte array in the form of [1-byte message type] | [8-byte offset] | [key] | 0 | [data]
     *
     * @param message byte array
     */
    public ReqRes(byte[] message) {
        int index = 1;
        byte[] offsetBytes = Utils.extractBytes(index, 9, message, false);
        this.offset = new BigInteger(offsetBytes).longValue();

        index += offsetBytes.length;
        byte[] keyBytes = Utils.extractBytes(index, message.length, message, true);
        this.key = new String(keyBytes, StandardCharsets.UTF_8);

        index += keyBytes.length + 1;
        this.data = Utils.extractBytes(index, message.length, message, false);
    }

    /**
     * Getter for offset.
     *
     * @return offset
     */
    public long getOffset() {
        return offset;
    }

    /**
     * Getter for key.
     *
     * @return key
     */
    public String getKey() {
        return key;
    }

    /**
     * Getter for data.
     *
     * @return data
     */
    public byte[] getData() {
        return data;
    }
}
