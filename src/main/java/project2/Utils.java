package project2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Class that has common methods to be used by multiple classes.
 *
 * @author anhnguyen
 */
public class Utils {
    /**
     * logger object.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger("operation");

    /**
     * method to validate program arguments.
     *
     * @param args program arguments
     */
    public static void validateArgs(String[] args) {
        if (args.length != 1) {
            System.err.println("please provide config file!");
            System.exit(1);
        }
    }

    /**
     * method to extract certain bytes from the byte array.
     *
     * @param index       starting index to read
     * @param length      limit of bytes to read
     * @param message     byte array to read from
     * @param isDelimited true if read should end at null terminator
     * @return byte array extracted based on the parameter conditions
     */
    public static byte[] extractBytes(int index, int length, byte[] message, boolean isDelimited) {
        int j = 0;
        byte[] tmp = new byte[Constants.BYTE_ALLOCATION];
        while (index < length) {
            if (isDelimited && message[index] == 0) {
                break;
            }
            tmp[j] = message[index];
            index++;
            j++;
        }
        byte[] bytes = new byte[j];
        System.arraycopy(tmp, 0, bytes, 0, j);
        return bytes;
    }

    /**
     * Method to traverse the folder and delete log files in the folder.
     *
     * @param dir folder name
     */
    public static void deleteFiles(File dir) {
        File[] files = dir.listFiles();
        if (files != null) {
            for (File file : files) {
                deleteFiles(file);
            }
        }
        if (dir.exists() && !dir.delete()) {
            LOGGER.error("can't delete: " + dir);
        }
    }

    /**
     * Method to create a new folder if folder doesn't exist.
     *
     * @param name folder name
     */
    public static void createFolder(String name) {
        File folder = new File(name);
        if (!folder.exists() && !folder.mkdirs()) {
            LOGGER.error("createFolder(): " + name);
        }
    }

    /**
     * Method to prepare the byte buffer in the form of 1-byte message type | 8-byte offset | byte data
     *
     * @param messageType message type
     * @param offset      offset
     * @param data        data
     * @return byte buffer
     */
    public static ByteBuffer prepareData(int messageType, long offset, byte[] data) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(data.length + 9);
        byteBuffer.put((byte) messageType);
        byteBuffer.putLong(offset);
        byteBuffer.put(data);
        return byteBuffer;
    }

    /**
     * Method to prepare the publish request in the form of 1-byte message type | topic | 0 | data | 0 | 2-byte number of partitions.
     *
     * @param data          data byte
     * @param topic         topic
     * @param numPartitions number of partitions
     * @return byte buffer
     */
    public static ByteBuffer preparePubReq(byte[] data, String topic, short numPartitions) {
        ByteBuffer response = ByteBuffer.allocate(data.length + 5 + topic.getBytes(StandardCharsets.UTF_8).length);
        response.put((byte) Constants.PUB_REQ);
        response.put(topic.getBytes(StandardCharsets.UTF_8));
        response.put((byte) 0);
        response.put(data);
        response.put((byte) 0);
        response.putShort(numPartitions);
        return response;
    }

}
