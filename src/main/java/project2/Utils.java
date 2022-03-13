package project2;

/**
 * Class that has common methods to be used by multiple classes.
 *
 * @author anhnguyen
 */
public class Utils {
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
}