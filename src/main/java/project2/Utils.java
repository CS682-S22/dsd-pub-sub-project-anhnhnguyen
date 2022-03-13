package project2;

public class Utils {
    public static void validateArgs(String[] args) {
        if (args.length != 1) {
            System.err.println("please provide config file!");
            System.exit(1);
        }
    }

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
