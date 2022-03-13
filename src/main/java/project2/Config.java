package project2;

import java.io.File;

public class Config {
    private final String host;
    private final int port;
    private final String file;
    private final String topic;
    private final long position;

    public Config(String host, int port, String file, String topic, long position) {
        this.host = host;
        this.port = port;
        this.file = file;
        this.topic = topic;
        this.position = position;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String getFile() {
        return file;
    }

    public String getTopic() {
        return topic;
    }

    public long getPosition() {
        return position;
    }

    public void validate() {
        if (host == null || port < 1150 || port > 1174) {
            System.err.println("need to specify host name and port needs to be between 1150 and 1174");
            System.exit(1);
        }
        if (file != null) {
            File f = new File(file);
            if (!f.exists()) {
                System.err.println(file + " doesn't exist");
                System.exit(1);
            }
        }
    }
}
