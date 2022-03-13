package project2.broker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import project2.Constants;
import project2.consumer.PullReq;
import project2.producer.PubReq;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Class that listens to request to publish message from Producer and request to pull message from Consumer.
 *
 * @author anhnguyen
 */
public class Broker {
    /**
     * logger object.
     */
    private final Logger LOGGER = LoggerFactory.getLogger(Broker.class);
    /**
     * server.
     */
    private AsynchronousServerSocketChannel server;
    /**
     * state of server.
     */
    private volatile boolean isRunning;
    /**
     * a hashmap that maps topic to a list of offsets in the topic and a list of first offset in segment files.
     */
    private final ConcurrentHashMap<String, CopyOnWriteArrayList<CopyOnWriteArrayList<Long>>> topics;
    /**
     * random access file.
     */
    private RandomAccessFile raf;

    /**
     * Start the broker to listen on the given host name and port number. Also delete old log files
     * and create new folder (if necessary) at initialization (for testing purpose).
     *
     * @param host host name
     * @param port port number
     */
    public Broker(String host, int port) {
        this.topics = new ConcurrentHashMap<>();
        try {
            this.server = AsynchronousServerSocketChannel.open().bind(new InetSocketAddress(host, port));
            LOGGER.info("broker started on: " + server.getLocalAddress());
            this.isRunning = true;
        } catch (IOException e) {
            LOGGER.error("can't start broker on: " + host + ":" + port + " " + e.getMessage());
        }
        deleteFiles(Constants.TMP_FOLDER);
        deleteFiles(Constants.LOG_FOLDER);
        createFolder(Constants.TMP_FOLDER);
        createFolder(Constants.LOG_FOLDER);
    }

    /**
     * Method to traverse the folder and delete log files in the folder.
     *
     * @param name folder name
     */
    private void deleteFiles(String name) {
        File folder = new File(name);
        if (folder.exists()) {
            String[] subFolders = folder.list();
            if (subFolders != null) {
                for (String f : subFolders) {
                    File subFolder = new File(name + f);
                    String[] fileNames = subFolder.list();
                    if (fileNames != null) {
                        for (String file : fileNames) {
                            File currentFile = new File(subFolder.getPath(), file);
                            if (!currentFile.delete()) {
                                LOGGER.error("deleteFiles(): " + currentFile.getPath());
                            }
                        }
                    }
                }
            }

        }
    }

    /**
     * Method to create a new folder if folder doesn't exist.
     *
     * @param name folder name
     */
    private void createFolder(String name) {
        File folder = new File(name);
        if (!folder.exists() && !folder.mkdirs()) {
            LOGGER.error("createFolder(): " + name);
        }
    }

    /**
     * Method to start the server to accept incoming request and handle the request accordingly.
     */
    public void start() {
        server.accept(null, new CompletionHandler<>() {
            @Override
            public void completed(AsynchronousSocketChannel result, Object attachment) {
                try {
                    LOGGER.info("connection from: " + result.getRemoteAddress());
                    if (server.isOpen()) {
                        server.accept(null, this);
                    }
                    Connection connection = new Connection(result);
                    while (isRunning) {
                        byte[] request = connection.receive();
                        if (request != null) {
                            processRequest(connection, request);
                        }
                    }
                    LOGGER.info("closing socket channel");
                    result.shutdownInput();
                    result.shutdownOutput();
                    result.close();
                } catch (IOException e) {
                    LOGGER.error("start(): " + e.getMessage());
                }
            }

            @Override
            public void failed(Throwable exc, Object attachment) {
                if (server.isOpen()) {
                    LOGGER.error("fail to accept connection");
                }
            }
        });
    }

    /**
     * Method to determine which request is which and route the request to the appropriate method to handle the request.
     *
     * @param connection socket connection
     * @param request    request
     */
    private void processRequest(Connection connection, byte[] request) {
        if (request[0] == Constants.PUB_REQ) {
            PubReq pubReq = new PubReq(request);
            String topic = pubReq.getTopic();
            LOGGER.info("publish request. topic: " + topic + ", key: " + pubReq.getKey() +
                    ", data: " + new String(pubReq.getData(), StandardCharsets.UTF_8));
            processPubReq(topic, pubReq);
        }
        if (request[0] == Constants.PULL_REQ) {
            PullReq pullReq = new PullReq(request);
            String topic = pullReq.getTopic();
            long startingPosition = pullReq.getStartingPosition();
            LOGGER.info("pull request. topic: " + topic + ", starting position: " + startingPosition);
            processPullReq(connection, topic, startingPosition);
        }
    }

    /**
     * Method to process the publish request.
     * If this is the first time topic is published, then create 2 lists: offset list that holds all offsets id in the list
     * and starting offset list that holds just the offset of the first message in every segment files. Add 0 (starting offset) to both lists.
     * <p>
     * Add id of the next message to the offset list (id = current message id + current message length)
     * Write key value to the segment files in the tmp/ folder.
     * When the file is ~ the maximum allowed size file for segment file, then copy the file to the log/folder and write message to the next segment file.
     * Add offset of the message of the next segment file to the starting offset list.
     *
     * @param topic  topic
     * @param pubReq publish request
     */
    private synchronized void processPubReq(String topic, PubReq pubReq) {
        CopyOnWriteArrayList<CopyOnWriteArrayList<Long>> indexes;
        if (topics.containsKey(topic)) {
            indexes = topics.get(topic);
        } else {
            indexes = new CopyOnWriteArrayList<>();
            topics.put(topic, indexes);
        }

        if (indexes.size() == 0) {
            initializeTopic(indexes, topic);
        }

        // add next message's id to the offset list
        long current = indexes.get(Constants.OFFSET_INDEX).get(indexes.get(Constants.OFFSET_INDEX).size() - 1);
        long offset = current + pubReq.getData().length + pubReq.getKey().getBytes(StandardCharsets.UTF_8).length + 1;
        indexes.get(Constants.OFFSET_INDEX).add(offset);

        // create new segment file if necessary and add the current offset to the list of starting offsets
        long currentFile = indexes.get(Constants.STARTING_OFFSET_INDEX).get(indexes.get(Constants.STARTING_OFFSET_INDEX).size() - 1);
        if (offset - currentFile > Constants.SEGMENT_SIZE) {
            initializeSegmentFile(topic, currentFile, indexes, current);
            currentFile = current;
        }

        // write key and data to file
        String activeFile = Constants.TMP_FOLDER + topic + Constants.PATH_STRING + currentFile + Constants.FILE_TYPE;
        try (FileOutputStream fos = new FileOutputStream(activeFile, true)) {
            fos.write(pubReq.getKey().getBytes(StandardCharsets.UTF_8));
            fos.write(0);
            fos.write(pubReq.getData());
            fos.flush();
        } catch (IOException e) {
            LOGGER.error("processPubReq(): " + e.getMessage());
        }

        LOGGER.info("data added to topic: " + topic + ", key: " + pubReq.getKey() + ", offset: " + current);
    }

    /**
     * Method to initialize folder in tmp/ folder for a topic, file output stream to write to segment file in this folder,
     * 2 lists to store the offsets in the topic and the starting offsets for first message in each segment file.
     *
     * @param indexes the list linked with the topic
     * @param topic   the topic
     */
    private synchronized void initializeTopic(CopyOnWriteArrayList<CopyOnWriteArrayList<Long>> indexes, String topic) {
        // create tmp folder for the topic
        String folder = Constants.TMP_FOLDER + topic + Constants.PATH_STRING;
        createFolder(folder);

        // initialize the offset list for the topic
        CopyOnWriteArrayList<Long> offsetList = new CopyOnWriteArrayList<>();
        offsetList.add((long) 0);
        indexes.add(offsetList);

        // initialize the starting offset list for the topic
        CopyOnWriteArrayList<Long> startingOffsetList = new CopyOnWriteArrayList<>();
        startingOffsetList.add((long) 0);
        indexes.add(startingOffsetList);

        // initialize the file output stream to write to the specific file
        String fileName = folder + "0" + Constants.FILE_TYPE;
        LOGGER.info("start writing to segment file: " + fileName);
    }

    /**
     * Initialize a new segment file when the current segment file is going to reach the size limit.
     * Flush a copy of the current segment file to the log/ folder and start a new segment file.
     *
     * @param topic       topic
     * @param currentFile current segment file's first message offset
     * @param indexes     indexes mapped to the topic
     * @param current     current offset
     */
    private synchronized void initializeSegmentFile(String topic, long currentFile,
                                                    CopyOnWriteArrayList<CopyOnWriteArrayList<Long>> indexes, long current) {
        try {
            String folder = Constants.LOG_FOLDER + topic + Constants.PATH_STRING;
            createFolder(folder);
            File permFile = new File(folder + currentFile + Constants.FILE_TYPE);
            Files.copy(new File(Constants.TMP_FOLDER + topic + Constants.PATH_STRING + currentFile
                    + Constants.FILE_TYPE).toPath(), permFile.toPath());
            LOGGER.info("flushed segment file: " + permFile.toPath());
            String fileName = Constants.TMP_FOLDER + topic + Constants.PATH_STRING + current + Constants.FILE_TYPE;
            LOGGER.info("start writing to segment file: " + fileName);
            indexes.get(Constants.STARTING_OFFSET_INDEX).add(current);
        } catch (IOException e) {
            LOGGER.error("initializeSegmentFile(): " + e.getMessage());
        }
    }

    /**
     * Method to first search for the index of the starting position in the offset list and the log file that has the
     * starting position. Then read key value from the file and send up to 10 messages to the consumer.
     *
     * @param connection       connection
     * @param topic            topic
     * @param startingPosition starting position
     */
    private void processPullReq(Connection connection, String topic, long startingPosition) {
        if (topics.containsKey(topic)) {
            CopyOnWriteArrayList<CopyOnWriteArrayList<Long>> list = topics.get(topic);
            if (list.size() == 2) {
                CopyOnWriteArrayList<Long> offSetList = list.get(Constants.OFFSET_INDEX);
                CopyOnWriteArrayList<Long> startingOffsetList = list.get(Constants.STARTING_OFFSET_INDEX);

                // search for the index of the offset in the offset list, excluding the last index because it belongs to the
                // future message
                int index = Arrays.binarySearch(offSetList.toArray(), startingPosition);
                if (index == offSetList.size() - 1) {
                    index = -1;
                }
                if (index >= 0) {
                    int count = 0;
                    while (index < offSetList.size() - 1 && count < Constants.NUM_RESPONSE) {

                        // search for the file that has the offset, binarySearch method include insertionPoint which is
                        // the index where the number would be put in if it doesn't find the number. So for this application
                        // return the lower index because that's where the byte offset would be.
                        int fileIndex = Arrays.binarySearch(startingOffsetList.toArray(), offSetList.get(index));
                        if (fileIndex < 0) {
                            fileIndex = -(fileIndex + 1) - 1;
                        }
                        String fileName = Constants.LOG_FOLDER + topic + Constants.PATH_STRING + startingOffsetList.get(fileIndex) + Constants.FILE_TYPE;
                        // only expose to consumer when data is flushed to disk, so need to check the log/ folder
                        if (Files.exists(Paths.get(fileName))) {
                            if (sendData(offSetList, startingOffsetList, index, fileIndex, fileName, connection, topic)) {
                                index++;
                                count++;
                            }
                        } else {
                            break;
                        }
                    }
                }
            }
        }
    }

    /**
     * Method to send data to consumer via connection by looking up the position of data in file and read data length.
     *
     * @param offSetList         offset list
     * @param startingOffsetList starting offset list
     * @param index              index of offset in the offset list
     * @param fileIndex          index of file in the starting offset list
     * @param connection         connection
     * @param topic              topic
     * @return true if data is sent, else false
     */
    private boolean sendData(CopyOnWriteArrayList<Long> offSetList, CopyOnWriteArrayList<Long> startingOffsetList,
                             int index, int fileIndex, String fileName, Connection connection, String topic) {
        long length = offSetList.get(index + 1) - offSetList.get(index);
        byte[] data = new byte[(int) length];
        long position = offSetList.get(index) - startingOffsetList.get(fileIndex);
        try {
            raf = new RandomAccessFile(fileName, "r");
            raf.seek(position);
            raf.read(data);
            ByteBuffer response = ByteBuffer.allocate((int) length + 9);
            response.put((byte) Constants.REQ_RES);
            response.putLong(offSetList.get(index));
            response.put(data);
            connection.send(response.array());
            LOGGER.info("data at offset: " + offSetList.get(index) + " from topic: " + topic + " sent");
            return true;
        } catch (IOException e) {
            LOGGER.error(e.getMessage());
        }
        return false;
    }

    /**
     * Method to close the broker.
     */
    public void close() {
        try {
            LOGGER.info("closing broker");
            isRunning = false;
            server.close();
            raf.close();
        } catch (IOException e) {
            LOGGER.error("close(): " + e.getMessage());
        }
    }
}
