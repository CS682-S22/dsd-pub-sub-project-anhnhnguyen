package project2.broker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import project2.Constants;
import project2.consumer.PullReq;
import project2.producer.PubReq;

import java.io.*;
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

public class Broker {
    private final Logger LOGGER = LoggerFactory.getLogger(Broker.class);
    private AsynchronousServerSocketChannel server;
    private volatile boolean isRunning;
    private final ConcurrentHashMap<String, CopyOnWriteArrayList<CopyOnWriteArrayList<Long>>> topics;
    private FileOutputStream fos;
    private RandomAccessFile raf;

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
                                LOGGER.error("can't delete: " + currentFile.getPath());
                            }
                        }
                    }
                }
            }

        }
    }

    private void createFolder(String name) {
        File folder = new File(name);
        if (!folder.exists() && !folder.mkdirs()) {
            LOGGER.error("can't make folder: " + name);
        }
    }

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

    private void processRequest(Connection connection, byte[] request) {
        if (request[0] == Constants.PUB_REQ) {
            PubReq pubReq = new PubReq(request);
            String topic = pubReq.getTopic();
            String key = pubReq.getKey();
            byte[] data = pubReq.getData();
            LOGGER.info("publish request. topic: " + topic + ", key: " + key +
                    ", data: " + new String(data, StandardCharsets.UTF_8));
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

    private synchronized void processPubReq(String topic, PubReq pubReq) {
        long current = 0;
        try {
            CopyOnWriteArrayList<CopyOnWriteArrayList<Long>> indexes;
            if (topics.containsKey(topic)) {
                indexes = topics.get(topic);
            } else {
                indexes = new CopyOnWriteArrayList<>();
                topics.put(topic, indexes);
            }

            if (indexes.size() == 0) {
                String folder = Constants.TMP_FOLDER + topic + "/";
                createFolder(folder);
                File file = new File(folder + "0" + Constants.FILE_TYPE);
                fos = new FileOutputStream(file, true);
                fos.write(pubReq.getKey().getBytes(StandardCharsets.UTF_8));
                fos.write(0);
                fos.write(pubReq.getData());
                CopyOnWriteArrayList<Long> offsetList = new CopyOnWriteArrayList<>();
                offsetList.add((long) 0);
                offsetList.add((long) pubReq.getData().length + pubReq.getKey().getBytes(StandardCharsets.UTF_8).length + 1);
                indexes.add(offsetList);
                CopyOnWriteArrayList<Long> startingOffsetList = new CopyOnWriteArrayList<>();
                startingOffsetList.add((long) 0);
                indexes.add(startingOffsetList);
            } else {
                current = indexes.get(Constants.OFFSET_INDEX).get(indexes.get(Constants.OFFSET_INDEX).size() - 1);
                long offset = current + pubReq.getData().length + pubReq.getKey().getBytes(StandardCharsets.UTF_8).length + 1;
                long currentFile = indexes.get(Constants.STARTING_OFFSET_INDEX).get(indexes.get(Constants.STARTING_OFFSET_INDEX).size() - 1);
                indexes.get(Constants.OFFSET_INDEX).add(offset);
                if (offset - currentFile > Constants.SEGMENT_SIZE) {
                    String folder = Constants.LOG_FOLDER + topic + "/";
                    createFolder(folder);
                    File permFile = new File(folder + currentFile + Constants.FILE_TYPE);
                    Files.copy(new File(Constants.TMP_FOLDER + topic + "/" + currentFile + Constants.FILE_TYPE).toPath(), permFile.toPath());
                    File file = new File(Constants.TMP_FOLDER + topic + "/" + current + Constants.FILE_TYPE);
                    fos = new FileOutputStream(file, true);
                    indexes.get(Constants.STARTING_OFFSET_INDEX).add(current);
                }
                fos.write(pubReq.getKey().getBytes(StandardCharsets.UTF_8));
                fos.write(0);
                fos.write(pubReq.getData());
            }
        } catch (IOException e) {
            LOGGER.error(e.getMessage());
        }

        LOGGER.info("data added to topic: " + topic + ", key: " + pubReq.getKey() + ", offset: " + current);
    }

    private void processPullReq(Connection connection, String topic, long startingPosition) {
        if (topics.containsKey(topic)) {
            CopyOnWriteArrayList<CopyOnWriteArrayList<Long>> list = topics.get(topic);
            if (list.size() == 2) {
                CopyOnWriteArrayList<Long> offSetList = list.get(Constants.OFFSET_INDEX);
                CopyOnWriteArrayList<Long> startingOffsetList = list.get(Constants.STARTING_OFFSET_INDEX);
                int index = Arrays.binarySearch(offSetList.toArray(), startingPosition);
                if (index == offSetList.size() - 1) {
                    index = -1;
                }
                if (index >= 0) {
                    int count = 0;
                    while (index < offSetList.size() - 1 && count < Constants.NUM_RESPONSE) {
                        int fileIndex = Arrays.binarySearch(startingOffsetList.toArray(), startingPosition);
                        if (fileIndex < 0) {
                            fileIndex = -(fileIndex + 1) - 1;
                        }
                        if (Files.exists(Paths.get(Constants.LOG_FOLDER + topic + "/" + fileIndex + ".log"))) {
                            long length = offSetList.get(index + 1) - offSetList.get(index);
                            byte[] data = new byte[(int) length];
                            long position = offSetList.get(index) - startingOffsetList.get(fileIndex);
                            try {
                                raf = new RandomAccessFile(Constants.LOG_FOLDER + topic + "/" + fileIndex + ".log", "r");
                                raf.seek(position);
                                raf.read(data);
                                ByteBuffer response = ByteBuffer.allocate((int) length + 9);
                                response.put((byte) Constants.REQ_RES);
                                response.putLong(offSetList.get(index));
                                response.put(data);
                                connection.send(response.array());
                                LOGGER.info("data at offset: " + offSetList.get(index) + " from topic: " + topic + " sent");
                                index++;
                                count++;
                            } catch (IOException e) {
                                LOGGER.error(e.getMessage());
                            }
                        } else {
                            break;
                        }
                    }
                }
            }
        }
    }

    public void close() {
        try {
            LOGGER.info("closing broker");
            isRunning = false;
            server.close();
            raf.close();
            fos.close();
        } catch (IOException e) {
            LOGGER.error("close(): " + e.getMessage());
        }
    }
}
