package project2.broker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import project2.Constants;
import project2.Utils;
import project2.producer.PubReq;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Topic {
    /**
     * a hashmap that maps topic to a map between partition number and list of offsets in the topic and a list of first offset in segment files.
     */
    private final Map<String, Map<Integer, List<List<Long>>>> topics;

    /**
     * in-memory data structure to store message before flushing to disk.
     */
    private final Map<String, Map<Integer, List<byte[]>>> tmp;

    private final Logger LOGGER = LoggerFactory.getLogger(Topic.class);

    public Topic() {
        this.topics = new HashMap<>();
        this.tmp = new HashMap<>();
    }

    public Topic(Topic other) {
        this.topics = new HashMap<>(other.topics);
        this.tmp = new HashMap<>(other.tmp);
    }

    public void updateTopic(PubReq pubReq) {
        String topic = pubReq.getTopic();

        Map<Integer, List<List<Long>>> partitionMap;
        if (topics.containsKey(topic)) {
            partitionMap = topics.get(topic);
        } else {
            partitionMap = new HashMap<>();
            topics.put(topic, partitionMap);
        }

        int partition = pubReq.getKey().hashCode() % pubReq.getNumPartitions();
        List<List<Long>> indexes;
        if (partitionMap.containsKey(partition)) {
            indexes = partitionMap.get(partition);
        } else {
            indexes = new ArrayList<>();
            partitionMap.put(partition, indexes);
        }

        if (indexes.size() == 0) {
            initializeTopic(indexes, topic, partition);
        }

        // add next message's id to the offset list
        long current = indexes.get(Constants.OFFSET_INDEX).get(indexes.get(Constants.OFFSET_INDEX).size() - 1);
        long offset = current + pubReq.getData().length + pubReq.getKey().getBytes(StandardCharsets.UTF_8).length + 1;
        indexes.get(Constants.OFFSET_INDEX).add(offset);

        // add number of partitions info for each of the topic to the list
        if (indexes.get(Constants.NUM_PARTITIONS_INDEX).size() == 0 || indexes.get(Constants.NUM_PARTITIONS_INDEX).get(0) != pubReq.getNumPartitions()) {
            indexes.get(Constants.NUM_PARTITIONS_INDEX).add(0, (long) pubReq.getNumPartitions());
        }

        // create new segment file if necessary and add the current offset to the list of starting offsets
        long currentFile = indexes.get(Constants.STARTING_OFFSET_INDEX).get(indexes.get(Constants.STARTING_OFFSET_INDEX).size() - 1);
        if (offset - currentFile > Constants.SEGMENT_SIZE) {
            initializeSegmentFile(topic, currentFile, indexes, current, partition);
        }

        // append key and data to tmp
        ByteBuffer byteBuffer = ByteBuffer.allocate(pubReq.getKey().getBytes(StandardCharsets.UTF_8).length +
                pubReq.getData().length + 1);
        byteBuffer.put(pubReq.getKey().getBytes(StandardCharsets.UTF_8));
        byteBuffer.put((byte) 0);
        byteBuffer.put(pubReq.getData());
        tmp.get(topic).get(partition).add(byteBuffer.array());
        LOGGER.info("data added to topic: " + topic + ", partition: " + partition + ", key: " + pubReq.getKey() + ", offset: " + current);
    }

    /**
     * Method to initialize folder in tmp/ folder for a partition in a topic, file output stream to write to segment file in this folder,
     * 2 lists to store the offsets in the topic and the starting offsets for first message in each segment file.
     *
     * @param indexes   the list linked with the topic
     * @param topic     the topic
     * @param partition partition number
     */
    private void initializeTopic(List<List<Long>> indexes, String topic, int partition) {
        // initialize the tmp data structure
        Map<Integer, List<byte[]>> partitionMap;
        if (tmp.containsKey(topic)) {
            partitionMap = tmp.get(topic);
        } else {
            partitionMap = new HashMap<>();
            tmp.put(topic, partitionMap);
        }
        List<byte[]> data = new ArrayList<>();
        if (!partitionMap.containsKey(partition)) {
            partitionMap.put(partition, data);
        }

        // initialize the offset list for the topic
        List<Long> offsetList = new ArrayList<>();
        offsetList.add((long) 0);
        indexes.add(offsetList);

        // initialize the starting offset list for the topic
        List<Long> startingOffsetList = new ArrayList<>();
        startingOffsetList.add((long) 0);
        indexes.add(startingOffsetList);

        // initialize the number of partitions list for the topic
        List<Long> numPartitionsList = new ArrayList<>();
        indexes.add(numPartitionsList);
    }

    /**
     * Initialize a new segment file when the current segment file is going to reach the size limit.
     * Flush a copy of the current segment file to the log/ folder and start a new segment file.
     *
     * @param topic       topic
     * @param currentFile current segment file's first message offset
     * @param indexes     indexes mapped to the topic
     * @param current     current offset
     * @param partition   partition number
     */
    private void initializeSegmentFile(String topic, long currentFile, List<List<Long>> indexes, long current, int partition) {
        String logFolder = Constants.LOG_FOLDER + topic + Constants.PATH_STRING;
        Utils.createFolder(logFolder);
        String folder = logFolder + partition + Constants.PATH_STRING;
        Utils.createFolder(folder);
        File permFile = new File(folder + currentFile + Constants.FILE_TYPE);
        try (FileOutputStream fos = new FileOutputStream(permFile, true)) {
            List<byte[]> data = tmp.get(topic).get(partition);
            while (data.size() != 0) {
                fos.write(data.remove(0));
                fos.flush();
            }
        } catch (IOException e) {
            LOGGER.error("initializeSegmentFile(): " + e.getMessage());
        }
        LOGGER.info("flushed segment file: " + permFile.toPath());
        indexes.get(Constants.STARTING_OFFSET_INDEX).add(current);
    }

    public Map<String, Map<Integer, List<List<Long>>>> getTopics() {
        return topics;
    }

    public Map<String, Map<Integer, List<byte[]>>> getTmp() {
        return tmp;
    }
}
