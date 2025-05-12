package org.arpha.persistence;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.arpha.broker.component.Partition;
import org.arpha.broker.component.Topic;
import org.arpha.configuration.ConfigurationManager;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class FileBasedPersistentStorage implements PersistentStorage {

    private static final String STORAGE_DIR = ConfigurationManager.getINSTANCE().getProperty("storage.dir", "");
    private final ObjectMapper objectMapper = new ObjectMapper();

    public FileBasedPersistentStorage() {
        File storageDir = new File(STORAGE_DIR);
        if (!storageDir.exists()) {
            storageDir.mkdirs();
        }
    }

    @Override
    public void saveTopic(Topic topic) {
        try {
            File topicFile = new File(STORAGE_DIR, topic.getName() + ".json");
            if (!topicFile.exists()) {
                topicFile.createNewFile();
            }
            objectMapper.writeValue(topicFile, topic);
        } catch (IOException e) {
            log.error("Failed to save topic: {}", topic.getName(), e);
        }
    }

    @Override
    public void saveMessage(String topicName, int partitionId, String message) {
        try {
            File topicFile = new File(STORAGE_DIR, topicName + ".json");
            if (!topicFile.exists()) {
                log.warn("Topic file does not exist: {}", topicName);
                return;
            }
            Topic topic = objectMapper.readValue(topicFile, Topic.class);
            Partition partition = topic.getPartitions().get(partitionId);
            if (partition != null) {
                partition.addMessage(message);
                objectMapper.writeValue(topicFile, topic);
            } else {
                log.warn("Partition does not exist: {} for topic: {}", partitionId, topicName);
            }
        } catch (IOException e) {
            log.error("Failed to save message to partition: {}", partitionId, e);
        }
    }

    @Override
    public Topic loadTopic(String topicName) {
        try {
            File topicFile = new File(STORAGE_DIR, topicName + ".json");
            if (!topicFile.exists()) {
                log.warn("Topic file does not exist: {}", topicName);
                return null;
            }
            return objectMapper.readValue(topicFile, Topic.class);
        } catch (IOException e) {
            log.error("Failed to load topic: {}", topicName, e);
            return null;
        }
    }

    @Override
    public List<String> loadAllTopicNames() {
        List<String> topicNames = new ArrayList<>();
        File storageDir = new File(STORAGE_DIR);
        File[] files = storageDir.listFiles((dir, name) -> name.endsWith(".json"));
        if (files != null) {
            for (File file : files) {
                String fileName = file.getName();
                if (fileName.endsWith(".json")) {
                    topicNames.add(fileName.substring(0, fileName.length() - 5));
                }
            }
        }
        return topicNames;
    }
}
