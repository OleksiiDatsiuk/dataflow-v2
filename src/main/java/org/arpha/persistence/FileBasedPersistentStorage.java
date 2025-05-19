package org.arpha.persistence;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.arpha.broker.component.Partition;
import org.arpha.broker.component.Topic;
import org.arpha.configuration.ConfigurationManager;
import org.arpha.persistence.dto.TopicMetadata;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

@Slf4j
public class FileBasedPersistentStorage implements PersistentStorage {

    private final String storageDirRootPath;
    private final ObjectMapper objectMapper = new ObjectMapper();

    private static final String METADATA_FILENAME = "metadata.json";
    private static final String PARTITION_LOG_SUFFIX = ".log";

    public FileBasedPersistentStorage() {
        String configuredStorageDir = ConfigurationManager.getINSTANCE().getProperty("storage.dir", "broker_data");
        if (configuredStorageDir.isEmpty()) {
            log.warn("'storage.dir' configuration is empty, defaulting to 'broker_data'.");
            configuredStorageDir = "broker_data";
        }
        this.storageDirRootPath = configuredStorageDir;
        File rootDirFile = new File(this.storageDirRootPath);
        if (!rootDirFile.exists()) {
            if (rootDirFile.mkdirs()) {
                log.info("Created storage directory: {}", rootDirFile.getAbsolutePath());
            } else {
                log.error("Failed to create storage directory: {}. Persistence will likely fail.", rootDirFile.getAbsolutePath());
            }
        }
        log.info("FileBasedPersistentStorage initialized with storage directory: {}", rootDirFile.getAbsolutePath());
    }

    private Path getTopicPath(String topicName) {
        return Paths.get(storageDirRootPath, topicName);
    }

    private Path getPartitionLogPath(String topicName, int partitionId) {
        return getTopicPath(topicName).resolve(partitionId + PARTITION_LOG_SUFFIX);
    }

    private Path getTopicMetadataPath(String topicName) {
        return getTopicPath(topicName).resolve(METADATA_FILENAME);
    }

    @Override
    public void saveTopic(Topic topic) {
        Path topicPath = getTopicPath(topic.getName());
        try {
            if (!Files.exists(topicPath)) {
                Files.createDirectories(topicPath);
                log.info("Created directory for topic: {}", topic.getName());
            }

            TopicMetadata metadata = new TopicMetadata(topic.getName(), topic.getPartitions().size());
            Path metadataPath = getTopicMetadataPath(topic.getName());
            objectMapper.writeValue(metadataPath.toFile(), metadata);
            log.info("Saved metadata for topic: {}", topic.getName());

            for (Partition partition : topic.getPartitions()) {
                Path partitionLogPath = getPartitionLogPath(topic.getName(), partition.getPartitionId());
                if (!Files.exists(partitionLogPath)) {
                    Files.createFile(partitionLogPath);
                    log.debug("Created empty log file for partition {} of topic {}", partition.getPartitionId(), topic.getName());
                }
            }

        } catch (IOException e) {
            log.error("Failed to save topic structure (metadata or directories) for topic: {}", topic.getName(), e);
        }
    }

    @Override
    public void saveMessage(String topicName, int partitionId, String message) {
        Path partitionLogPath = getPartitionLogPath(topicName, partitionId);
        try {
            Files.createDirectories(partitionLogPath.getParent());

            String lineToAppend = message + System.lineSeparator();
            Files.write(partitionLogPath, lineToAppend.getBytes(),
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            log.trace("Saved message to topic {}, partition {}: {}", topicName, partitionId, message);
        } catch (IOException e) {
            log.error("Failed to save message to partition log: {} for topic {}, partition {}",
                    partitionLogPath.toString(), topicName, partitionId, e);
        }
    }

    @Override
    public Topic loadTopic(String topicName) {
        Path metadataPath = getTopicMetadataPath(topicName);
        if (!Files.exists(metadataPath)) {
            log.warn("Metadata file not found for topic: {}. Cannot load topic.", topicName);
            return null;
        }

        try {
            TopicMetadata metadata = objectMapper.readValue(metadataPath.toFile(), TopicMetadata.class);
            Topic topic = new Topic(metadata.getName(), metadata.getNumberOfPartitions()); // This constructor creates empty partitions

            for (Partition partition : topic.getPartitions()) {
                Path partitionLogPath = getPartitionLogPath(topicName, partition.getPartitionId());
                if (Files.exists(partitionLogPath)) {
                    try (Stream<String> lines = Files.lines(partitionLogPath)) {
                        lines.forEach(partition::addMessage);
                        log.info("Loaded {} messages for topic {}, partition {}",
                                partition.getMessages().size(), topicName, partition.getPartitionId());
                    } catch (IOException e) {
                        log.error("Failed to read messages for topic {}, partition {}. Partition data may be incomplete.",
                                topicName, partition.getPartitionId(), e);
                    }
                } else {
                    log.warn("Partition log file not found for topic {}, partition {}. Assuming empty partition.",
                            topicName, partition.getPartitionId());
                }
            }
            log.info("Successfully loaded topic {} with {} partitions.", topicName, topic.getPartitions().size());
            return topic;
        } catch (IOException e) {
            log.error("Failed to load metadata for topic: {}. Error: {}", topicName, e.getMessage(), e);
            return null;
        }
    }

    @Override
    public List<String> loadAllTopicNames() {
        File rootDir = new File(storageDirRootPath);
        if (!rootDir.exists() || !rootDir.isDirectory()) {
            log.warn("Storage directory {} does not exist or is not a directory. Cannot load topic names.", storageDirRootPath);
            return Collections.emptyList();
        }

        List<String> topicNames = new ArrayList<>();
        File[] topicDirs = rootDir.listFiles(File::isDirectory);

        if (topicDirs != null) {
            for (File topicDir : topicDirs) {
                if (Files.exists(getTopicMetadataPath(topicDir.getName()))) {
                    topicNames.add(topicDir.getName());
                } else {
                    log.warn("Directory {} found in storage root, but it does not contain a {} file. Skipping.",
                            topicDir.getName(), METADATA_FILENAME);
                }
            }
        }
        log.info("Found topic directories: {}", topicNames);
        return topicNames;
    }
}
