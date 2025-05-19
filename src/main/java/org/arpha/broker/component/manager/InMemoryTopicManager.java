package org.arpha.broker.component.manager;

import lombok.extern.slf4j.Slf4j;
import org.arpha.broker.component.Partition;
import org.arpha.broker.component.Topic;
import org.arpha.configuration.ConfigurationManager;
import org.arpha.persistence.PersistentStorage;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

@Slf4j
public class InMemoryTopicManager implements TopicManager {

    private final Map<String, Topic> topics = new ConcurrentHashMap<>();
    private final PersistentStorage persistentStorage;
    private final Map<String, List<BiConsumer<String, Integer>>> subscribers = new ConcurrentHashMap<>();
    private final int defaultPartitions = ConfigurationManager.getINSTANCE().getIntProperty("default.partitions", 3);

    public InMemoryTopicManager(PersistentStorage persistentStorage) {
        this.persistentStorage = persistentStorage;
        log.info("InMemoryTopicManager initializing...");
        loadExistingData();
        log.info("InMemoryTopicManager initialization complete. {} topics loaded.", topics.size());
    }

    private void loadExistingData() {
        log.info("Starting to load existing topic data...");
        List<String> topicNames = persistentStorage.loadAllTopicNames();
        if (topicNames.isEmpty()) {
            log.info("No existing topics found in persistent storage.");
            return;
        }
        for (String topicName : topicNames) {
            log.debug("Loading data for topic: {}", topicName);
            Topic topic = persistentStorage.loadTopic(topicName);
            if (topic != null) {
                topics.put(topicName, topic);
                log.info("Loaded topic '{}' into memory.", topicName);
            } else {
                log.warn("Failed to load topic '{}' from persistent storage during initialization.", topicName);
            }
        }
    }

    @Override
    public void addMessageToTopic(String topicName, String message) {
        Topic topic = topics.computeIfAbsent(topicName, k -> {
            log.info("Topic '{}' not found in memory, creating new one with {} default partitions.", k, defaultPartitions);
            Topic newTopic = new Topic(k, defaultPartitions);
            persistentStorage.saveTopic(newTopic);
            log.info("New topic '{}' structure saved to persistent storage.", k);
            return newTopic;
        });

        int partitionId = topic.addMessageToPartition(message);
        log.debug("Message added to topic '{}', partitionId {} (in-memory).", topicName, partitionId);


        persistentStorage.saveMessage(topicName, partitionId, message);
        log.debug("Message for topic '{}', partitionId {} persisted.", topicName, partitionId);

        List<BiConsumer<String, Integer>> subs = subscribers.get(topicName);
        if (subs != null) {
            subs.forEach(consumer -> consumer.accept(message, partitionId));
        }
    }

    @Override
    public String getNextMessageFromTopic(String topicName) {
        Topic topic = topics.get(topicName);
        if (topic != null) {
            for (Partition partition : topic.getPartitions()) {
                String message = partition.getNextMessage();
                if (message != null) {
                    return message;
                }
            }
        }
        return null;
    }

    @Override
    public String getMessageAtOffset(String topicName, int partitionId, long offset) {
        Topic topic = topics.get(topicName);
        if (topic == null) {
            log.warn("Topic '{}' not found for getMessageAtOffset.", topicName);
            return null;
        }

        Partition partition;
        try {
            partition = topic.getPartition(partitionId);
        } catch (IllegalArgumentException e) {
            log.warn("Invalid partitionId {} for topic '{}'.", partitionId, topicName);
            return null;
        }

        // Messages are now loaded into partition.getMessages() (ConcurrentLinkedQueue)
        // Converting to list each time for offset access is inefficient for large partitions.
        // For true offset-based access on potentially very large persisted logs,
        // you'd read directly from the file for the given offset, or use an indexed in-memory structure.
        // However, for now, working with the in-memory queue:
        if (offset < 0) return null;

        // ConcurrentLinkedQueue does not support direct index access.
        // We need to iterate or convert to a list. If messages are loaded into a List in Partition on startup, this is easier.
        // Let's assume Partition.getMessages() provides access that can be used.
        // If Partition.messages is ConcurrentLinkedQueue:
        if (offset >= partition.getMessages().size()) { // size() can be O(N) for some concurrent queues, but for CLQ it's O(1) usually.
            return null;
        }
        int i = 0;
        for (String msg : partition.getMessages()) {
            if (i == offset) {
                return msg;
            }
            i++;
        }
        return null;
    }


    @Override
    public Optional<Topic> getTopic(String topicName) {
        return Optional.ofNullable(topics.get(topicName));
    }

    @Override
    public Topic createTopic(String topicName, int numberOfPartitions) {
        return topics.computeIfAbsent(topicName, k -> {
            log.info("Creating new topic '{}' with {} partitions via createTopic method.", k, numberOfPartitions);
            Topic newTopic = new Topic(k, numberOfPartitions);
            persistentStorage.saveTopic(newTopic);
            log.info("New topic '{}' ({} partitions) structure saved via createTopic method.", k, numberOfPartitions);
            return newTopic;
        });
    }

    @Override
    public List<Topic> getAllTopics() {
        return List.copyOf(topics.values());
    }

    @Override
    public void subscribeToTopic(String topicName, BiConsumer<String, Integer> subscriber) {
        subscribers.computeIfAbsent(topicName, k -> new ArrayList<>()).add(subscriber);
    }
}