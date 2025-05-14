package org.arpha.broker.component.manager;

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

public class InMemoryTopicManager implements TopicManager {

    private final Map<String, Topic> topics = new ConcurrentHashMap<>();
    private final PersistentStorage persistentStorage;
    private final Map<String, List<BiConsumer<String, Integer>>> subscribers = new ConcurrentHashMap<>();
    private final int defaultPartitions = ConfigurationManager.getINSTANCE().getIntProperty("default.partitions", 5);

    public InMemoryTopicManager(PersistentStorage persistentStorage) {
        this.persistentStorage = persistentStorage;
        loadExistingData();
    }

    private void loadExistingData() {
        for (String topicName : persistentStorage.loadAllTopicNames()) {
            Topic topic = persistentStorage.loadTopic(topicName);
            if (topic != null) {
                topics.put(topicName, topic);
            }
        }
    }

    @Override
    public void addMessageToTopic(String topicName, String message) {
        Topic topic = topics.computeIfAbsent(topicName, k -> {
            Topic newTopic = new Topic(topicName, defaultPartitions);
            persistentStorage.saveTopic(newTopic);
            return newTopic;
        });
        int partitionId = topic.addMessageToPartition(message);

        List<BiConsumer<String, Integer>> subs = subscribers.get(topicName);
        if (subs != null) {
            subs.forEach(consumer -> consumer.accept(message, partitionId));
        }

        persistentStorage.saveMessage(topicName, partitionId, message);
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
    public Optional<Topic> getTopic(String topicName) {
        return Optional.ofNullable(topics.get(topicName));
    }

    @Override
    public Topic createTopic(String topicName, int numberOfPartitions) {
        return topics.computeIfAbsent(topicName, k -> {
            Topic newTopic = new Topic(topicName, numberOfPartitions);
            persistentStorage.saveTopic(newTopic);
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
