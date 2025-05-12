package org.arpha.broker.component.manager;

import org.arpha.broker.component.Topic;

import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

public interface TopicManager {

    void addMessageToTopic(String topic, String message);
    String getNextMessageFromTopic(String topic);
    Optional<Topic> getTopic(String topicName);
    Topic createTopic(String topicName, int numberOfPartitions);

    List<Topic> getAllTopics();
    void subscribeToTopic(String topicName, Consumer<String> subscriber);

}
