package org.arpha.persistence;

import org.arpha.broker.component.Topic;

import java.util.List;

public interface PersistentStorage {

    void saveTopic(Topic topic);
    void saveMessage(String topicName, int partitionId, String message);
    Topic loadTopic(String topicName);
    List<String> loadAllTopicNames();

}
