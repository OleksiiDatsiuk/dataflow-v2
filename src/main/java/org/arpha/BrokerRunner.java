package org.arpha;

import lombok.SneakyThrows;
import org.arpha.broker.MessageBroker;
import org.arpha.broker.component.manager.InMemoryTopicManager;
import org.arpha.broker.component.manager.TopicManager;
import org.arpha.persistence.FileBasedPersistentStorage;
import org.arpha.server.DataflowHttp;

public class BrokerRunner {

    @SneakyThrows
    public static void main(String[] args) {

        TopicManager topicManager = new InMemoryTopicManager(new FileBasedPersistentStorage());

        Runnable brokerTask = () -> {
            try {
                new MessageBroker(9999, topicManager).start();
            } catch (InterruptedException e) {
                throw new IllegalArgumentException(e);
            }
        };

        Runnable brokerHttpServerTask = () -> {
            try {
                DataflowHttp dataflowHttp = new DataflowHttp(topicManager);
                dataflowHttp.start(9092);
            } catch (InterruptedException e) {
                throw new IllegalArgumentException(e);
            }
        };

        Thread brokerThread = new Thread(brokerTask);
        Thread brokerHttpServer = new Thread(brokerHttpServerTask);

        brokerThread.start();
        brokerHttpServer.start();
    }

}