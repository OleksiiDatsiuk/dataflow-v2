package org.arpha;

import lombok.SneakyThrows;
import org.arpha.broker.MessageBroker;
import org.arpha.broker.component.manager.InMemoryTopicManager;
import org.arpha.broker.component.manager.TopicManager;
import org.arpha.broker.handler.MessageBrokerHandler;
import org.arpha.cluster.ClusterClient;
import org.arpha.cluster.ClusterContext;
import org.arpha.cluster.ClusterManager;
import org.arpha.configuration.ConfigurationManager;
import org.arpha.persistence.FileBasedPersistentStorage;
import org.arpha.server.DataflowHttp;
import org.arpha.server.dto.BrokerProperties;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class BrokerRunner {

    @SneakyThrows
    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Usage: java -jar your-jar.jar <path-to-properties>");
            System.exit(1);
        }

        String configFilePath = args[0];
        ConfigurationManager.overrideProperties(configFilePath);

        BrokerProperties brokerProperties = BrokerProperties.initialize();

        TopicManager topicManager = new InMemoryTopicManager(new FileBasedPersistentStorage());
        ClusterManager clusterManager = getClusterManager(brokerProperties);

        MessageBrokerHandler messageHandler = new MessageBrokerHandler(topicManager);
        ClusterClient.tryConnectToLeaderIfNotLeader(messageHandler);

        if (!clusterManager.isLeader()) {
            startHeartbeatTask(clusterManager.getBrokerId(), messageHandler);
        } else {
            startFollowerCleanupTask(clusterManager);
        }

        Runnable brokerTask = () -> {
            try {
                new MessageBroker(brokerProperties.getPort(), messageHandler).start();
            } catch (InterruptedException e) {
                throw new IllegalArgumentException(e);
            }
        };

        Runnable brokerHttpServerTask = () -> {
            try {
                DataflowHttp dataflowHttp = new DataflowHttp(topicManager);
                dataflowHttp.start(brokerProperties.getHttpServerPort());
            } catch (InterruptedException e) {
                throw new IllegalArgumentException(e);
            }
        };

        Thread brokerThread = new Thread(brokerTask);
        Thread brokerHttpServer = new Thread(brokerHttpServerTask);

        brokerThread.start();
        brokerHttpServer.start();
    }

    private static ClusterManager getClusterManager(BrokerProperties brokerProperties) {
        ClusterManager clusterManager = new ClusterManager(brokerProperties.getId(),
                brokerProperties.getLeaderProperties().host(),
                brokerProperties.getLeaderProperties().port(),
                brokerProperties.getHost(),
                brokerProperties.getPort()
        );

        if (brokerProperties.isLeader()) {
            clusterManager.markAsLeader();
            clusterManager.registerBroker(brokerProperties.getId(),
                    brokerProperties.getHost() + ":" + brokerProperties.getPort(), ClusterManager.BrokerStatus.LEADER);
        }

        ClusterContext.set(clusterManager);
        return clusterManager;
    }

    private static void startHeartbeatTask(int brokerId, MessageBrokerHandler handler) {
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleAtFixedRate(() -> {
            try {
                handler.sendFollowerHeartbeat(brokerId);
            } catch (Exception e) {
                System.err.println("Failed to send heartbeat: " + e.getMessage());
            }
        }, 0, 5, TimeUnit.SECONDS);
    }

    private static void startFollowerCleanupTask(ClusterManager clusterManager) {
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleAtFixedRate(() -> {
            long now = System.currentTimeMillis();
            clusterManager.getHeartbeatTimestamps().forEach((id, timestamp) -> {
                if ((now - timestamp) > 10_000) {
                    System.out.println("[LEADER] Broker " + id + " timed out. Removing.");
                    clusterManager.getRegisteredBrokers().remove(id);
                    clusterManager.getBrokerStatuses().remove(id);
                    clusterManager.getHeartbeatTimestamps().remove(id);
                }
            });
        }, 0, 5, TimeUnit.SECONDS);
    }

}
