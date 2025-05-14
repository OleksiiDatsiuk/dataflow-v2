package org.arpha.http.endpoint.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.arpha.cluster.ClusterContext;
import org.arpha.cluster.ClusterManager;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Data
@AllArgsConstructor
public class ClusterMetadataResponse {

    private ClusterMetadata clusterMetadata;
    private int numberOfTopics;
    private int numberOfBrokers;
    private List<BrokerInfo> connectedBrokers;

    public static ClusterMetadataResponse fromClusterState(int numberOfTopics) {
        ClusterManager clusterManager = ClusterContext.get();

        Map<Integer, String> brokers = clusterManager.getRegisteredBrokers();
        Map<Integer, ClusterManager.BrokerStatus> statuses = clusterManager.getBrokerStatuses();
        Map<Integer, Long> heartbeats = clusterManager.getHeartbeatTimestamps();

        long now = System.currentTimeMillis();
        final long heartbeatTimeoutMillis = 10_000;

        List<BrokerInfo> brokerList = brokers.entrySet().stream()
                .map(entry -> {
                    int brokerId = entry.getKey();
                    long lastHeartbeat = heartbeats.getOrDefault(brokerId, -1L);
                    String status = (lastHeartbeat != -1L && (now - lastHeartbeat) <= heartbeatTimeoutMillis)
                            ? "ONLINE" : "OFFLINE";

                    return new BrokerInfo(
                            brokerId,
                            entry.getValue(),
                            status,
                            lastHeartbeat
                    );
                })
                .collect(Collectors.toList());

        ClusterMetadata metadata = new ClusterMetadata(
                clusterManager.getBrokerId(),
                clusterManager.getLeaderHost(),
                clusterManager.getLeaderPort()
        );

        return new ClusterMetadataResponse(
                metadata,
                numberOfTopics,
                brokerList.size(),
                brokerList
        );
    }

    @Data
    @AllArgsConstructor
    public static class ClusterMetadata {
        private int clusterId;
        private String host;
        private int port;
    }

    @Data
    @AllArgsConstructor
    public static class BrokerInfo {
        private int brokerId;
        private String address;
        private String status;
        private long lastHeartbeat;
    }

}
