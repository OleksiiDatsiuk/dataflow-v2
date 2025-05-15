package org.arpha.cluster;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ClusterManager {

    public enum BrokerStatus {
        SELF, LEADER, FOLLOWER, UNKNOWN
    }

    private final int brokerId;
    private final String leaderHost;
    private final int leaderPort;
    private boolean isLeader;

    private final Map<Integer, String> registeredBrokers = new ConcurrentHashMap<>();
    private final Map<Integer, BrokerStatus> brokerStatuses = new ConcurrentHashMap<>();
    private final Map<Integer, Long> heartbeatTimestamps = new ConcurrentHashMap<>();


    public ClusterManager(int brokerId, String leaderHost, int leaderPort) {
        this.brokerId = brokerId;
        this.leaderHost = leaderHost;
        this.leaderPort = leaderPort;
        this.isLeader = false;
        this.brokerStatuses.put(brokerId, BrokerStatus.SELF);
    }

    public boolean isLeader() {
        return isLeader;
    }

    public void markAsLeader() {
        this.isLeader = true;
        brokerStatuses.put(brokerId, BrokerStatus.LEADER);
    }

    public int getBrokerId() {
        return brokerId;
    }

    public String getLeaderHost() {
        return leaderHost;
    }

    public int getLeaderPort() {
        return leaderPort;
    }

    public Map<Integer, String> getRegisteredBrokers() {
        return registeredBrokers;
    }

    public void registerBroker(int id, String address, BrokerStatus brokerStatus) {
        registeredBrokers.put(id, address);
        brokerStatuses.put(id, brokerStatus);
    }

    public Map<Integer, BrokerStatus> getBrokerStatuses() {
        return brokerStatuses;
    }

    public Map<Integer, Long> getHeartbeatTimestamps() {
        return heartbeatTimestamps;
    }

}
