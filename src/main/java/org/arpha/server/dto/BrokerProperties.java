package org.arpha.server.dto;

import lombok.Builder;
import lombok.Data;
import org.arpha.configuration.ConfigurationManager;

@Data
@Builder
public class BrokerProperties {

    private int id;
    private String host;
    private int port;
    private int httpServerPort;
    private boolean isLeader;
    private LeaderProperties leaderProperties;

    public static BrokerProperties initialize() {
        ConfigurationManager config = ConfigurationManager.getINSTANCE();

        String baseHost = config.getProperty("broker.server.host", "localhost");
        int basePort = Integer.parseInt(config.getProperty("broker.server.port", "9999"));

        boolean isLeader = Boolean.parseBoolean(config.getProperty("broker.isLeader", "false"));

        return BrokerProperties.builder()
                .id(Integer.parseInt(config.getProperty("broker.id", "1")))
                .host(baseHost)
                .port(basePort)
                .httpServerPort(Integer.parseInt(config.getProperty("broker.server.http.port", "9092")))
                .isLeader(isLeader)
                .leaderProperties(new LeaderProperties(
                        isLeader ? baseHost : config.getProperty("leader.host", "localhost"),
                        isLeader ? basePort : Integer.parseInt(config.getProperty("leader.port", "8080"))
                ))
                .build();
    }

    public record LeaderProperties(String host, int port) { }

}
