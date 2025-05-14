package org.arpha.http.endpoint.dto;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class ClusterMetadataResponse {

    private ClusterMetadata clusterMetadata;
    private int numberOfTopics;


    @Data
    @AllArgsConstructor
    public static class ClusterMetadata {

        private int clusterId;
        private String host;
        private int port;

    }

}
