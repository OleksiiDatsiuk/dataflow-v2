package org.arpha.http.endpoint.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class MetadataResponse {

    private ClusterMetadata clusterMetadata;
    private int numberOfTopics;


    @Data
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    public static class ClusterMetadata {
        private String clusterId;
        private String clusterName;
    }

}
