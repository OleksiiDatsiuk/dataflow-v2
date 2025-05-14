package org.arpha.http.endpoint.controller;

import lombok.RequiredArgsConstructor;
import org.arpha.broker.component.manager.TopicManager;
import org.arpha.cluster.ClusterContext;
import org.arpha.cluster.ClusterManager;
import org.arpha.http.annotation.HttpRoute;
import org.arpha.http.common.HttpMethod;
import org.arpha.http.endpoint.dto.ClusterMetadataResponse;

@RequiredArgsConstructor
public class ClusterMetadataController {

    private final TopicManager topicManager;

    @HttpRoute(path = "/metadata", method = HttpMethod.GET)
    public ClusterMetadataResponse getMetadata() {
        ClusterManager clusterManager = ClusterContext.get();
        return new ClusterMetadataResponse(
                new ClusterMetadataResponse.ClusterMetadata(
                        clusterManager.getBrokerId(),
                        clusterManager.getLeaderHost(),
                        clusterManager.getLeaderPort()
                ),
                topicManager.getAllTopics().size()

        );
    }

    @HttpRoute(path = "/heartbeat", method = HttpMethod.GET)
    public String heartbeat() {
        return "OK";
    }

}
