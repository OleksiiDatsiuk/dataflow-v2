package org.arpha.http.endpoint.controller;

import lombok.RequiredArgsConstructor;
import org.arpha.broker.component.manager.TopicManager;
import org.arpha.http.annotation.HttpRoute;
import org.arpha.http.common.HttpMethod;
import org.arpha.http.endpoint.dto.ClusterMetadataResponse;

@RequiredArgsConstructor
public class ClusterMetadataController {

    private final TopicManager topicManager;

    @HttpRoute(path = "/metadata", method = HttpMethod.GET)
    public ClusterMetadataResponse getMetadata() {
        return ClusterMetadataResponse.fromClusterState(topicManager.getAllTopics().size());

    }

    @HttpRoute(path = "/heartbeat", method = HttpMethod.GET)
    public String heartbeat() {
        return "OK";
    }

}
