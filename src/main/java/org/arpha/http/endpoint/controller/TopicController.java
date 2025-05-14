package org.arpha.http.endpoint.controller;

import io.netty.channel.ChannelHandlerContext;
import lombok.RequiredArgsConstructor;
import org.arpha.broker.component.Partition;
import org.arpha.broker.component.Topic;
import org.arpha.broker.component.manager.TopicManager;
import org.arpha.http.common.HttpMethod;
import org.arpha.http.annotation.HttpRoute;
import org.arpha.http.annotation.PathParam;
import org.arpha.http.endpoint.dto.TopicMetadataResponse;
import org.arpha.http.endpoint.dto.TopicNameResponse;
import org.arpha.http.streaming.SseStream;

@RequiredArgsConstructor
public class TopicController {

    private final TopicManager topicManager;

    @HttpRoute(path = "/topics", method = HttpMethod.GET)
    public TopicNameResponse getTopics() {
        return new TopicNameResponse(topicManager.getAllTopics().stream().map(Topic::getName).toList());
    }

    @HttpRoute(path = "/topics/{topicName}", method = HttpMethod.GET)
    public TopicMetadataResponse getByName(@PathParam("topicName") String topicName) {
        Topic topic = topicManager.getTopic(topicName).orElseThrow();

        return new TopicMetadataResponse(
                topic.getName(),
                topic.getPartitions().size(),
                topic.getPartitions().stream().map(Partition::getPartitionId).toList()
        );
    }

    @HttpRoute(path = "/topics/{topicName}/stream", method = HttpMethod.GET)
    public SseStream streamTopic(@PathParam("topicName") String topicName, ChannelHandlerContext ctx) {
        SseStream sseStream = new SseStream(ctx);
        topicManager.subscribeToTopic(topicName, sseStream::sendEvent);
        sseStream.sendEvent("Subscribed to topic " + topicName, 1);
        return sseStream;
    }

}
