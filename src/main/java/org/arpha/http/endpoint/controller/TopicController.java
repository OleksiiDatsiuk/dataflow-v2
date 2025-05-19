package org.arpha.http.endpoint.controller;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.channel.ChannelHandlerContext;
import lombok.RequiredArgsConstructor;
import org.arpha.broker.component.Partition;
import org.arpha.broker.component.Topic;
import org.arpha.broker.component.manager.TopicManager;
import org.arpha.http.annotation.RequestBody;
import org.arpha.http.common.HttpMethod;
import org.arpha.http.annotation.HttpRoute;
import org.arpha.http.annotation.PathParam;
import org.arpha.http.endpoint.dto.CreateTopicRequest;
import org.arpha.http.endpoint.dto.CreateTopicResponse;
import org.arpha.http.endpoint.dto.ErrorResponse;
import org.arpha.http.endpoint.dto.TopicMetadataResponse;
import org.arpha.http.endpoint.dto.TopicNameResponse;
import org.arpha.http.streaming.SseStream;

@RequiredArgsConstructor
public class TopicController {

    private final TopicManager topicManager;
    private final ObjectMapper objectMapper = new ObjectMapper();

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

    @HttpRoute(path = "/topics/{topicName}/produce/{message}", method = HttpMethod.POST)
    public String produceMessages(
            @PathParam("topicName") String topicName, String requestBody
    ) {
        try {
            JsonNode root = objectMapper.readTree(requestBody);
            JsonNode messagesNode = root.get("messages");

            if (messagesNode == null || !messagesNode.isArray()) {
                return "{\"status\":\"error\",\"message\":\"Missing or invalid 'messages' array\"}";
            }

            for (JsonNode messageNode : messagesNode) {
                String message = messageNode.asText();
                topicManager.addMessageToTopic(topicName, message);
            }

            return "{\"status\":\"success\",\"produced\":" + messagesNode.size() + "}";
        } catch (Exception e) {
            return "{\"status\":\"error\",\"message\":\"" + e.getMessage() + "\"}";
        }
    }

    @HttpRoute(path = "/topics/create", method = HttpMethod.POST)
    public Object createTopic(@RequestBody CreateTopicRequest requestBody) {
        try {
            if (requestBody.getPartitions() <= 0) {
                return ErrorResponse.builder()
                        .status("ERROR")
                        .message("Partitions must be positive")
                        .build();
            }

            topicManager.createTopic(requestBody.getTopicName(), requestBody.getPartitions());

            return CreateTopicResponse.builder()
                    .status("SUCCESS")
                    .topic(requestBody.getTopicName())
                    .partitions(requestBody.getPartitions())
                    .build();
        } catch (Exception e) {
            return ErrorResponse.builder()
                    .status("ERROR")
                    .message("e.getMessage()")
                    .build();
        }
    }


    @HttpRoute(path = "/topics/{topicName}/stream", method = HttpMethod.GET)
    public SseStream streamTopic(@PathParam("topicName") String topicName, ChannelHandlerContext ctx) {
        SseStream sseStream = new SseStream(ctx);
        topicManager.subscribeToTopic(topicName, sseStream::sendEvent);
        sseStream.sendEvent("Subscribed to topic " + topicName, 1);
        return sseStream;
    }

}
