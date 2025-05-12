package org.arpha.broker.handler;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
@NoArgsConstructor
public class Message {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private MessageType type;
    private String topic;
    private String content;

    public Message(MessageType type, String topic, String content) {
        this.type = type;
        this.topic = topic;
        this.content = content;
    }

    public static Message parse(String rawMessage) {
        try {
            return OBJECT_MAPPER.readValue(rawMessage, Message.class);
        } catch (JsonMappingException e) {
            throw new IllegalArgumentException("Invalid message format: " + e.getMessage(), e);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Error processing message: " + e.getMessage(), e);
        }
    }

    public String serialize() {
        try {
            return OBJECT_MAPPER.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Error serializing message: " + e.getMessage(), e);
        }
    }
}
