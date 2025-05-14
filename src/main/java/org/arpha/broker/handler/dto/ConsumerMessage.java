package org.arpha.broker.handler.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ConsumerMessage {

    private ConsumerMessageType action;
    private String topic;
    private int partition;
    private int maxMessages;
    private long offset;
    private String consumerGroup;
    private String consumerId;

}
