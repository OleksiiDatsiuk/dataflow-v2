package org.arpha.broker.handler.dto;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;

@Getter
public class BrokerPollResponse {
    private final int partition;
    private final long messageOffset;
    private final String payload;

    @JsonCreator
    public BrokerPollResponse(@JsonProperty("partition") int partition,
                              @JsonProperty("messageOffset") long messageOffset,
                              @JsonProperty("payload") String payload) {
        this.partition = partition;
        this.messageOffset = messageOffset;
        this.payload = payload;
    }

    public boolean hasPayload() {
        return payload != null;
    }

}
