package org.arpha.broker.component;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;

import java.util.concurrent.ConcurrentLinkedQueue;

@Getter
@Setter
public class Partition {

    private int partitionId;
    private ConcurrentLinkedQueue<String> messages;

    @JsonCreator
    public Partition(@JsonProperty("partitionId") int partitionId, @JsonProperty("messages") ConcurrentLinkedQueue<String> messages, @JsonProperty("nextMessage") String nextMessage) {
        this.partitionId = partitionId;
        this.messages = messages != null ? messages : new ConcurrentLinkedQueue<>();
    }

    public Partition(int partitionId) {
        this.partitionId = partitionId;
        this.messages = new ConcurrentLinkedQueue<>();
    }

    public void addMessage(String message) {
        messages.add(message);
    }

    public String getNextMessage() {
        return messages.poll();
    }

}
