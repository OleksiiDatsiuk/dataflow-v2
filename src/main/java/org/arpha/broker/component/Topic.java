package org.arpha.broker.component;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

@Setter
@Getter
public class Topic {

    private final String name;
    private final List<Partition> partitions;

    @JsonCreator
    public Topic(@JsonProperty("name") String name, @JsonProperty("partitions") List<Partition> partitions) {
        this.name = name;
        this.partitions = partitions != null ? partitions : new ArrayList<>();
    }

    public Topic(String name, int numberOfPartitions) {
        this.name = name;
        this.partitions = new ArrayList<>();
        for (int i = 0; i < numberOfPartitions; i++) {
            partitions.add(new Partition(i));
        }
    }

    public int addMessageToPartition(String message) {
        if (partitions.isEmpty()) {
            throw new IllegalStateException("No partitions available to add a message.");
        }
        int partitionId = ThreadLocalRandom.current().nextInt(partitions.size());
        partitions.get(partitionId).addMessage(message);
        return partitionId;
    }

    public Optional<String> getNextMessageFromPartition(int partitionId) {
        return partitions.stream()
                .filter(p -> p.getPartitionId() == partitionId)
                .findFirst()
                .map(Partition::getNextMessage);
    }
}