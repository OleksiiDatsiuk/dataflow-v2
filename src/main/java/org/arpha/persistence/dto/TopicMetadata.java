package org.arpha.persistence.dto;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class TopicMetadata {

    private final String name;
    private final int numberOfPartitions;

    @JsonCreator
    public TopicMetadata(@JsonProperty("name") String name,
                         @JsonProperty("numberOfPartitions") int numberOfPartitions) {
        this.name = name;
        this.numberOfPartitions = numberOfPartitions;
    }

    public String getName() {
        return name;
    }

    public int getNumberOfPartitions() {
        return numberOfPartitions;
    }
}
