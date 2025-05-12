package org.arpha.http.endpoint.dto;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;

@Data
@AllArgsConstructor
public class TopicMetadataResponse {

    private String name;
    private int numberOfPartitions;
    private List<Integer> partitionIds;

}
