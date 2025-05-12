package org.arpha.http.endpoint.dto;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;

@Data
@AllArgsConstructor
public class TopicNameResponse {

    private List<String> topicNames;

}
