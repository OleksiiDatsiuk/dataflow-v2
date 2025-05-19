package org.arpha.broker.handler.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
@JsonInclude(JsonInclude.Include.NON_NULL)
public class BrokerPollResponse {
    private int partition;
    private long offset;
    private String message;

    private String redirectToAddress;
    private Integer redirectToBrokerId;

    public BrokerPollResponse() {}

    public BrokerPollResponse(int partition, long offset, String message) {
        this(partition, offset, message, null, null);
    }

    public BrokerPollResponse(int partition, long offset, String message, String redirectToAddress, Integer redirectToBrokerId) {
        this.partition = partition;
        this.offset = offset;
        this.message = message;
        this.redirectToAddress = redirectToAddress;
        this.redirectToBrokerId = redirectToBrokerId;
    }

}