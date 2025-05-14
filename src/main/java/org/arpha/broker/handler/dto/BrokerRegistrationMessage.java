package org.arpha.broker.handler.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class BrokerRegistrationMessage {

    private int brokerId;
    private String address;

}
