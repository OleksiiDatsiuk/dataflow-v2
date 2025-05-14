package org.arpha.broker.handler;

public enum MessageType {

    PRODUCER,
    CONSUMER,
    BROKER,
    UNKNOWN,
    BROKER_REGISTRATION,
    BROKER_ACK,
    FOLLOWER_HEARTBEAT,
    REPLICA

}