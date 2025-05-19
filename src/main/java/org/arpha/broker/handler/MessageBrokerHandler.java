package org.arpha.broker.handler;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import lombok.extern.slf4j.Slf4j;
import org.arpha.broker.component.Topic;
import org.arpha.broker.component.manager.TopicManager;
import org.arpha.broker.handler.dto.BrokerPollResponse;
import org.arpha.broker.handler.dto.BrokerRegistrationMessage;
import org.arpha.broker.handler.dto.ConsumerMessage;
import org.arpha.broker.handler.dto.ConsumerMessageType;
import org.arpha.cluster.ClusterContext;
import org.arpha.cluster.ClusterManager;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

@Slf4j
@ChannelHandler.Sharable
public class MessageBrokerHandler extends SimpleChannelInboundHandler<String> {

    private static volatile boolean isRegistered = false;
    private static final ObjectMapper mapper = new ObjectMapper();

    private final TopicManager topicManager;
    private final Map<Channel, MessageType> connectionRegistry;
    private final BlockingQueue<MessageTask> messageQueue;
    private final ExecutorService executorService;
    private final Map<String, Map<Integer, Long>> committedOffsets = new ConcurrentHashMap<>();
    private final Map<String, Map<String, Integer>> consumerAssignments = new ConcurrentHashMap<>();
    private final Map<Integer, CompletableFuture<String>> registrationAcks = new ConcurrentHashMap<>();
    private final Map<String, Map<String, Long>> consumerHeartbeats = new ConcurrentHashMap<>();
    private final Map<String, Map<Integer, Integer>> partitionAssignments = new ConcurrentHashMap<>();

    private volatile Channel leaderChannel;

    private final Map<Integer, Channel> followerConnections = new ConcurrentHashMap<>();
    private final EventLoopGroup followerReplicationGroup;
    private final EventLoopGroup clientConnectionGroup;

    public MessageBrokerHandler(TopicManager topicManager) {
        this.topicManager = topicManager;
        this.connectionRegistry = new ConcurrentHashMap<>();
        this.messageQueue = new LinkedBlockingQueue<>();
        this.executorService = Executors.newFixedThreadPool(10);

        ClusterManager currentClusterManager = ClusterContext.get();
        if (currentClusterManager != null && currentClusterManager.isLeader()) {
            this.followerReplicationGroup = new NioEventLoopGroup();
            this.clientConnectionGroup = null;
            log.info("Broker ID {} is Leader. Initialized followerReplicationGroup.", currentClusterManager.getBrokerId());
        } else {
            this.followerReplicationGroup = null;
            this.clientConnectionGroup = new NioEventLoopGroup(1);
            if (currentClusterManager != null) {
                log.info("Broker ID {} is Follower. Initialized clientConnectionGroup.", currentClusterManager.getBrokerId());
            } else {
                log.info("Broker (ClusterManager not yet fully available) is likely Follower. Initialized clientConnectionGroup.");
            }
        }
        startMessageProcessing();
    }

    public void setLeaderChannel(Channel channel) {
        this.leaderChannel = channel;
        if (channel != null && channel.remoteAddress() != null) {
            log.info("Leader channel for this follower set to: {}", channel.remoteAddress());
        }
        if (channel != null) {
            this.connectionRegistry.put(channel, MessageType.BROKER);
        }
    }


    @Override
    protected void channelRead0(ChannelHandlerContext ctx, String rawMessage) {
        try {
            messageQueue.put(new MessageTask(ctx, rawMessage));
        } catch (InterruptedException e) {
            log.error("Failed to enqueue message: {}", rawMessage, e);
            Thread.currentThread().interrupt();
        }
    }

    private void startMessageProcessing() {
        int numProcessingThreads = 4;
        for (int i = 0; i < numProcessingThreads; i++) {
            executorService.submit(() -> {
                while (!Thread.currentThread().isInterrupted()) {
                    try {
                        MessageTask task = messageQueue.take();
                        processMessageTask(task);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        log.error("Message processing thread interrupted, stopping.", e);
                        break;
                    } catch (Exception e) {
                        log.error("Exception during message task processing", e);
                    }
                }
                log.info("Message processing thread finished.");
            });
        }
    }

    public CompletableFuture<String> registerWithLeader(Channel channel, int brokerId, String address) {
        if (isRegistered) {
            log.warn("Broker {} attempting to register with leader again. Current isRegistered={}. This might be a re-registration attempt.", brokerId, isRegistered);
        }

        Message registration = new Message(MessageType.BROKER_REGISTRATION, null,
                String.format("{\"brokerId\":%d,\"address\":\"%s\"}", brokerId, address));
        CompletableFuture<String> ackFuture = new CompletableFuture<>();

        CompletableFuture<String> previousAck = registrationAcks.put(brokerId, ackFuture);
        if (previousAck != null && !previousAck.isDone()) {
            previousAck.completeExceptionally(new IllegalStateException("Superseded by new registration attempt"));
        }

        log.info("Follower {} sending registration to leader via channel {}: {}", brokerId, channel.id(), registration.serialize());
        channel.writeAndFlush(registration.serialize() + "\n").addListener(writeFuture -> {
            if (!writeFuture.isSuccess()) {
                log.error("Follower {} failed to send registration message to leader. Channel: {}, Cause: {}", brokerId, channel.id(), writeFuture.cause().getMessage(), writeFuture.cause());
                ackFuture.completeExceptionally(writeFuture.cause());
                registrationAcks.remove(brokerId, ackFuture);
            } else {
                log.info("Follower {} successfully sent registration request to leader. Waiting for ACK.", brokerId);
            }
        });
        return ackFuture;
    }

    private void processMessageTask(MessageTask task) {
        try {
            Message message = Message.parse(task.getRawMessage());
            MessageType messageType = message.getType();
            log.debug("Received message on channel {} of type {}: {}", task.getCtx().channel().id(), messageType, task.getRawMessage());
            connectionRegistry.put(task.getCtx().channel(), messageType);

            switch (messageType) {
                case PRODUCER:
                    handleProducerMessage(task.getCtx(), message);
                    break;
                case CONSUMER:
                    handleConsumerMessage(task.getCtx(), message);
                    break;
                case BROKER:
                    handleBrokerMessage(task.getCtx(), message);
                    break;
                case BROKER_REGISTRATION:
                    handleBrokerRegistrationMessage(task.getCtx(), message);
                    break;
                case BROKER_ACK:
                    handleBrokerAck(task.getCtx(), message);
                    break;
                case FOLLOWER_HEARTBEAT:
                    handleFollowerHeartbeat(task.getCtx(), message);
                    break;
                case REPLICA:
                    handleReplicaMessage(task.getCtx(), message);
                    break;
                default:
                    handleUnknownMessage(task.getCtx(), message);
                    break;
            }
            log.debug("Successfully processed message type {}: {}", messageType, (message.getTopic() != null ? message.getTopic() : "N/A"));
        } catch (IllegalArgumentException e) {
            log.error("Invalid message format received on channel {}: {}. Error: {}",task.getCtx().channel().id(), task.getRawMessage(), e.getMessage(), e);
        } catch (Exception e) {
            log.error("Failed to process message on channel {}: {}. Error: {}", task.getCtx().channel().id(), task.getRawMessage(), e.getMessage(), e);
        }
    }

    private void handleReplicaMessage(ChannelHandlerContext ctx, Message message) {
        String topic = message.getTopic();
        topicManager.addMessageToTopic(topic, message.getContent());
        log.info("Follower: Replicated message for topic {} successfully written.", topic);
    }

    private void handleBrokerAck(ChannelHandlerContext ctx, Message message) {
        log.info("Follower received BROKER_ACK from leader (channel {}): {}", ctx.channel().id(), message.getContent());
        try {
            ClusterManager clusterManager = ClusterContext.get();
            if (clusterManager != null && !clusterManager.isLeader()) {
                CompletableFuture<String> ackFuture = registrationAcks.get(clusterManager.getBrokerId());
                if (ackFuture != null) {
                    if (ackFuture.complete(message.getContent())) {
                        isRegistered = true;
                        log.info("Broker {} successfully registered with leader. isRegistered = true. ACK content: {}", clusterManager.getBrokerId(), message.getContent());
                        registrationAcks.remove(clusterManager.getBrokerId(), ackFuture);
                    } else {
                        log.warn("Broker {} received BROKER_ACK, but ackFuture was already completed. Content: {}", clusterManager.getBrokerId(), message.getContent());
                    }
                } else {
                    log.warn("Broker {} received BROKER_ACK but no pending registration future found. Message: {}", clusterManager.getBrokerId(), message.getContent());
                }
            } else if (clusterManager != null && clusterManager.isLeader()){
                log.warn("Leader {} received a BROKER_ACK. This is unexpected. Message: {}", clusterManager.getBrokerId(), message.getContent());
            } else {
                log.error("ClusterManager not available or role unclear while handling Broker ACK.");
            }
            connectionRegistry.put(ctx.channel(), MessageType.BROKER);
        } catch (Exception e) {
            log.error("Error processing BROKER_ACK: {}", message.getContent(), e);
        }
    }


    private void handleProducerMessage(ChannelHandlerContext ctx, Message message) {
        String topic = message.getTopic();
        topicManager.addMessageToTopic(topic, message.getContent());

        ClusterManager clusterManager = ClusterContext.get();
        if (clusterManager.isLeader()) {
            log.debug("Leader handling producer message for topic {}. Replicating...", topic);
            if (!partitionAssignments.containsKey(topic)) {
                Optional<Topic> topicOpt = topicManager.getTopic(topic);
                topicOpt.ifPresent(t -> {
                    List<Integer> brokerIds = new ArrayList<>(clusterManager.getRegisteredBrokers().keySet());
                    brokerIds.add(clusterManager.getBrokerId());
                    assignPartitionsToBrokers(topic, brokerIds, t.getPartitions().size());
                });
            }

            clusterManager.getRegisteredBrokers().keySet().forEach(followerBrokerId -> {
                replicateMessageToFollower(followerBrokerId, message);
            });
        } else {
            log.warn("Follower received a PRODUCER message directly. Topic: {}", topic);
        }
    }

    private void connectToFollowerForReplication(int followerBrokerId, String followerAddress) {
        ClusterManager clusterManager = ClusterContext.get();
        if (!clusterManager.isLeader()) {
            log.warn("Non-leader broker ({}) attempted to connect to follower {}.", clusterManager.getBrokerId(), followerBrokerId);
            return;
        }

        Channel existingChannel = followerConnections.get(followerBrokerId);
        if (existingChannel != null && existingChannel.isActive()) {
            log.info("Leader already has an active replication connection to follower {} at {}.", followerBrokerId, followerAddress);
            return;
        }
        if (this.followerReplicationGroup == null || this.followerReplicationGroup.isShutdown() || this.followerReplicationGroup.isShuttingDown()) {
            log.error("Leader's followerReplicationGroup is not available. Cannot connect to follower {}.", followerBrokerId);
            return;
        }

        log.info("Leader attempting to establish replication connection to follower {} at {}", followerBrokerId, followerAddress);
        String[] hostPort = followerAddress.split(":");
        String host = hostPort[0];
        int port = Integer.parseInt(hostPort[1]);

        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(this.followerReplicationGroup)
                .channel(NioSocketChannel.class)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ChannelPipeline pipeline = ch.pipeline();
                        pipeline.addLast(new io.netty.handler.codec.LineBasedFrameDecoder(8192));
                        pipeline.addLast(new StringEncoder());
                        pipeline.addLast(new StringDecoder());
                    }
                });

        try {
            ChannelFuture future = bootstrap.connect(host, port).syncUninterruptibly();
            if (future.isSuccess()) {
                Channel newFollowerChannel = future.channel();
                log.info("Leader successfully connected to follower {} at {} for replication via channel {}.", followerBrokerId, followerAddress, newFollowerChannel.id());
                Channel oldChannel = followerConnections.put(followerBrokerId, newFollowerChannel);
                if(oldChannel != null && oldChannel.isActive()){
                    log.warn("Replaced an existing active channel for follower {} during connect.", followerBrokerId);
                    oldChannel.close();
                }
                newFollowerChannel.closeFuture().addListener(closeFuture -> {
                    log.warn("Replication channel {} to follower {} (address: {}) closed.", newFollowerChannel.id(), followerBrokerId, followerAddress);
                    followerConnections.remove(followerBrokerId, newFollowerChannel);
                });
            } else {
                log.warn("Leader failed to connect to follower {} at {}. Reason: {}", followerBrokerId, followerAddress, future.cause() != null ? future.cause().getMessage() : "Unknown");
            }
        } catch (Exception e) {
            log.error("Exception while leader connecting to follower {} at {}: {}", followerBrokerId, followerAddress, e.getMessage(), e);
        }
    }


    private void replicateMessageToFollower(int followerBrokerId, Message originalMessage) {
        ClusterManager clusterManager = ClusterContext.get();
        if (!clusterManager.isLeader()) return;

        Channel followerChannel = followerConnections.get(followerBrokerId);

        if (followerChannel != null && followerChannel.isActive()) {
            Message replica = new Message(MessageType.REPLICA, originalMessage.getTopic(), originalMessage.getContent());
            log.debug("Leader replicating message to follower {} via channel {}: Topic {}, Content Snippet: {}",
                    followerBrokerId, followerChannel.id(), replica.getTopic(),
                    (replica.getContent() != null && replica.getContent().length() > 50 ? replica.getContent().substring(0, 50) + "..." : replica.getContent()));

            followerChannel.writeAndFlush(replica.serialize() + "\n").addListener(future -> {
                if (!future.isSuccess()) {
                    log.warn("Leader failed to send replica to follower {} via channel {}: {}. Closing channel.", followerBrokerId, followerChannel.id(), future.cause().getMessage(), future.cause());
                    followerChannel.close();
                    followerConnections.remove(followerBrokerId, followerChannel);
                } else {
                    log.debug("Leader successfully sent replica to follower {} via channel {}", followerBrokerId, followerChannel.id());
                }
            });
        } else {
            log.warn("Leader: No active replication channel to follower {}. Message for topic {} not replicated. Attempting to establish connection.",
                    followerBrokerId, originalMessage.getTopic());
            if (followerChannel != null) {
                followerConnections.remove(followerBrokerId, followerChannel);
            }
            String followerAddress = clusterManager.getRegisteredBrokers().get(followerBrokerId);
            if (followerAddress != null) {
                connectToFollowerForReplication(followerBrokerId, followerAddress);
            } else {
                log.warn("Leader: Address for follower {} not found in registered brokers. Cannot attempt to connect for replication.", followerBrokerId);
            }
        }
    }


    private void handleConsumerMessage(ChannelHandlerContext ctx, Message message) {
        ConsumerMessage consumerMsg = null;
        String actionTypeInfo = "UNKNOWN_ACTION";

        try {
            consumerMsg = mapper.readValue(message.getContent(), ConsumerMessage.class);
            actionTypeInfo = (consumerMsg.getAction() != null) ? consumerMsg.getAction().toString() : "PARSED_ACTION_NULL";

            switch (consumerMsg.getAction()) {
                case POLL:
                    handlePoll(ctx, consumerMsg);
                    break;
                case COMMIT:
                    handleCommit(ctx, consumerMsg);
                    break;
                case HEARTBEAT:
                    handleConsumerHeartbeat(consumerMsg);
                    break;
                default:
                    log.warn("Unknown consumer action on channel {}: {}", ctx.channel().id(), consumerMsg.getAction());
                    break;
            }
        } catch (Exception e) {
            log.error("Failed to process CONSUMER message (action: {}) on channel {}: Content: '{}'. Error: {}",
                    actionTypeInfo, ctx.channel().id(), message.getContent(), e.getMessage(), e);

            if (consumerMsg != null && consumerMsg.getAction() == ConsumerMessageType.POLL) {
                try {
                    String errorPayload = String.format("Error processing POLL request: %s", e.getMessage().replace("\"", "\\\""));
                    BrokerPollResponse errResponse = new BrokerPollResponse(-1, -1, errorPayload, null, null);
                    String errorJson = mapper.writeValueAsString(errResponse);
                    ctx.writeAndFlush(errorJson + "\n").addListener(future -> {
                        if (!future.isSuccess()) {
                            log.warn("Failed to send error POLL response to consumer on channel {}, closing connection.", ctx.channel().id(), future.cause());
                            ctx.close();
                        }
                    });
                } catch (Exception ex) {
                    log.error("Error serializing/sending error POLL response to consumer on channel {}. Closing connection.", ctx.channel().id(), ex);
                    ctx.close();
                }
            } else {
                log.warn("Closing connection with consumer on channel {} due to error processing {} request.", ctx.channel().id(), actionTypeInfo);
                ctx.close();
            }
        }
    }

    private void handlePoll(ChannelHandlerContext ctx, ConsumerMessage msg) throws Exception {
        String topic = msg.getTopic();
        String group = msg.getConsumerGroup();
        String consumerId = msg.getConsumerId();

        consumerHeartbeats
                .computeIfAbsent(group, g -> new ConcurrentHashMap<>())
                .put(consumerId, System.currentTimeMillis());

        maybeRebalance(group, topic);

        Integer assignedPartition = consumerAssignments
                .getOrDefault(group, Map.of())
                .get(consumerId);

        if (assignedPartition == null) {
            log.warn("Consumer {} in group {} for topic {} was not assigned a partition. Sending empty response.", consumerId, group, topic);
            BrokerPollResponse emptyResponse = new BrokerPollResponse(-1, -1, null, null, null);
            ctx.writeAndFlush(mapper.writeValueAsString(emptyResponse) + "\n");
            return;
        }

        final int partition = assignedPartition;
        long committedOffset = committedOffsets
                .getOrDefault(group, Map.of())
                .getOrDefault(partition, 0L);

        BrokerPollResponse response;
        ClusterManager currentClusterManager = ClusterContext.get();

        if (currentClusterManager.isLeader()) {
            Integer ownerBrokerId = partitionAssignments
                    .getOrDefault(topic, Map.of())
                    .get(partition);

            if (ownerBrokerId == null) {
                log.warn("Leader: Partition {} of topic {} is not assigned. Sending empty for consumer {}.", partition, topic, consumerId);
                response = new BrokerPollResponse(partition, -1, null, null, null);
            } else if (ownerBrokerId.equals(currentClusterManager.getBrokerId())) {
                log.debug("Leader owns partition {}. Serving data for topic {} to consumer {}.", partition, topic, consumerId);
                String nextMessage = topicManager.getMessageAtOffset(topic, partition, committedOffset);
                response = (nextMessage != null) ? new BrokerPollResponse(partition, committedOffset, nextMessage)
                        : new BrokerPollResponse(partition, -1, null);
            } else {
                String followerAddress = currentClusterManager.getRegisteredBrokers().get(ownerBrokerId);
                if (followerAddress != null) {
                    log.info("Leader: Redirecting consumer {} for topic {} partition {} to follower {} at {}.",
                            consumerId, topic, partition, ownerBrokerId, followerAddress);
                    response = new BrokerPollResponse(partition, -1, null, followerAddress, ownerBrokerId);
                } else {
                    log.warn("Leader: Follower {} owns topic {} partition {} but address not found. Sending empty for consumer {}.",
                            ownerBrokerId, topic, partition, consumerId);
                    response = new BrokerPollResponse(partition, -1, null, null, null);
                }
            }
        } else {
            log.debug("Follower handling poll for topic {}, partition {}, consumer {}.", topic, partition, consumerId);
            String nextMessage = topicManager.getMessageAtOffset(topic, partition, committedOffset);
            response = (nextMessage != null) ? new BrokerPollResponse(partition, committedOffset, nextMessage)
                    : new BrokerPollResponse(partition, -1, null);
        }
        ctx.writeAndFlush(mapper.writeValueAsString(response) + "\n");
    }


    private void handleCommit(ChannelHandlerContext ctx, ConsumerMessage msg) {
        log.info("Received COMMIT for topic={}, group={}, partition={}, offset={}",
                msg.getTopic(), msg.getConsumerGroup(), msg.getPartition(), msg.getOffset());
        committedOffsets
                .computeIfAbsent(msg.getConsumerGroup(), k -> new ConcurrentHashMap<>())
                .put(msg.getPartition(), msg.getOffset());
    }


    private void handleConsumerHeartbeat(ConsumerMessage msg) {
        String group = msg.getConsumerGroup();
        String consumerId = msg.getConsumerId();
        consumerHeartbeats
                .computeIfAbsent(group, g -> new ConcurrentHashMap<>())
                .put(consumerId, System.currentTimeMillis());
        log.debug("Heartbeat received from consumer {} in group {}", consumerId, group);
    }

    private void handleBrokerMessage(ChannelHandlerContext ctx, Message message) {
        log.info("Handling generic BROKER message from channel {}: {}", ctx.channel().id(), message.getContent());
    }

    private void handleBrokerRegistrationMessage(ChannelHandlerContext ctx, Message message) {
        if (message.getContent() == null || message.getContent().isBlank()) {
            log.warn("Leader received empty content in BROKER_REGISTRATION message from channel {}. Ignoring.", ctx.channel().id());
            return;
        }

        ClusterManager clusterManager = ClusterContext.get();
        if (!clusterManager.isLeader()) {
            log.warn("Non-leader broker received broker registration on channel {}. Ignoring. Message: {}", ctx.channel().id(), message.getContent());
            return;
        }

        try {
            BrokerRegistrationMessage dto = mapper.readValue(message.getContent(), BrokerRegistrationMessage.class);
            log.info("Leader received registration request from brokerId {} at address {} via channel {}", dto.getBrokerId(), dto.getAddress(), ctx.channel().id());

            clusterManager.registerBroker(dto.getBrokerId(), dto.getAddress(), ClusterManager.BrokerStatus.FOLLOWER);
            log.info("Leader registered follower broker {} with address {}.", dto.getBrokerId(), dto.getAddress());

            connectToFollowerForReplication(dto.getBrokerId(), dto.getAddress());

            Message ack = new Message(MessageType.BROKER_ACK, null,
                    String.format("{\"status\":\"SUCCESS\", \"message\":\"ACK: Registered broker %d successfully with leader %d\"}",
                            dto.getBrokerId(), clusterManager.getBrokerId()));
            if (ctx.channel().isActive()) {
                ctx.writeAndFlush(ack.serialize() + "\n").addListener(future -> {
                    if (future.isSuccess()) {
                        log.info("Leader sent ACK for registration to broker {} via channel {}", dto.getBrokerId(), ctx.channel().id());
                    } else {
                        log.warn("Leader failed to send ACK for registration to broker {} via channel {}. Cause: {}", dto.getBrokerId(), ctx.channel().id(), future.cause().getMessage(), future.cause());
                    }
                });
            } else {
                log.warn("Leader: Channel {} for broker {} registration was inactive when trying to send ACK.", ctx.channel().id(), dto.getBrokerId());
            }

        } catch (Exception e) {
            log.error("Leader failed to process BROKER_REGISTRATION message from channel {}: {}. Error: {}", ctx.channel().id(), message.getContent(), e.getMessage(), e);
            if (ctx.channel().isActive()) {
                try {
                    Message nack = new Message(MessageType.BROKER_ACK, null,
                            String.format("{\"status\":\"FAILURE\", \"message\":\"Error processing registration: %s\"}", e.getMessage()));
                    ctx.writeAndFlush(nack.serialize() + "\n");
                } catch (Exception ex) {
                    log.error("Leader failed to send NACK for registration on channel {}. Error: {}", ctx.channel().id(), ex.getMessage(), ex);
                }
            }
        }
    }

    private void handleUnknownMessage(ChannelHandlerContext ctx, Message message) {
        log.warn("Received unknown message type on channel {}: {} Content: {}", ctx.channel().id(), message.getType(), message.getContent());
    }

    private void handleFollowerHeartbeat(ChannelHandlerContext ctx, Message message) {
        ClusterManager clusterManager = ClusterContext.get();
        if (!clusterManager.isLeader()) {
            log.warn("Non-leader received follower heartbeat on channel {}. Ignoring.", ctx.channel().id());
            return;
        }
        try {
            Map<String, Object> payload = mapper.readValue(message.getContent(), Map.class);
            Integer brokerId = (Integer) payload.get("brokerId");

            if (brokerId != null) {
                clusterManager.getHeartbeatTimestamps().put(brokerId, System.currentTimeMillis());
                log.debug("Leader received heartbeat from follower brokerId {} via channel {}.", brokerId, ctx.channel().id());

                if (!followerConnections.containsKey(brokerId) || !followerConnections.get(brokerId).isActive()) {
                    String followerAddress = clusterManager.getRegisteredBrokers().get(brokerId);
                    if (followerAddress != null) {
                        log.info("Heartbeat from follower {} received, but replication channel is down/missing. Attempting to reconnect to {}.", brokerId, followerAddress);
                        connectToFollowerForReplication(brokerId, followerAddress);
                    } else {
                        log.warn("Heartbeat from follower {} (channel {}) received, but address not found. Cannot attempt reconnection.", brokerId, ctx.channel().id());
                    }
                }
            } else {
                log.warn("Leader received follower heartbeat with missing brokerId in payload: {} from channel {}", message.getContent(), ctx.channel().id());
            }
        } catch (Exception e) {
            log.warn("Leader failed to parse FOLLOWER_HEARTBEAT message from channel {}: {}. Error: {}", ctx.channel().id(), message.getContent(), e.getMessage(), e);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("Exception caught in Netty pipeline for channel {}(remote: {}): {}", ctx.channel().id(), ctx.channel().remoteAddress(), cause.getMessage(), cause);
        connectionRegistry.remove(ctx.channel());
        ctx.close();
    }

    public void sendFollowerHeartbeat(int brokerId) {
        if (leaderChannel == null || !leaderChannel.isActive()) {
            log.warn("Follower {}: Leader channel is not active or null. Attempting to reconnect...", brokerId);
            try {
                reconnectToLeader();
            } catch (Exception e) {
                log.error("Follower {}: Reconnection to leader failed: {}", brokerId, e.getMessage(), e);
                return;
            }
        }

        if (leaderChannel != null && leaderChannel.isActive()) {
            Message heartbeat = new Message(MessageType.FOLLOWER_HEARTBEAT, null, String.format("{\"brokerId\":%d}", brokerId));
            log.debug("Follower {} sending heartbeat to leader via channel {}.", brokerId, leaderChannel.id());
            leaderChannel.writeAndFlush(heartbeat.serialize() + "\n").addListener(future -> {
                if (!future.isSuccess()) {
                    log.warn("Follower {} failed to send heartbeat to leader via channel {}. Error: {}", brokerId, leaderChannel.id(), future.cause().getMessage(), future.cause());
                    leaderChannel.close();
                }
            });
        } else {
            log.warn("Follower {}: Leader channel still not active after attempting reconnect. Cannot send heartbeat.", brokerId);
        }
    }

    private void reconnectToLeader() throws InterruptedException {
        ClusterManager clusterManager = ClusterContext.get();
        if (clusterManager.isLeader()) {
            log.warn("Leader broker (ID {}) trying to reconnect to leader. This is unusual. Aborting.", clusterManager.getBrokerId());
            return;
        }
        if (this.clientConnectionGroup == null || this.clientConnectionGroup.isShutdown() || this.clientConnectionGroup.isShuttingDown()) {
            log.error("Follower {} clientConnectionGroup is not available. Cannot reconnect to leader.", clusterManager.getBrokerId());
            return;
        }

        // Close existing leaderChannel before attempting to reconnect to avoid multiple inconsistent channels
        if (this.leaderChannel != null && this.leaderChannel.isOpen()){
            log.info("Follower {}: Closing existing leader channel {} before reconnecting.", clusterManager.getBrokerId(), this.leaderChannel.id());
            this.leaderChannel.close().awaitUninterruptibly(2, TimeUnit.SECONDS); // Wait briefly for close
            this.leaderChannel = null; // Clear the reference
        }


        log.info("Follower {} attempting to reconnect to leader at {}:{}",
                clusterManager.getBrokerId(), clusterManager.getLeaderHost(), clusterManager.getLeaderPort());

        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(this.clientConnectionGroup)
                .channel(NioSocketChannel.class)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ChannelPipeline pipeline = ch.pipeline();
                        pipeline.addLast(new io.netty.handler.codec.LineBasedFrameDecoder(8192));
                        pipeline.addLast(new StringDecoder());
                        pipeline.addLast(new StringEncoder());
                        pipeline.addLast(MessageBrokerHandler.this);
                    }
                });

        try {
            ChannelFuture future = bootstrap.connect(clusterManager.getLeaderHost(), clusterManager.getLeaderPort()).sync();
            if (future.isSuccess()) {
                Channel newLeaderChannel = future.channel();
                log.info("Follower {} successfully reconnected to leader. New leader channel: {}", clusterManager.getBrokerId(), newLeaderChannel.id());
                this.setLeaderChannel(newLeaderChannel);
                isRegistered = false;
                CompletableFuture<String> registrationFuture = this.registerWithLeader(
                        this.leaderChannel,
                        clusterManager.getBrokerId(),
                        clusterManager.getBrokerHost() + ":" + clusterManager.getBrokerPort()
                );
                registrationFuture.thenAccept(ack -> log.info("Follower {} re-registration acknowledged by leader: {}", clusterManager.getBrokerId(), ack))
                        .exceptionally(ex -> {
                            log.error("Follower {} re-registration failed after reconnect: {}", clusterManager.getBrokerId(), ex.getMessage(), ex);
                            if (this.leaderChannel != null && this.leaderChannel.isActive()) this.leaderChannel.close(); // Close if reg failed
                            return null;
                        });
            } else {
                log.warn("Follower {} failed to reconnect to leader: {}", clusterManager.getBrokerId(), future.cause() != null ? future.cause().getMessage() : "Unknown", future.cause());
            }
        } catch (Exception e) {
            Thread.currentThread().interrupt();
            log.error("Follower {} exception during reconnect to leader: {}", clusterManager.getBrokerId(), e.getMessage(), e);
        }
    }


    private void maybeRebalance(String group, String topic) {
        log.debug("Checking for rebalance conditions for group {} and topic {}", group, topic);
        Map<String, Long> groupHeartbeatsSource = this.consumerHeartbeats.computeIfAbsent(group, k -> new ConcurrentHashMap<>());
        long now = System.currentTimeMillis();
        final long heartbeatTimeoutMs = 30000;

        List<String> activeConsumerIds = new ArrayList<>();
        new ArrayList<>(groupHeartbeatsSource.keySet()).forEach(consumerId -> {
            Long lastHeartbeat = groupHeartbeatsSource.get(consumerId);
            if (lastHeartbeat != null && (now - lastHeartbeat <= heartbeatTimeoutMs)) {
                activeConsumerIds.add(consumerId);
            } else {
                log.info("Consumer {} in group {} for topic {} timed out. Removing.", consumerId, group, topic);
                groupHeartbeatsSource.remove(consumerId);
                consumerAssignments.getOrDefault(group, new ConcurrentHashMap<>()).remove(consumerId);
            }
        });

        if (groupHeartbeatsSource.isEmpty()) {
            this.consumerHeartbeats.remove(group);
            this.consumerAssignments.remove(group);
            log.info("No active consumers in group {} for topic {}. Cleared assignments.", group, topic);
            return;
        }
        activeConsumerIds.sort(String::compareTo);

        Map<String, Integer> currentAssignmentsForGroup = consumerAssignments.getOrDefault(group, Map.of());
        boolean needsRebalance = currentAssignmentsForGroup.size() != activeConsumerIds.size() ||
                activeConsumerIds.stream().anyMatch(cid -> !currentAssignmentsForGroup.containsKey(cid)) ||
                !consumerAssignments.containsKey(group); // Rebalance if no assignments yet for group

        if (!needsRebalance) {
            log.debug("No change in active consumer set for group {} topic {}. Skipping rebalance.", group, topic);
            return;
        }

        log.info("Performing rebalance for group {} and topic {}. Active consumers: {}", group, topic, activeConsumerIds);
        Map<String, Integer> newAssignmentsForGroup = new ConcurrentHashMap<>();
        Optional<Topic> optionalTopic = topicManager.getTopic(topic);

        if (optionalTopic.isEmpty() || optionalTopic.get().getPartitions().isEmpty()) {
            log.warn("Topic {} not found or has no partitions during rebalance for group {}. Clearing assignments.", topic, group);
            consumerAssignments.put(group, newAssignmentsForGroup);
            return;
        }

        List<Integer> sortedPartitionIds = new ArrayList<>();
        for (int i = 0; i < optionalTopic.get().getPartitions().size(); i++) sortedPartitionIds.add(i);

        for (int i = 0; i < sortedPartitionIds.size(); i++) {
            if (activeConsumerIds.isEmpty()) break;
            String consumerId = activeConsumerIds.get(i % activeConsumerIds.size());
            newAssignmentsForGroup.put(consumerId, sortedPartitionIds.get(i));
            log.info("REBALANCE: Assigning topic {} partition {} to consumer {} (group {}).",
                    topic, sortedPartitionIds.get(i), consumerId, group);
        }
        activeConsumerIds.forEach(consumerId -> {
            if (!newAssignmentsForGroup.containsKey(consumerId)) {
                log.info("REBALANCE: Consumer {} (group {}) not assigned any partition for topic {} (more consumers than partitions or uneven distribution).",
                        consumerId, group, topic);
            }
        });
        consumerAssignments.put(group, newAssignmentsForGroup);
        log.info("Rebalance complete for group {} topic {}. Assignments: {}", group, topic, newAssignmentsForGroup);
    }

    private void assignPartitionsToBrokers(String topic, List<Integer> brokerIds, int partitionCount) {
        if (brokerIds.isEmpty()) {
            log.warn("Cannot assign partitions for topic {}: no brokers available.", topic);
            return;
        }
        Map<Integer, Integer> assignments = new ConcurrentHashMap<>();
        for (int i = 0; i < partitionCount; i++) {
            int brokerId = brokerIds.get(i % brokerIds.size());
            assignments.put(i, brokerId);
        }
        partitionAssignments.put(topic, assignments);
        log.info("Leader assigned partitions for topic {}. Assignments: {}. Broker IDs used: {}", topic, assignments, brokerIds);
    }


    public void shutdown() {
        log.info("Shutting down MessageBrokerHandler...");
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
                log.warn("Executor service did not terminate in 5 seconds, forcing shutdown.");
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            log.warn("Shutdown of executor service interrupted.");
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }

        if (followerReplicationGroup != null && !followerReplicationGroup.isShutdown()) {
            log.info("Shutting down followerReplicationGroup...");
            followerReplicationGroup.shutdownGracefully().awaitUninterruptibly(5, TimeUnit.SECONDS);
        }

        if (clientConnectionGroup != null && !clientConnectionGroup.isShutdown()) {
            log.info("Shutting down clientConnectionGroup...");
            clientConnectionGroup.shutdownGracefully().awaitUninterruptibly(5, TimeUnit.SECONDS);
        }

        log.info("Closing follower replication connections...");
        followerConnections.values().forEach(channel -> {
            if (channel.isActive()) channel.close();
        });
        followerConnections.clear();

        if (leaderChannel != null && leaderChannel.isActive()) {
            log.info("Closing leader channel...");
            leaderChannel.close().awaitUninterruptibly(2, TimeUnit.SECONDS);
        }

        log.info("Closing all other registered connections...");
        connectionRegistry.keySet().forEach(channel -> {
            if (channel.isActive()) channel.close();
        });
        connectionRegistry.clear();

        log.info("MessageBrokerHandler shut down completed.");
    }


    private static class MessageTask {
        private final ChannelHandlerContext ctx;
        private final String rawMessage;

        public MessageTask(ChannelHandlerContext ctx, String rawMessage) {
            this.ctx = ctx;
            this.rawMessage = rawMessage;
        }
        public ChannelHandlerContext getCtx() { return ctx; }
        public String getRawMessage() { return rawMessage; }
    }
}
