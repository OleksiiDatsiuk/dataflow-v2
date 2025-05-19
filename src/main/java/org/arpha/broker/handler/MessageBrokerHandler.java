package org.arpha.broker.handler;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LineBasedFrameDecoder;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.util.CharsetUtil;
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
import java.util.Collections;
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
import java.util.stream.Collectors;

@Slf4j
@ChannelHandler.Sharable
public class MessageBrokerHandler extends SimpleChannelInboundHandler<String> {

    private static volatile boolean isRegistered = false;
    private static final ObjectMapper mapper = new ObjectMapper();

    private final TopicManager topicManager;
    private final Map<Channel, MessageType> connectionRegistry;
    private final BlockingQueue<MessageTask> messageQueue;
    private final ExecutorService executorService;

    // Key: GroupID, Value: Map<PartitionID, Offset>
    private final Map<String, Map<Integer, Long>> committedOffsets = new ConcurrentHashMap<>();

    // Key: GroupID, Value: Map<ConsumerID, List<PartitionID>>
    private final Map<String, Map<String, List<Integer>>> consumerAssignments = new ConcurrentHashMap<>();

    private final Map<Integer, CompletableFuture<String>> registrationAcks = new ConcurrentHashMap<>();
    private final Map<String, Map<String, Long>> consumerHeartbeats = new ConcurrentHashMap<>();

    // Key: TopicName, Value: Map<PartitionID, BrokerID (Owner)>
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
            // Basic check for excessively long messages to prevent OOM from LineBasedFrameDecoder if max length is huge
            if (rawMessage.length() > 1024 * 1024) { // Example: 1MB limit per message string
                log.warn("Received overly long message ({} bytes) on channel {}. Potential framing issue or attack. Closing channel.", rawMessage.length(), ctx.channel().id());
                ctx.close();
                return;
            }
            messageQueue.put(new MessageTask(ctx, rawMessage));
        } catch (InterruptedException e) {
            log.error("Failed to enqueue message: {}", rawMessage, e);
            Thread.currentThread().interrupt();
        }  catch (Exception e) {
            log.error("Unexpected error in channelRead0 for raw message '{}' on channel {}: {}", rawMessage, ctx.channel().id(), e.getMessage(), e);
            ctx.close(); // Close on other unexpected errors during enqueue attempt
        }
    }

    private void startMessageProcessing() {
        int numProcessingThreads = 4; // Consider making this configurable
        for (int i = 0; i < numProcessingThreads; i++) {
            executorService.submit(() -> {
                MessageTask task = null; // For logging in catch block
                while (!Thread.currentThread().isInterrupted()) {
                    try {
                        task = messageQueue.take();
                        processMessageTask(task);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        log.error("Message processing thread interrupted, stopping.", e);
                        break;
                    } catch (Exception e) {
                        log.error("Unhandled exception during message task processing for message: '{}'", (task != null ? task.getRawMessage() : "UNKNOWN_TASK"), e);
                        // Depending on the severity, this thread might need to be restarted, or the error handled more gracefully.
                        // For now, the loop continues, but this indicates a bug in processMessageTask or its sub-handlers.
                    }
                }
                log.info("Message processing thread finished.");
            });
        }
    }

    public CompletableFuture<String> registerWithLeader(Channel channel, int brokerId, String address) {
        if (isRegistered && leaderChannel != null && leaderChannel.equals(channel)) { // Check if already registered via THIS channel
            log.warn("Broker {} attempting to register with leader again via the same channel {}. Current isRegistered={}.", brokerId, channel.id(), isRegistered);
        } else {
            log.info("Broker {} attempting registration. Current isRegistered={}", brokerId, isRegistered);
        }


        Message registration = new Message(MessageType.BROKER_REGISTRATION, null,
                String.format("{\"brokerId\":%d,\"address\":\"%s\"}", brokerId, address));
        CompletableFuture<String> ackFuture = new CompletableFuture<>();

        CompletableFuture<String> previousAck = registrationAcks.put(brokerId, ackFuture);
        if (previousAck != null && !previousAck.isDone()) {
            log.warn("Superseding previous incomplete registration attempt for broker {}", brokerId);
            previousAck.completeExceptionally(new IllegalStateException("Superseded by new registration attempt for broker " + brokerId));
        }

        log.info("Follower {} sending registration to leader via channel {}: {}", brokerId, channel.id(), registration.serialize());
        channel.writeAndFlush(registration.serialize() + "\n").addListener(writeFuture -> {
            if (!writeFuture.isSuccess()) {
                log.error("Follower {} failed to send registration message to leader. Channel: {}, Cause: {}", brokerId, channel.id(), writeFuture.cause().getMessage(), writeFuture.cause());
                ackFuture.completeExceptionally(writeFuture.cause());
                registrationAcks.remove(brokerId, ackFuture); // Clean up if send failed
            } else {
                log.info("Follower {} successfully sent registration request to leader. Waiting for ACK.", brokerId);
            }
        });
        return ackFuture;
    }

    private void processMessageTask(MessageTask task) {
        Message message = null;
        try {
            message = Message.parse(task.getRawMessage());
            MessageType messageType = message.getType();
            log.debug("Received message on channel {} of type {}: {}", task.getCtx().channel().id(), messageType, task.getRawMessage().length() > 200 ? task.getRawMessage().substring(0,200) + "..." : task.getRawMessage()); // Log snippet
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
            log.error("Invalid message format received on channel {}: '{}'. Error: {}",task.getCtx().channel().id(), task.getRawMessage(), e.getMessage()); // No need to print stacktrace for this, message is enough
            // Consider closing the channel for malformed messages if it's a protocol violation
            task.getCtx().close();
        } catch (Exception e) {
            String topicInfo = (message != null && message.getTopic() != null) ? message.getTopic() : "N/A";
            MessageType typeInfo = (message != null) ? message.getType() : MessageType.UNKNOWN;
            log.error("Failed to process message (type: {}, topic: {}) on channel {}: '{}'. Error: {}",
                    typeInfo, topicInfo, task.getCtx().channel().id(), task.getRawMessage(), e.getMessage(), e);
            task.getCtx().close(); // Close on other unexpected errors during processing
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
                log.error("ClusterManager not available or role unclear while handling Broker ACK on channel {}.", ctx.channel().id());
            }
            connectionRegistry.put(ctx.channel(), MessageType.BROKER);
        } catch (Exception e) {
            log.error("Error processing BROKER_ACK from channel {}: {}", ctx.channel().id(), message.getContent(), e);
        }
    }


    private void handleProducerMessage(ChannelHandlerContext ctx, Message message) {
        String topic = message.getTopic();
        topicManager.addMessageToTopic(topic, message.getContent());

        ClusterManager clusterManager = ClusterContext.get();
        if (clusterManager == null) {
            log.error("ClusterManager is null in handleProducerMessage. Cannot determine leadership or replicate.");
            return;
        }

        if (clusterManager.isLeader()) {
            log.debug("Leader handling producer message for topic {}. Replicating...", topic);
            if (!partitionAssignments.containsKey(topic)) {
                Optional<Topic> topicOpt = topicManager.getTopic(topic);
                topicOpt.ifPresent(t -> {
                    List<Integer> brokerIdsForAssignment = new ArrayList<>(clusterManager.getRegisteredBrokers().keySet());
                    brokerIdsForAssignment.add(clusterManager.getBrokerId());
                    List<Integer> distinctBrokerIds = brokerIdsForAssignment.stream().distinct().collect(Collectors.toList());
                    assignPartitionsToBrokers(topic, distinctBrokerIds, t.getPartitions().size());
                });
            }

            clusterManager.getRegisteredBrokers().keySet().forEach(followerBrokerId -> {
                replicateMessageToFollower(followerBrokerId, message);
            });
        } else {
            log.warn("Follower ID {} received a PRODUCER message directly for topic {}. Storing locally.", clusterManager.getBrokerId(), topic);
        }
    }

    private void connectToFollowerForReplication(int followerBrokerId, String followerAddress) {
        ClusterManager clusterManager = ClusterContext.get();
        if (clusterManager == null) {
            log.error("ClusterManager is null in connectToFollowerForReplication. Cannot determine leadership.");
            return;
        }
        if (!clusterManager.isLeader()) {
            log.warn("Non-leader broker ({}) attempted to connect to follower {}.", clusterManager.getBrokerId(), followerBrokerId);
            return;
        }

        Channel existingChannel = followerConnections.get(followerBrokerId);
        if (existingChannel != null && existingChannel.isActive()) {
            log.info("Leader already has an active replication connection to follower {} at {} via channel {}.", followerBrokerId, followerAddress, existingChannel.id());
            return;
        }
        if (this.followerReplicationGroup == null || this.followerReplicationGroup.isShutdown() || this.followerReplicationGroup.isShuttingDown()) {
            log.error("Leader's followerReplicationGroup is not available. Cannot connect to follower {}.", followerBrokerId);
            return;
        }

        log.info("Leader attempting to establish new replication connection to follower {} at {}", followerBrokerId, followerAddress);
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
                        pipeline.addLast(new LineBasedFrameDecoder(8192));
                        pipeline.addLast(new StringEncoder(CharsetUtil.UTF_8));
                        pipeline.addLast(new StringDecoder(CharsetUtil.UTF_8));
                    }
                });

        try {
            ChannelFuture future = bootstrap.connect(host, port).syncUninterruptibly();
            if (future.isSuccess()) {
                Channel newFollowerChannel = future.channel();
                log.info("Leader successfully connected to follower {} at {} for replication via new channel {}.", followerBrokerId, followerAddress, newFollowerChannel.id());
                Channel oldChannel = followerConnections.put(followerBrokerId, newFollowerChannel);
                if(oldChannel != null && !oldChannel.equals(newFollowerChannel) && oldChannel.isActive()){
                    log.warn("Replaced an existing active channel {} for follower {} during connect.", oldChannel.id(), followerBrokerId);
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
        if (clusterManager == null || !clusterManager.isLeader()) return;

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
                    // Removal is handled by closeFuture listener in connectToFollowerForReplication
                } else {
                    log.debug("Leader successfully sent replica to follower {} via channel {}", followerBrokerId, followerChannel.id());
                }
            });
        } else {
            log.warn("Leader: No active replication channel to follower {}. Message for topic {} not replicated. Will attempt to establish connection.",
                    followerBrokerId, originalMessage.getTopic());
            // Removing potentially stale/inactive channel entry before attempting to reconnect
            if (followerChannel != null) {
                followerConnections.remove(followerBrokerId, followerChannel); // Ensure removal if instance was bad
            }
            String followerAddress = clusterManager.getRegisteredBrokers().get(followerBrokerId);
            if (followerAddress != null) {
                connectToFollowerForReplication(followerBrokerId, followerAddress);
            } else {
                log.warn("Leader: Address for follower {} not found. Cannot connect for replication.", followerBrokerId);
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
                    // If unknown action, we might just ignore or send a generic error.
                    // For now, it falls through and does nothing further unless an error was thrown before.
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
            } else { // Errors for COMMIT, HEARTBEAT, or parsing consumerMsg itself
                log.warn("Closing connection with consumer on channel {} due to error processing {} request.", ctx.channel().id(), actionTypeInfo);
                ctx.close();
            }
        }
    }

    private void handlePoll(ChannelHandlerContext ctx, ConsumerMessage msg) throws Exception {
        ClusterManager currentClusterManager = ClusterContext.get();
        if (currentClusterManager == null) {
            log.error("CRITICAL: ClusterManager is null in handlePoll for consumer {}. Closing connection.", msg.getConsumerId());
            if (ctx.channel().isActive()) {
                BrokerPollResponse errorResponse = new BrokerPollResponse(-1, -1, "Broker internal error: Cluster configuration unavailable.", null, null);
                ctx.writeAndFlush(mapper.writeValueAsString(errorResponse) + "\n").addListener(ChannelFutureListener.CLOSE);
            }
            return;
        }

        String topic = msg.getTopic();
        String group = msg.getConsumerGroup();
        String consumerId = msg.getConsumerId();

        if (group == null || consumerId == null || topic == null) {
            log.warn("Received POLL request with null group, consumerId, or topic from channel {}. Group: {}, ConsumerId: {}, Topic: {}. Sending error.", ctx.channel().id(), group, consumerId, topic);
            BrokerPollResponse errResponse = new BrokerPollResponse(-1, -1, "Invalid POLL request: group, consumerId, or topic is null.", null, null);
            if (ctx.channel().isActive()) {
                ctx.writeAndFlush(mapper.writeValueAsString(errResponse) + "\n");
            }
            return;
        }

        consumerHeartbeats.computeIfAbsent(group, k -> new ConcurrentHashMap<>()).put(consumerId, System.currentTimeMillis());
        maybeRebalance(group, topic);

        Map<String, List<Integer>> groupAssignments = this.consumerAssignments.get(group);
        List<Integer> assignedPartitionsForThisConsumerOnThisBroker = (groupAssignments != null) ?
                groupAssignments.getOrDefault(consumerId, Collections.emptyList()) : Collections.emptyList();

        if (assignedPartitionsForThisConsumerOnThisBroker.isEmpty()) {
            log.warn("Consumer {} in group {} for topic {} has no partitions assigned on this broker (ID {}). This broker might not own partitions for this consumer or rebalance hasn't assigned any. Sending empty response.",
                    consumerId, group, topic, currentClusterManager.getBrokerId());
            BrokerPollResponse emptyResponse = new BrokerPollResponse(-1, -1, null, null, null);
            if (ctx.channel().isActive()) {
                ctx.writeAndFlush(mapper.writeValueAsString(emptyResponse) + "\n");
            }
            return;
        }

        BrokerPollResponse responseToSend = null;

        for (int partitionId : assignedPartitionsForThisConsumerOnThisBroker) {
            Integer globalOwnerBrokerId = partitionAssignments
                    .getOrDefault(topic, Collections.emptyMap())
                    .get(partitionId);

            if (globalOwnerBrokerId == null) {
                log.warn("Broker {}: No global owner found for topic '{}' partition {} in partitionAssignments. Consumer {}. This partition might be new or unassigned cluster-wide. Skipping.",
                        currentClusterManager.getBrokerId(), topic, partitionId, consumerId);
                continue;
            }

            if (Integer.valueOf(currentClusterManager.getBrokerId()).equals(globalOwnerBrokerId)) {
                long committedOffsetForPartition = committedOffsets
                        .getOrDefault(group, Collections.emptyMap())
                        .getOrDefault(partitionId, 0L);

                String nextMessage = topicManager.getMessageAtOffset(topic, partitionId, committedOffsetForPartition);
                if (nextMessage != null) {
                    log.debug("Broker {} (Owner): Serving message for consumer {} in group {} on topic {} partition {} at offset {}",
                            currentClusterManager.getBrokerId(), consumerId, group, topic, partitionId, committedOffsetForPartition);
                    responseToSend = new BrokerPollResponse(partitionId, committedOffsetForPartition, nextMessage);
                    break;
                }
            } else {
                if (currentClusterManager.isLeader()) {
                    String ownerAddress = currentClusterManager.getRegisteredBrokers().get(globalOwnerBrokerId);
                    if (ownerAddress != null) {
                        log.info("Leader (ID {}): Consumer {} (channel {}) polled for partition {} (topic {}) which is globally owned by broker {}. Redirecting to {}.",
                                currentClusterManager.getBrokerId(), consumerId, ctx.channel().id(), partitionId, topic, globalOwnerBrokerId, ownerAddress);
                        responseToSend = new BrokerPollResponse(partitionId, -1, null, ownerAddress, globalOwnerBrokerId);
                        break;
                    } else {
                        log.warn("Leader (ID {}): Consumer {} (channel {}) polled for partition {} (topic {}) globally owned by broker {}, but owner's address not found. Cannot redirect. Consumer might need to retry or re-evaluate assignments.",
                                currentClusterManager.getBrokerId(), consumerId, ctx.channel().id(), partitionId, topic, globalOwnerBrokerId);
                    }
                } else {
                    log.warn("Follower (ID {}): Consumer {} (channel {}) polled for partition {} (topic {}) which this follower does not own (Global Owner: {}). This consumer should be polling the owner. Sending empty response for this poll cycle regarding this partition.",
                            currentClusterManager.getBrokerId(), consumerId, ctx.channel().id(), partitionId, topic, globalOwnerBrokerId);
                }
            }
        }

        if (responseToSend != null) {
            if (ctx.channel().isActive()) {
                ctx.writeAndFlush(mapper.writeValueAsString(responseToSend) + "\n");
            }
        } else {
            int representativePartition = -1;
            if (!assignedPartitionsForThisConsumerOnThisBroker.isEmpty()) {
                for (int pId : assignedPartitionsForThisConsumerOnThisBroker) {
                    Integer globalOwnerId = partitionAssignments.getOrDefault(topic, Collections.emptyMap()).get(pId);
                    if (Integer.valueOf(currentClusterManager.getBrokerId()).equals(globalOwnerId)) {
                        representativePartition = pId;
                        break;
                    }
                }
            }

            log.debug("Broker {} (ID {}): No new messages for consumer {} in group {} for topic {} across its checked partitions. Sending empty response (context partition: {}).",
                    currentClusterManager.isLeader() ? "Leader" : "Follower", currentClusterManager.getBrokerId(),
                    consumerId, group, topic, representativePartition);
            BrokerPollResponse emptyResponse = new BrokerPollResponse(representativePartition, -1, null, null, null);
            if (ctx.channel().isActive()) {
                ctx.writeAndFlush(mapper.writeValueAsString(emptyResponse) + "\n");
            }
        }
    }


    private void handleCommit(ChannelHandlerContext ctx, ConsumerMessage msg) {
        if (msg.getConsumerGroup() == null) {
            log.warn("Received COMMIT with null consumerGroup from consumer {} on channel {}. Ignoring.", msg.getConsumerId(), ctx.channel().id());
            return;
        }
        log.info("Received COMMIT on channel {} for topic={}, group={}, partition={}, offset={}",
                ctx.channel().id(), msg.getTopic(), msg.getConsumerGroup(), msg.getPartition(), msg.getOffset());
        committedOffsets
                .computeIfAbsent(msg.getConsumerGroup(), k -> new ConcurrentHashMap<>())
                .put(msg.getPartition(), msg.getOffset());
    }


    private void handleConsumerHeartbeat(ConsumerMessage msg) {
        if (msg.getConsumerGroup() == null || msg.getConsumerId() == null) {
            log.warn("Received HEARTBEAT with null consumerGroup or consumerId. Group: {}, ConsumerID: {}. Ignoring.",
                    msg.getConsumerGroup(), msg.getConsumerId());
            return;
        }
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
        if (clusterManager == null) {
            log.error("ClusterManager is null in handleBrokerRegistrationMessage. Cannot process registration from channel {}.", ctx.channel().id());
            return;
        }
        if (!clusterManager.isLeader()) {
            log.warn("Non-leader broker ID {} received broker registration on channel {}. Ignoring. Message: {}", clusterManager.getBrokerId(), ctx.channel().id(), message.getContent());
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
                            String.format("{\"status\":\"FAILURE\", \"message\":\"Error processing registration: %s\"}", e.getMessage().replace("\"", "\\\"")));
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
        if (clusterManager == null) {
            log.error("ClusterManager is null in handleFollowerHeartbeat. Cannot process heartbeat from channel {}.", ctx.channel().id());
            return;
        }
        if (!clusterManager.isLeader()) {
            log.warn("Non-leader ID {} received follower heartbeat on channel {}. Ignoring.", clusterManager.getBrokerId(), ctx.channel().id());
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
        if (clusterManager == null) {
            log.error("ClusterManager is null in reconnectToLeader. Cannot determine role or leader info.");
            return;
        }
        if (clusterManager.isLeader()) {
            log.warn("Leader broker (ID {}) trying to reconnect to leader. This is unusual. Aborting.", clusterManager.getBrokerId());
            return;
        }
        if (this.clientConnectionGroup == null || this.clientConnectionGroup.isShutdown() || this.clientConnectionGroup.isShuttingDown()) {
            log.error("Follower {} clientConnectionGroup is not available. Cannot reconnect to leader.", clusterManager.getBrokerId());
            return;
        }

        if (this.leaderChannel != null && this.leaderChannel.isOpen()){
            log.info("Follower {}: Closing existing leader channel {} before reconnecting.", clusterManager.getBrokerId(), this.leaderChannel.id());
            this.leaderChannel.close().awaitUninterruptibly(1, TimeUnit.SECONDS); // Shorter wait
            this.leaderChannel = null;
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
                        pipeline.addLast(new LineBasedFrameDecoder(8192));
                        pipeline.addLast(new StringDecoder(CharsetUtil.UTF_8));
                        pipeline.addLast(new StringEncoder(CharsetUtil.UTF_8));
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
                            if (this.leaderChannel != null && this.leaderChannel.isActive()) this.leaderChannel.close();
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
        ClusterManager clusterManager = ClusterContext.get();
        if (clusterManager == null) {
            log.error("ClusterManager is null in maybeRebalance for group {}, topic {}. Cannot perform rebalance.", group, topic);
            return;
        }

        int currentBrokerId = clusterManager.getBrokerId();
        log.debug("Broker {} performing local rebalance check for group {} and topic {}", currentBrokerId, group, topic);

        Map<String, Long> groupHeartbeats = this.consumerHeartbeats.computeIfAbsent(group, k -> new ConcurrentHashMap<>());
        long now = System.currentTimeMillis();
        final long heartbeatTimeoutMs = 30000;

        List<String> activeConsumerIds = new ArrayList<>();
        groupHeartbeats.forEach((consumerId, lastHeartbeat) -> {
            if (now - lastHeartbeat <= heartbeatTimeoutMs) {
                activeConsumerIds.add(consumerId);
            }
        });
        // Clean up timed-out consumers from heartbeats and current assignments for this group
        groupHeartbeats.entrySet().removeIf(entry -> {
            boolean timedOut = now - entry.getValue() > heartbeatTimeoutMs;
            if (timedOut) {
                log.info("Consumer {} in group {} for topic {} on broker {} timed out. Removing from active list for rebalance.", entry.getKey(), group, topic, currentBrokerId);
                Optional.ofNullable(this.consumerAssignments.get(group)).ifPresent(assignments -> assignments.remove(entry.getKey()));
            }
            return timedOut;
        });

        if (activeConsumerIds.isEmpty()) {
            Map<String, List<Integer>> currentGroupAssignments = this.consumerAssignments.get(group);
            if (currentGroupAssignments != null && !currentGroupAssignments.isEmpty()) {
                log.info("No active consumers in group {} for topic {} on broker {}. Clearing all assignments for this group.", group, topic, currentBrokerId);
                currentGroupAssignments.clear(); // Clear assignments for all consumers in this group
            } else {
                log.debug("No active consumers and no assignments to clear for group {} topic {} on broker {}.", group, topic, currentBrokerId);
            }
            return;
        }
        Collections.sort(activeConsumerIds); // For consistent assignment order

        Optional<Topic> optionalTopic = topicManager.getTopic(topic);
        if (optionalTopic.isEmpty() || optionalTopic.get().getPartitions().isEmpty()) {
            log.warn("Topic {} not found or has no partitions. Cannot rebalance for group {} on broker {}.", topic, group, currentBrokerId);
            Map<String, List<Integer>> currentGroupAssignments = this.consumerAssignments.get(group);
            if (currentGroupAssignments != null) currentGroupAssignments.clear();
            return;
        }

        List<Integer> locallyOwnedPartitions = new ArrayList<>();
        Map<Integer, Integer> topicPartitionOwners = this.partitionAssignments.getOrDefault(topic, Collections.emptyMap());
        int totalPartitionsInTopic = optionalTopic.get().getPartitions().size();

        for (int i = 0; i < totalPartitionsInTopic; i++) {
            if (Integer.valueOf(currentBrokerId).equals(topicPartitionOwners.get(i))) {
                locallyOwnedPartitions.add(i);
            }
        }
        Collections.sort(locallyOwnedPartitions);

        if (locallyOwnedPartitions.isEmpty()) {
            log.info("Broker {} owns no partitions for topic {}. Clearing assignments for group {}.", currentBrokerId, topic, group);
            Map<String, List<Integer>> currentGroupAssignments = this.consumerAssignments.get(group);
            if (currentGroupAssignments != null) currentGroupAssignments.clear();
            return;
        }

        // Calculate proposed new assignments
        Map<String, List<Integer>> proposedAssignments = new ConcurrentHashMap<>();
        activeConsumerIds.forEach(cId -> proposedAssignments.put(cId, new ArrayList<>())); // Initialize lists

        for (int i = 0; i < locallyOwnedPartitions.size(); i++) {
            int partitionId = locallyOwnedPartitions.get(i);
            String consumerId = activeConsumerIds.get(i % activeConsumerIds.size());
            proposedAssignments.get(consumerId).add(partitionId);
        }
        proposedAssignments.values().forEach(Collections::sort); // Sort lists for consistent comparison

        // Get current assignments for this group, or initialize if not present
        Map<String, List<Integer>> currentGroupAssignments = this.consumerAssignments.computeIfAbsent(group, k -> new ConcurrentHashMap<>());
        boolean assignmentsHaveChanged = false;

        // Check if proposed assignments differ from current ones
        if (currentGroupAssignments.size() != proposedAssignments.size() ||
                !currentGroupAssignments.keySet().equals(proposedAssignments.keySet())) {
            assignmentsHaveChanged = true;
        } else {
            for (Map.Entry<String, List<Integer>> entry : proposedAssignments.entrySet()) {
                if (!currentGroupAssignments.get(entry.getKey()).equals(entry.getValue())) {
                    assignmentsHaveChanged = true;
                    break;
                }
            }
        }
        // Also consider consumers that were in currentAssignments but are no longer active
        if(!assignmentsHaveChanged){
            for(String existingConsumer : currentGroupAssignments.keySet()){
                if(!activeConsumerIds.contains(existingConsumer)){
                    assignmentsHaveChanged = true;
                    break;
                }
            }
        }


        if (!assignmentsHaveChanged) {
            log.debug("No change in assignments needed for group {} topic {} on broker {}. Current assignments: {}", group, topic, currentBrokerId, currentGroupAssignments);
            return;
        }

        log.info("Performing rebalance on broker {} for group {} and topic {}. Active consumers: {}. Owned partitions: {}. Proposed assignments: {}",
                currentBrokerId, group, topic, activeConsumerIds, locallyOwnedPartitions, proposedAssignments);

        // Update assignments: remove inactive consumers, then update/add active ones
        currentGroupAssignments.keySet().removeIf(consumerId -> !activeConsumerIds.contains(consumerId));
        currentGroupAssignments.putAll(proposedAssignments);

        log.info("Rebalance complete on broker {} for group {} topic {}. Final assignments: {}", currentBrokerId, group, topic, currentGroupAssignments);
        currentGroupAssignments.forEach((consumer, partitions) -> {
            if (partitions.isEmpty() && activeConsumerIds.contains(consumer)) { // Check if active consumer ended up with no partitions
                log.info("REBALANCE (Broker {}): Active consumer {} (group {}) was not assigned any partitions for topic {}.", currentBrokerId, consumer, group, topic);
            } else if (!partitions.isEmpty()){
                log.info("REBALANCE (Broker {}): Consumer {} (group {}) assigned partitions {} for topic {}.", currentBrokerId, consumer, group, partitions, topic);
            }
        });
    }

    private void assignPartitionsToBrokers(String topic, List<Integer> allBrokerIdsInCluster, int partitionCount) {
        if (allBrokerIdsInCluster.isEmpty()) {
            log.warn("Cannot assign partitions for topic {}: no brokers available in cluster.", topic);
            return;
        }
        Map<Integer, Integer> newTopicPartitionOwners = new ConcurrentHashMap<>();
        for (int i = 0; i < partitionCount; i++) {
            int ownerBrokerId = allBrokerIdsInCluster.get(i % allBrokerIdsInCluster.size());
            newTopicPartitionOwners.put(i, ownerBrokerId);
        }
        partitionAssignments.put(topic, newTopicPartitionOwners);
        log.info("Leader assigned/updated partition ownership for topic {}. Assignments: {}. Brokers considered: {}", topic, newTopicPartitionOwners, allBrokerIdsInCluster);
    }


    public void shutdown() {
        log.info("Shutting down MessageBrokerHandler for broker ID: {}...", (ClusterContext.get() != null ? ClusterContext.get().getBrokerId() : "UNKNOWN"));
        if (executorService != null && !executorService.isShutdown()) {
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

        log.info("Closing all other registered connections from connectionRegistry...");
        connectionRegistry.keySet().forEach(channel -> {
            if (channel.isActive()) channel.close();
        });
        connectionRegistry.clear();

        log.info("MessageBrokerHandler shut down completed for broker ID: {}.", (ClusterContext.get() != null ? ClusterContext.get().getBrokerId() : "UNKNOWN"));
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
