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
import org.arpha.broker.component.Partition;
import org.arpha.broker.component.Topic;
import org.arpha.broker.component.manager.TopicManager;
import org.arpha.broker.handler.dto.BrokerPollResponse;
import org.arpha.broker.handler.dto.BrokerRegistrationMessage;
import org.arpha.broker.handler.dto.ConsumerMessage;
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


    public MessageBrokerHandler(TopicManager topicManager) {
        this.topicManager = topicManager;
        this.connectionRegistry = new ConcurrentHashMap<>();
        this.messageQueue = new LinkedBlockingQueue<>();
        this.executorService = Executors.newFixedThreadPool(10);
        startMessageProcessing();
    }

    public void setLeaderChannel(Channel channel) {
        this.leaderChannel = channel;
        this.connectionRegistry.put(channel, MessageType.BROKER);
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
        for (int i = 0; i < 4; i++) {
            executorService.submit(() -> {
                while (true) {
                    try {
                        MessageTask task = messageQueue.take();
                        processMessageTask(task);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        log.error("Message processing interrupted", e);
                        break;
                    }
                }
            });
        }
    }

    public CompletableFuture<String> registerWithLeader(Channel channel, int brokerId, String address) {
        if (isRegistered) {
            throw new IllegalStateException("Broker is already registered to a cluster. Only one registration is allowed.");
        }

        Message registration = new Message(MessageType.BROKER_REGISTRATION, null,
                String.format("{\"brokerId\":%d,\"address\":\"%s\"}", brokerId, address));
        CompletableFuture<String> ackFuture = new CompletableFuture<>();
        registrationAcks.put(brokerId, ackFuture);
        channel.writeAndFlush(registration.serialize());
        return ackFuture;
    }

    private void processMessageTask(MessageTask task) {
        try {
            Message message = Message.parse(task.getRawMessage());
            MessageType messageType = message.getType();

            log.info("Handling {} message: {}", messageType, message);

            connectionRegistry.put(task.getCtx().channel(), messageType);

            switch (messageType) {
                case PRODUCER -> handleProducerMessage(task.getCtx(), message);
                case CONSUMER -> handleConsumerMessage(task.getCtx(), message);
                case BROKER -> handleBrokerMessage(task.getCtx(), message);
                case BROKER_REGISTRATION -> handleBrokerRegistrationMessage(task.getCtx(), message);
                case BROKER_ACK -> handleBrokerAck(task.getCtx(), message);
                case FOLLOWER_HEARTBEAT -> handleFollowerHeartbeat(task.getCtx(), message);
                case REPLICA -> handleReplicaMessage(task.getCtx(), message);
                default -> handleUnknownMessage(task.getCtx(), message);
            }
            log.info("Successfully processed message: {}", message);
        } catch (IllegalArgumentException e) {
            log.error("Invalid message format received: {}", task.getRawMessage(), e);
        } catch (Exception e) {
            log.error("Failed to process message: {}", task.getRawMessage(), e);
        }
    }

    private void handleReplicaMessage(ChannelHandlerContext ctx, Message message) {
        String topic = message.getTopic();
        topicManager.addMessageToTopic(topic, message.getContent());
        log.info("Replicated message written to topic {}", topic);
    }

    private void handleBrokerAck(ChannelHandlerContext ctx, Message message) {
        registrationAcks.values().forEach(future -> future.complete(message.getContent()));
        connectionRegistry.put(ctx.channel(), MessageType.BROKER);
        isRegistered = true;
    }


    private void handleProducerMessage(ChannelHandlerContext ctx, Message message) {
        String topic = message.getTopic();
        topicManager.addMessageToTopic(topic, message.getContent());

        ClusterManager clusterManager = ClusterContext.get();
        if (clusterManager.isLeader()) {
            if (!partitionAssignments.containsKey(topic)) {
                Optional<Topic> topicOpt = topicManager.getTopic(topic);
                topicOpt.ifPresent(t -> {
                    List<Integer> brokerIds = new ArrayList<>(clusterManager.getRegisteredBrokers().keySet());
                    brokerIds.add(clusterManager.getBrokerId());
                    assignPartitionsToBrokers(topic, brokerIds, t.getPartitions().size());
                });
            }
            clusterManager.getRegisteredBrokers().forEach((brokerId, address) -> {
                if (brokerId == clusterManager.getBrokerId()) {
                    return;
                }
                replicateMessageToFollower(brokerId, address, message);
            });
        }
    }

    private void replicateMessageToFollower(int brokerId, String address, Message original) {
        String[] hostPort = address.split(":");
        String host = hostPort[0];
        int port = Integer.parseInt(hostPort[1]);

        Bootstrap bootstrap = new Bootstrap();
        EventLoopGroup group = new NioEventLoopGroup();
        bootstrap.group(group)
                .channel(NioSocketChannel.class)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ChannelPipeline pipeline = ch.pipeline();
                        pipeline.addLast(new StringEncoder());
                        pipeline.addLast(new StringDecoder());
                    }
                });

        bootstrap.connect(host, port).addListener((ChannelFuture future) -> {
            if (future.isSuccess()) {
                Message replica = new Message(MessageType.REPLICA, original.getTopic(), original.getContent());
                future.channel().writeAndFlush(replica.serialize());
                future.channel().close();
            } else {
                log.warn("Failed to replicate to broker {} at {}", brokerId, address);
            }
        });
    }

    private void handleConsumerMessage(ChannelHandlerContext ctx, Message message) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            ConsumerMessage consumerMsg = mapper.readValue(message.getContent(), ConsumerMessage.class);

            switch (consumerMsg.getAction()) {
                case POLL -> handlePoll(ctx, consumerMsg);
                case COMMIT -> handleCommit(ctx, consumerMsg);
                case HEARTBEAT -> handleConsumerHeartbeat(consumerMsg);
                default -> log.warn("Unknown consumer action: {}", consumerMsg.getAction());
            }
        } catch (Exception e) {
            log.error("Failed to process CONSUMER message", e);
        }
    }

    private void handlePoll(ChannelHandlerContext ctx, ConsumerMessage msg) {
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
            try {
                BrokerPollResponse emptyResponse = new BrokerPollResponse(-1, -1, null);
                ctx.writeAndFlush(mapper.writeValueAsString(emptyResponse) + "\n");
            } catch (Exception e) {
                log.error("Error sending empty unassigned partition response to consumer {}", consumerId, e);
            }
            return;
        }

        final int partition = assignedPartition;

        long committedOffset = committedOffsets
                .getOrDefault(group, Map.of())
                .getOrDefault(partition, 0L);

        if (ClusterContext.get().isLeader()) {
            Integer brokerIdForPartition = partitionAssignments
                    .getOrDefault(topic, Map.of())
                    .getOrDefault(partition, -1);

            if (brokerIdForPartition == null || brokerIdForPartition != ClusterContext.get().getBrokerId()) {
                log.warn("Broker (ID {}) does not own partition {} of topic {}. Assigned broker ID: {}. Sending empty response.",
                        ClusterContext.get().getBrokerId(), partition, topic, brokerIdForPartition);
                try {
                    BrokerPollResponse emptyResponse = new BrokerPollResponse(partition, -1, null);
                    ctx.writeAndFlush(mapper.writeValueAsString(emptyResponse) + "\n");
                } catch (Exception e) {
                    log.error("Error sending empty non-owned partition response to consumer {}", consumerId, e);
                }
                return;
            }
        }

        String nextMessage = topicManager.getMessageAtOffset(topic, partition, committedOffset);
        BrokerPollResponse response;

        if (nextMessage != null) {
            response = new BrokerPollResponse(partition, committedOffset, nextMessage);
        } else {
            response = new BrokerPollResponse(partition, -1, null);
        }

        try {
            ctx.writeAndFlush(mapper.writeValueAsString(response) + "\n");
        } catch (Exception e) {
            log.error("Error sending poll response to consumer {}: {}", consumerId, response, e);
        }
    }

    private void handleCommit(ChannelHandlerContext ctx, ConsumerMessage msg) {
        log.info("Received COMMIT for topic={} partition={} offset={}",
                msg.getTopic(), msg.getPartition(), msg.getOffset());

        committedOffsets
                .computeIfAbsent(msg.getConsumerGroup(), k -> new ConcurrentHashMap<>())
                .put(msg.getPartition(), msg.getOffset());
    }

    private int assignPartitionRoundRobin(String topic, String group) {
        Optional<Topic> optionalTopic = topicManager.getTopic(topic);
        if (optionalTopic.isEmpty()){
            return 0;
        }

        List<Partition> partitions = optionalTopic.get().getPartitions();
        int currentCount = consumerAssignments.get(group).size();
        return currentCount % partitions.size();
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
        log.info("Handling broker message: {}", message);
    }

    private void handleBrokerRegistrationMessage(ChannelHandlerContext ctx, Message message) {
        if (message.getContent() == null || message.getContent().isBlank()) {
            log.warn("Empty content in BROKER message");
            return;
        }

        ObjectMapper mapper = new ObjectMapper();
        try {
            BrokerRegistrationMessage dto = mapper.readValue(message.getContent(), BrokerRegistrationMessage.class);

            ClusterManager clusterManager = ClusterContext.get();
            if (!clusterManager.isLeader()) {
                log.warn("Received broker registration on non-leader node. Ignoring.");
                return;
            }

            clusterManager.registerBroker(dto.getBrokerId(), dto.getAddress(),
                    ClusterManager.BrokerStatus.FOLLOWER);
            log.info("Registered broker {} at {}", dto.getBrokerId(), dto.getAddress());

            Message ack = new Message(MessageType.BROKER_ACK, null, "ACK: Registered broker " + dto.getBrokerId());
            ctx.writeAndFlush(ack.serialize());

        } catch (Exception e) {
            log.error("Failed to parse BROKER registration message: {}", message.getContent(), e);
        }
    }

    private void handleUnknownMessage(ChannelHandlerContext ctx, Message message) {
        log.warn("Received unknown message: {}", message);
    }

    private void handleFollowerHeartbeat(ChannelHandlerContext ctx, Message message) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            Map<String, Integer> payload = mapper.readValue(message.getContent(), Map.class);
            int brokerId = payload.get("brokerId");

            ClusterManager clusterManager = ClusterContext.get();
            if (clusterManager.isLeader()) {
                clusterManager.getHeartbeatTimestamps().put(brokerId, System.currentTimeMillis());
            }
        } catch (Exception e) {
            log.warn("Failed to parse heartbeat message: {}", message.getContent(), e);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("Exception caught: ", cause);
        ctx.close();
    }

    public void sendFollowerHeartbeat(int brokerId) {
        log.info("Sending heartbeat to leader from broker {}", brokerId);

        if (leaderChannel == null || !leaderChannel.isActive()) {
            log.warn("Leader channel is not active. Trying to reconnect...");

            try {
                reconnectToLeader();
            } catch (Exception e) {
                log.error("Reconnection to leader failed: {}", e.getMessage());
                return;
            }
        }

        Message heartbeat = new Message(MessageType.FOLLOWER_HEARTBEAT, null,
                String.format("{\"brokerId\":%d}", brokerId));

        leaderChannel.writeAndFlush(heartbeat.serialize());
    }

    private void reconnectToLeader() throws InterruptedException {
        ClusterManager clusterManager = ClusterContext.get();

        Bootstrap bootstrap = new Bootstrap();
        EventLoopGroup group = new NioEventLoopGroup();

        bootstrap.group(group)
                .channel(NioSocketChannel.class)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ChannelPipeline pipeline = ch.pipeline();
                        pipeline.addLast(new StringDecoder());
                        pipeline.addLast(new StringEncoder());
                    }
                });

        String host = clusterManager.getLeaderHost();
        int port = clusterManager.getLeaderPort();

        ChannelFuture future = bootstrap.connect(host, port).sync();
        this.setLeaderChannel(future.channel());

        this.registerWithLeader(leaderChannel, clusterManager.getBrokerId(), host + ":" + port);
    }

    private void maybeRebalance(String group, String topic) {
        log.info("Attempting rebalance for group {} and topic {}", group, topic);

        Map<String, Long> groupHeartbeatsSource = this.consumerHeartbeats.computeIfAbsent(group, k -> new ConcurrentHashMap<>());
        long now = System.currentTimeMillis();

        List<String> activeConsumerIds = new ArrayList<>();
        new ArrayList<>(groupHeartbeatsSource.keySet()).forEach(consumerId -> {
            Long lastHeartbeat = groupHeartbeatsSource.get(consumerId);
            if (lastHeartbeat != null && (now - lastHeartbeat <= 10000)) {
                activeConsumerIds.add(consumerId);
            } else {
                log.info("Consumer {} in group {} timed out or no heartbeat. Last heartbeat at {}. Removing for rebalance.",
                        consumerId, group, lastHeartbeat == null ? "N/A" : lastHeartbeat);
                groupHeartbeatsSource.remove(consumerId);
            }
        });

        if (groupHeartbeatsSource.isEmpty()) {
            this.consumerHeartbeats.remove(group);
        }

        activeConsumerIds.sort(String::compareTo);

        Map<String, Integer> newAssignmentsForGroup = new ConcurrentHashMap<>();

        if (activeConsumerIds.isEmpty()) {
            log.info("No active consumers in group {} for topic {}. Clearing assignments.", group, topic);
            consumerAssignments.put(group, newAssignmentsForGroup);
            return;
        }

        Optional<Topic> optionalTopic = topicManager.getTopic(topic);
        if (optionalTopic.isEmpty()) {
            log.warn("Topic {} not found. Cannot perform rebalance for group {}.", topic, group);
            consumerAssignments.put(group, newAssignmentsForGroup);
            return;
        }

        List<Partition> topicPartitionsList = optionalTopic.get().getPartitions();
        if (topicPartitionsList.isEmpty()) {
            log.warn("Topic {} has no partitions. Clearing assignments for group {}.", topic, group);
            consumerAssignments.put(group, newAssignmentsForGroup);
            return;
        }

        List<Integer> sortedPartitionIds = new ArrayList<>();
        for (int i = 0; i < topicPartitionsList.size(); i++) {
            sortedPartitionIds.add(i);
        }
        sortedPartitionIds.sort(Integer::compareTo);

        for (int i = 0; i < activeConsumerIds.size(); i++) {
            String consumerId = activeConsumerIds.get(i);
            if (i < sortedPartitionIds.size()) {
                Integer partitionToAssign = sortedPartitionIds.get(i);
                newAssignmentsForGroup.put(consumerId, partitionToAssign);
                log.info("REBALANCE: Assigning partition {} (topic {}) to consumer {} (group {}).",
                        partitionToAssign, topic, consumerId, group);
            } else {
                log.info("REBALANCE: Consumer {} (group {}) not assigned a partition for topic {} (not enough partitions).",
                        consumerId, group, topic);
            }
        }

        if (sortedPartitionIds.size() > activeConsumerIds.size()) {
            log.info("REBALANCE: {} partitions remain unassigned for topic {} in group {} as there are not enough active consumers ({} consumers, {} partitions).",
                    sortedPartitionIds.size() - activeConsumerIds.size(), topic, group, activeConsumerIds.size(), sortedPartitionIds.size());
        }

        consumerAssignments.put(group, newAssignmentsForGroup);
        log.info("Rebalance complete for group {} topic {}. Assignments: {}", group, topic, newAssignmentsForGroup);
    }

    private void assignPartitionsToBrokers(String topic, List<Integer> brokerIds, int partitionCount) {
        Map<Integer, Integer> assignments = new ConcurrentHashMap<>();
        for (int i = 0; i < partitionCount; i++) {
            int brokerId = brokerIds.get(i % brokerIds.size());
            assignments.put(i, brokerId);
        }
        log.info("[FUCK] Assigned topic {}, map = {}", topic, assignments);
        partitionAssignments.put(topic, assignments);
    }

    private static class MessageTask {
        private final ChannelHandlerContext ctx;
        private final String rawMessage;

        public MessageTask(ChannelHandlerContext ctx, String rawMessage) {
            this.ctx = ctx;
            this.rawMessage = rawMessage;
        }

        public ChannelHandlerContext getCtx() {
            return ctx;
        }

        public String getRawMessage() {
            return rawMessage;
        }
    }

}
