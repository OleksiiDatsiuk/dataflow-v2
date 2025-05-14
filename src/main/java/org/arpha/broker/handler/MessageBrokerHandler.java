package org.arpha.broker.handler;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
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
import org.arpha.broker.component.manager.TopicManager;
import org.arpha.broker.handler.dto.BrokerRegistrationMessage;
import org.arpha.cluster.ClusterContext;
import org.arpha.cluster.ClusterManager;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

@Slf4j
public class MessageBrokerHandler extends SimpleChannelInboundHandler<String> {

    private final TopicManager topicManager;
    private final Map<Channel, MessageType> connectionRegistry;
    private final BlockingQueue<MessageTask> messageQueue;
    private final ExecutorService executorService;

    private volatile Channel leaderChannel;

    private final Map<Integer, CompletableFuture<String>> registrationAcks = new ConcurrentHashMap<>();
    private static volatile boolean isRegistered = false;

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
                default -> handleUnknownMessage(task.getCtx(), message);
            }
            log.info("Successfully processed message: {}", message);
        } catch (IllegalArgumentException e) {
            log.error("Invalid message format received: {}", task.getRawMessage(), e);
        } catch (Exception e) {
            log.error("Failed to process message: {}", task.getRawMessage(), e);
        }
    }

    private void handleBrokerAck(ChannelHandlerContext ctx, Message message) {
        registrationAcks.values().forEach(future -> future.complete(message.getContent()));
        connectionRegistry.put(ctx.channel(), MessageType.BROKER);
        isRegistered = true;
    }


    private void handleProducerMessage(ChannelHandlerContext ctx, Message message) {
        String topic = message.getTopic();
        topicManager.addMessageToTopic(topic, message.getContent());
    }

    private void handleConsumerMessage(ChannelHandlerContext ctx, Message message) {
        String topic = message.getTopic();
        String nextMessage = topicManager.getNextMessageFromTopic(topic);
        if (nextMessage != null) {
            ctx.writeAndFlush(nextMessage);
        }
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

            clusterManager.registerBroker(dto.getBrokerId(), dto.getAddress());
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
            log.info("Handling hearbeat message from broker {}", brokerId);

            ClusterManager clusterManager = ClusterContext.get();
            if (clusterManager.isLeader()) {
                clusterManager.getHeartbeatTimestamps().put(brokerId, System.currentTimeMillis());
                log.info("Heartbeat received from broker {}", brokerId);
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
