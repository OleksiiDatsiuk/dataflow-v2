package org.arpha.broker.handler;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.extern.slf4j.Slf4j;
import org.arpha.broker.component.manager.TopicManager;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
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

    public MessageBrokerHandler(TopicManager topicManager) {
        this.topicManager = topicManager;
        this.connectionRegistry = new ConcurrentHashMap<>();
        this.messageQueue = new LinkedBlockingQueue<>();
        this.executorService = Executors.newFixedThreadPool(10);
        startMessageProcessing();
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
                default -> handleUnknownMessage(task.getCtx(), message);
            }
            log.info("Successfully processed message: {}", message);
        } catch (IllegalArgumentException e) {
            log.error("Invalid message format received: {}", task.getRawMessage(), e);
        } catch (Exception e) {
            log.error("Failed to process message: {}", task.getRawMessage(), e);
        }
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

    private void handleUnknownMessage(ChannelHandlerContext ctx, Message message) {
        log.warn("Received unknown message: {}", message);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("Exception caught: ", cause);
        ctx.close();
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
