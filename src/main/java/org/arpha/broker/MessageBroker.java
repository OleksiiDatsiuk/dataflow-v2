package org.arpha.broker;

import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import org.arpha.broker.handler.MessageBrokerHandler;
import org.arpha.server.AbstractNettyServer;

public class MessageBroker extends AbstractNettyServer {

    private final MessageBrokerHandler messageBrokerHandler;

    public MessageBroker(int port, MessageBrokerHandler messageBrokerHandler) {
        super(port);
        this.messageBrokerHandler = messageBrokerHandler;
    }

    @Override
    protected void initChannel(SocketChannel socketChannel) {
        ChannelPipeline pipeline = socketChannel.pipeline();
        pipeline.addLast(new StringDecoder());
        pipeline.addLast(new StringEncoder());
        pipeline.addLast(messageBrokerHandler);
    }

}