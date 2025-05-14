package org.arpha.cluster;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import lombok.extern.slf4j.Slf4j;
import org.arpha.broker.handler.MessageBrokerHandler;
import org.arpha.configuration.ConfigurationManager;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Slf4j
public class ClusterClient {

    public static void tryConnectToLeaderIfNotLeader(MessageBrokerHandler handler) {
        ClusterManager clusterManager = ClusterContext.get();
        if (clusterManager.isLeader()){
            return;
        }

        String leaderHost = clusterManager.getLeaderHost();
        int leaderPort = clusterManager.getLeaderPort();

        EventLoopGroup group = new NioEventLoopGroup();
        try {
            Bootstrap bootstrap = new Bootstrap();
            bootstrap.group(group)
                    .channel(NioSocketChannel.class)
                    .option(ChannelOption.SO_KEEPALIVE, true)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ChannelPipeline pipeline = ch.pipeline();
                            pipeline.addLast(new StringDecoder());
                            pipeline.addLast(new StringEncoder());
                            pipeline.addLast(handler);
                        }
                    });

            ChannelFuture future = bootstrap.connect(leaderHost, leaderPort).sync();
            Channel channel = future.channel();

            int brokerId = clusterManager.getBrokerId();
            handler.setLeaderChannel(channel);


            String host = ConfigurationManager.getINSTANCE().getProperty("broker.server.host", "localost");
            int port = ConfigurationManager.getINSTANCE().getIntProperty("broker.server.port", 43243432);
            String address = "%s:%s".formatted(host, port);

            CompletableFuture<String> ackFuture = handler.registerWithLeader(channel, brokerId, address);
            String ack = ackFuture.get(5, TimeUnit.SECONDS);
            log.info("Successfully registered to cluster: {}", ack);

        } catch (TimeoutException e) {
            log.error("Registration timed out. Broker failed to join cluster");
            System.exit(1);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to connect to cluster leader.", e);
        } finally {
            group.shutdownGracefully();
        }
    }

}
