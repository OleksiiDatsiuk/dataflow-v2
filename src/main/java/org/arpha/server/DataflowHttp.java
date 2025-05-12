package org.arpha.server;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.stream.ChunkedWriteHandler;
import lombok.RequiredArgsConstructor;
import org.arpha.broker.component.manager.TopicManager;
import org.arpha.http.routing.Router;
import org.arpha.http.endpoint.controller.TopicController;
import org.arpha.http.routing.DispatcherHandler;

@RequiredArgsConstructor
public class DataflowHttp {

    private final Router router;

    public DataflowHttp(TopicManager topicManager) {
        this.router = new Router();
        this.router.registerController(new TopicController(topicManager));
    }

    public void start(int port) throws InterruptedException {
        EventLoopGroup bossGroup = new NioEventLoopGroup();
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        try {
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<>() {
                        @Override
                        protected void initChannel(Channel ch) {
                            ChannelPipeline pipeline = ch.pipeline();
                            pipeline.addLast("codec", new HttpServerCodec());
                            pipeline.addLast("aggregator", new HttpObjectAggregator(512 * 1024));
                            pipeline.addLast("handler", new DispatcherHandler(router));
                            pipeline.addLast("chunkedWriter", new ChunkedWriteHandler());
                        }
                    });

            ChannelFuture future = bootstrap.bind(port).sync();
            future.channel().closeFuture().sync();
        } finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }
}




