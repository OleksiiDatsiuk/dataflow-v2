package org.arpha;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.TimeUnit;

@Slf4j
public class Producer {

    private final String host;
    private final int port;

    public Producer(String host, int port) {
        this.host = host;
        this.port = port;
    }

    @SneakyThrows
    public void run() throws InterruptedException {
        EventLoopGroup group = new NioEventLoopGroup();
        try {
            Bootstrap bootstrap = new Bootstrap();
            bootstrap.group(group)
                    .channel(NioSocketChannel.class)
                    .option(ChannelOption.SO_KEEPALIVE, true)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel socketChannel) {
                            socketChannel.pipeline().addLast(new StringDecoder(), new StringEncoder());
                        }
                    });

            ChannelFuture channelFuture = bootstrap.connect(host, port).sync();
            log.info("Producer connected to broker at {}:{}", host, port);

            for (int i = 1; i <= 10; i++) {
                String message = """
                        {
                          "type": "PRODUCER",
                          "topic": "another_4_topic",
                          "content": "my_topic %s"
                        }
                        """.formatted(i);
                log.info("Sending message: {}", message);
                channelFuture.channel().writeAndFlush(message);
                TimeUnit.MILLISECONDS.sleep(100);
            }

            channelFuture.channel().closeFuture().sync();
        } finally {
            group.shutdownGracefully();
        }
    }

}