package org.arpha;

import com.fasterxml.jackson.databind.ObjectMapper;
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

import java.util.HashMap;
import java.util.Map;
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

            // Example of sending messages
            for (int i = 1; i <= 20; i++) {
                ObjectMapper mapper = new ObjectMapper();

                Map<String, String> contentMap = new HashMap<>() {{
                    put("key_1", "value_1");
                    put("key_2", "value_2");
                    put("key_3", "value_3");
                    put("key_4", "value_4");
                    put("key_5", "value_5");
                    put("key_6", "value_6");
                    put("key_7", "value_7");
                    put("key_8", "value_8");
                    put("key_9", "value_9");
                    put("key_10", "value_10");
                    put("key_11", "value_11");
                    put("key_12", "value_12");
                    put("key_13", "value_13");
                    put("key_14", "value_14");
                    put("key_15", "value_15");
                    put("key_16", "value_16");
                    put("key_17", "value_17");
                    put("key_18", "value_18");
                    put("key_19", "value_19");
                    put("key_20", "value_20");
                    put("key_21", "value_21");
                    put("key_22", "value_22");
                    put("key_23", "value_23");
                    put("key_24", "value_24");
                    put("key_25", "value_25");
                    put("key_26", "value_26");
                    put("key_27", "value_27");
                    put("key_28", "value_28");
                    put("key_29", "value_29");
                    put("key_30", "value_30");
                    put("key_31", "value_31");
                    put("key_32", "value_32");
                    put("key_33", "value_33");
                    put("key_34", "value_34");
                    put("key_35", "value_35");
                    put("key_36", "value_36");
                    put("key_37", "value_37");
                    put("key_38", "value_38");
                    put("key_39", "value_39");
                    put("key_40", "value_40");
                    put("key_41", "value_41");
                    put("key_42", "value_42");
                    put("key_43", "value_43");
                    put("key_44", "value_44");
                    put("key_45", "value_45");
                    put("key_46", "value_46");
                    put("key_47", "value_47");
                    put("key_48", "value_48");
                    put("key_49", "value_49");
                    put("key_50", "value_50");
                    put("key_51", "value_51");
                    put("key_52", "value_52");
                    put("key_53", "value_53");
                    put("key_54", "value_54");
                    put("key_55", "value_55");
                    put("key_56", "value_56");
                    put("key_57", "value_57");
                    put("key_58", "value_58");
                    put("key_59", "value_59");
                    put("key_60", "value_60");
                    put("key_61", "value_61");
                    put("key_62", "value_62");
                    put("key_63", "value_63");
                    put("key_64", "value_64");
                    put("key_65", "value_65");
                    put("key_66", "value_66");
                    put("key_67", "value_67");
                    put("key_68", "value_68");
                    put("key_69", "value_69");
                    put("key_70", "value_70");
                    put("key_71", "value_71");
                    put("key_72", "value_72");
                    put("key_73", "value_73");
                    put("key_74", "value_74");
                    put("key_75", "value_75");
                    put("key_76", "value_76");
                    put("key_77", "value_77");
                    put("key_78", "value_78");
                    put("key_79", "value_79");
                    put("key_80", "value_80");
                    put("key_81", "value_81");
                    put("key_82", "value_82");
                    put("key_83", "value_83");
                    put("key_84", "value_84");
                    put("key_85", "value_85");
                    put("key_86", "value_86");
                    put("key_87", "value_87");
                    put("key_88", "value_88");
                    put("key_89", "value_89");
                    put("key_90", "value_90");
                    put("key_91", "value_91");
                    put("key_92", "value_92");
                    put("key_93", "value_93");
                    put("key_94", "value_94");
                    put("key_95", "value_95");
                    put("key_96", "value_96");
                    put("key_97", "value_97");
                    put("key_98", "value_98");
                    put("key_99", "value_99");
                    put("key_100", "value_100");
                }};

                String innerContent = mapper.writeValueAsString(contentMap);

                String message = mapper.writeValueAsString(Map.of(
                        "type", "PRODUCER",
                        "topic", "my_topic",
                        "content", innerContent
                ));

                log.info("Sending message: {}", message);
                channelFuture.channel().writeAndFlush(message);
                TimeUnit.MILLISECONDS.sleep(100);
            }

            for (int i = 1; i <= 10; i++) {
                String message = """
                        {
                          "type": "PRODUCER",
                          "topic": "my_topic2",
                          "content": "my_topic2 %s"
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