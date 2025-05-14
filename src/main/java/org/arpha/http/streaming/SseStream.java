package org.arpha.http.streaming;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.*;

import java.util.Map;

import static io.netty.util.CharsetUtil.UTF_8;

public class SseStream {
    private final ChannelHandlerContext ctx;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public SseStream(ChannelHandlerContext ctx) {
        this.ctx = ctx;

        DefaultHttpResponse response = new DefaultHttpResponse(
                HttpVersion.HTTP_1_1,
                HttpResponseStatus.OK
        );

        HttpHeaders headers = response.headers();
        headers.set(HttpHeaderNames.CONTENT_TYPE, "text/event-stream; charset=UTF-8");
        headers.set(HttpHeaderNames.CACHE_CONTROL, "no-cache");
        headers.set(HttpHeaderNames.CONNECTION, "keep-alive");
        headers.set(HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN, "http://localhost:3000");
        headers.set(HttpHeaderNames.ACCESS_CONTROL_ALLOW_HEADERS, "Content-Type");
        headers.set(HttpHeaderNames.ACCESS_CONTROL_ALLOW_METHODS, "GET");
        headers.set(HttpHeaderNames.ACCESS_CONTROL_ALLOW_CREDENTIALS, "true");

        HttpUtil.setTransferEncodingChunked(response, true);

        ctx.writeAndFlush(response);
    }

    public void sendEvent(Object event, Integer partitionId) {
        try {
            Object payload;

            if (event instanceof String) {
                payload = Map.of("message", event, "partition", partitionId);
            } else {
                payload = event;
            }

            String json = objectMapper.writeValueAsString(payload);
            String formatted = "data: " + json + "\n\n";

            ByteBuf buf = Unpooled.copiedBuffer(formatted, UTF_8);
            ctx.writeAndFlush(new DefaultHttpContent(buf));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
