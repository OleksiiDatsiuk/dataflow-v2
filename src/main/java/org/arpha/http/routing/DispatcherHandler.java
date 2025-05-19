package org.arpha.http.routing;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import lombok.SneakyThrows;
import org.arpha.http.annotation.RequestBody;
import org.arpha.http.common.HttpMethod;
import org.arpha.http.annotation.PathParam;
import org.arpha.http.streaming.SseStream;

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;

public class DispatcherHandler extends SimpleChannelInboundHandler<FullHttpRequest> {

    private final Router router;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public DispatcherHandler(Router router) {
        this.router = router;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest request) {
        String uri = request.uri().split("\\?")[0]; // remove query params
        String method = request.method().name();

        if (method.equals("OPTIONS")) {
            FullHttpResponse optionsResponse = buildOptionsResponse();
            ctx.writeAndFlush(optionsResponse).addListener(ChannelFutureListener.CLOSE);
            return;
        }

        try {
            var routeMatchOpt = router.findRoute(uri, method);
            if (routeMatchOpt.isPresent()) {
                var routeMatch = routeMatchOpt.get();
                Object result = invokeRoute(routeMatch, request, ctx);
                if (!(result instanceof SseStream)) {
                    FullHttpResponse httpResponse = buildResponse(HttpResponseStatus.OK, result);
                    ctx.writeAndFlush(httpResponse).addListener(ChannelFutureListener.CLOSE);
                }
            } else {
                FullHttpResponse response = buildResponse(HttpResponseStatus.NOT_FOUND, "Unknown endpoint");
                ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
            }
        } catch (Exception e) {
            FullHttpResponse response = buildResponse(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                    "Internal error: " + e.getMessage());
            ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
        }
    }


    @SneakyThrows
    private Object invokeRoute(RouteMatch routeMatch, FullHttpRequest request, ChannelHandlerContext ctx) {
        Method handler = routeMatch.route().handlerMethod();
        Object controller = routeMatch.route().controller();
        Parameter[] parameters = handler.getParameters();
        Object[] args = new Object[parameters.length];

        String requestBody = request.content().toString(java.nio.charset.StandardCharsets.UTF_8);

        for (int i = 0; i < parameters.length; i++) {
            Parameter param = parameters[i];

            if (param.getType().equals(FullHttpRequest.class)) {
                args[i] = request;

            } else if (param.getType().equals(ChannelHandlerContext.class)) {
                args[i] = ctx;

            } else if (param.isAnnotationPresent(PathParam.class)) {
                String name = param.getAnnotation(PathParam.class).value();
                args[i] = routeMatch.pathParams().get(name);

            } else if (param.isAnnotationPresent(RequestBody.class)) {
                if (param.getType().equals(String.class)) {
                    args[i] = requestBody;
                } else {
                    args[i] = objectMapper.readValue(requestBody, param.getType());
                }

            } else {
                args[i] = getQueryParam(request.uri(), param.getName());
            }
        }


        return handler.invoke(controller, args);
    }

    private String getQueryParam(String uri, String param) {
        String[] parts = uri.split("\\?");
        if (parts.length > 1) {
            String query = parts[1];
            for (String pair : query.split("&")) {
                String[] keyValue = pair.split("=");
                if (keyValue.length == 2 && keyValue[0].equals(param)) {
                    return keyValue[1];
                }
            }
        }
        return null;
    }

    @SneakyThrows
    private FullHttpResponse buildResponse(HttpResponseStatus status, Object message) {
        byte[] jsonBytes = objectMapper.writeValueAsBytes(message);
        FullHttpResponse response = new DefaultFullHttpResponse(
                HttpVersion.HTTP_1_1,
                status,
                Unpooled.copiedBuffer(jsonBytes)
        );
        addCorsHeaders(response);
        response.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/json; charset=UTF-8");
        response.headers().set(HttpHeaderNames.CONTENT_LENGTH, jsonBytes.length);
        return response;
    }

    private FullHttpResponse buildOptionsResponse() {
        FullHttpResponse response = new DefaultFullHttpResponse(
                HttpVersion.HTTP_1_1,
                HttpResponseStatus.OK,
                Unpooled.EMPTY_BUFFER
        );
        addCorsHeaders(response);
        response.headers().set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS");
        response.headers().set("Access-Control-Allow-Headers", "Content-Type, Authorization");
        return response;
    }

    private void addCorsHeaders(FullHttpResponse response) {
        response.headers().set("Access-Control-Allow-Origin", "http://localhost:3000");
        response.headers().set("Access-Control-Allow-Credentials", "true");
    }

}
