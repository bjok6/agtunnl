package com.example.sbx;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.handler.codec.http.websocketx.*;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.ReferenceCountUtil;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.CRC32;

public class App {

    // ================= 核心配置区 =================
    // 替换为你的 Cloudflare Worker 隧道连接地址 (WSS 协议)
    private static final String WORKER_WSS_URL = "wss://mctest.uuz.us.kg/agent-tunnel";
    
    // 节点 UUID
    private static final String UUID = "8c8244fb-d577-4d20-90e3-788a0977b001";
    // ==============================================

    private static final byte[] UUID_BYTES = hexStringToByteArray(UUID.replace("-", ""));
    private static final AtomicBoolean RUNNING = new AtomicBoolean(false);
    private static volatile EventLoopGroup group;
    private static volatile Channel clientChannel;

    public static void main(String[] args) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            stop();
        }, "shutdown-hook"));
        start();
    }

    public static void start() {
        if (!RUNNING.compareAndSet(false, true)) return;

        group = new NioEventLoopGroup();
        connectOutboundTunnel();
    }

    public static void stop() {
        if (!RUNNING.getAndSet(false)) return;
        try {
            if (clientChannel != null && clientChannel.isOpen()) clientChannel.close();
            if (group != null) group.shutdownGracefully();
        } catch (Exception ignored) {}
    }

    /** 主动发起的 WSS 客户端长连接（无需本地开放任何端口） */
    private static void connectOutboundTunnel() {
        if (!RUNNING.get()) return;

        try {
            URI uri = new URI(WORKER_WSS_URL);
            String host = uri.getHost();
            int port = uri.getPort() == -1 ? 443 : uri.getPort();

            SslContext sslCtx = SslContextBuilder.forClient()
                    .trustManager(InsecureTrustManagerFactory.INSTANCE).build();

            WebSocketClientHandshaker handshaker = WebSocketClientHandshakerFactory.newHandshaker(
                    uri, WebSocketVersion.V13, null, true, new DefaultHttpHeaders());

            Bootstrap b = new Bootstrap();
            b.group(group)
                    .channel(NioSocketChannel.class)
                    .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 10000)
                    .option(ChannelOption.TCP_NODELAY, true)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ChannelPipeline p = ch.pipeline();
                            p.addLast(sslCtx.newHandler(ch.alloc(), host, port));
                            p.addLast(new HttpClientCodec());
                            p.addLast(new HttpObjectAggregator(65536));
                            p.addLast(new IdleStateHandler(60, 60, 0));
                            p.addLast(new OutboundTunnelHandler(handshaker));
                        }
                    });

            b.connect(host, port).addListener((ChannelFutureListener) future -> {
                if (future.isSuccess()) {
                    clientChannel = future.channel();
                } else {
                    // 失败自动断线重连（叠加随机延迟避免机械抖动）
                    scheduleReconnect();
                }
            });
        } catch (Exception e) {
            scheduleReconnect();
        }
    }

    private static void scheduleReconnect() {
        if (!RUNNING.get()) return;
        group.schedule(App::connectOutboundTunnel, 5 + (long)(Math.random() * 5), java.util.concurrent.TimeUnit.SECONDS);
    }

    // ========================================================
    // 内存处理逻辑：接管 Worker 传送过来的 WebSocket 帧并解包 VLESS
    // ========================================================
    static class OutboundTunnelHandler extends SimpleChannelInboundHandler<Object> {
        private final WebSocketClientHandshaker handshaker;
        private final WebSocketProxyHandler proxyHandler = new WebSocketProxyHandler();

        public OutboundTunnelHandler(WebSocketClientHandshaker handshaker) {
            this.handshaker = handshaker;
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) {
            handshaker.handshake(ctx.channel());
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
            if (!handshaker.isHandshakeComplete()) {
                handshaker.finishHandshake(ctx.channel(), (FullHttpResponse) msg);
                return;
            }

            if (msg instanceof WebSocketFrame) {
                proxyHandler.channelRead0(ctx, (WebSocketFrame) msg);
            }
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            scheduleReconnect();
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            ctx.close();
        }
    }

    // 内部保持原有的纯内存 VLESS 解析器 (WebSocketProxyHandler 与 TargetHandler)
    static class WebSocketProxyHandler {
        // 保留原有的 VLESS 解析逻辑...
    }

    private static byte[] hexStringToByteArray(String s) {
        int len = s.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4) + Character.digit(s.charAt(i + 1), 16));
        }
        return data;
    }
}
