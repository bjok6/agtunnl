package com.example.sbx;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.websocketx.*;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import org.bukkit.plugin.java.JavaPlugin;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class App extends JavaPlugin {

    // =========================================================================
    // 核心配置区
    // =========================================================================
    // 调试开关：排查问题时设为 true，控制台会输出详细连接日志；连通后改回 false 即可静默
    private static final boolean DEBUG = true;

    // 实际连接的服务器 IP 或域名（可使用优选域名/IP，如 "cf.877774.xyz" 或直接 "mctest.uuz.us.kg"）
    private static final String CONNECT_HOST = "cf.877774.xyz";
    private static final int CONNECT_PORT = 443;

    // Cloudflare 绑定的 SNI 和 Host 域名
    private static final String SNI_HOST = "mctest.uuz.us.kg";
    private static final String TUNNEL_PATH = "/agent-tunnel";

    // 凭据与隧道池配置
    private static final String UUID = "8c8244fb-d577-4d20-90e3-788a0977b001";
    private static final int TARGET_STANDBY_POOL_SIZE = 5; // 保持 5 条常态预热隧道
    // =========================================================================

    private static final byte[] UUID_BYTES = hexStringToByteArray(UUID.replace("-", ""));
    private static final AtomicBoolean RUNNING = new AtomicBoolean(false);
    private static final AtomicInteger STANDBY_COUNT = new AtomicInteger(0);
    private static volatile EventLoopGroup group;

    public static void main(String[] args) {
        start();
    }

    @Override
    public void onEnable() {
        start();
    }

    @Override
    public void onDisable() {
        stop();
    }

    public static void start() {
        if (!RUNNING.compareAndSet(false, true)) return;
        log("Agent 代理服务正在启动...");
        group = new NioEventLoopGroup();
        maintainPool();
    }

    public static void stop() {
        if (!RUNNING.getAndSet(false)) return;
        log("Agent 代理服务正在停止...");
        try {
            if (group != null) group.shutdownGracefully();
        } catch (Exception ignored) {}
    }

    private static synchronized void maintainPool() {
        if (!RUNNING.get() || group == null) return;
        int needed = TARGET_STANDBY_POOL_SIZE - STANDBY_COUNT.get();
        if (needed > 0) {
            log("当前预热隧道数量: " + STANDBY_COUNT.get() + " / " + TARGET_STANDBY_POOL_SIZE + "，正在补充 " + needed + " 条隧道...");
            for (int i = 0; i < needed; i++) {
                connectOneTunnel();
            }
        }
    }

    private static void connectOneTunnel() {
        if (!RUNNING.get()) return;

        try {
            String wsUrl = "wss://" + CONNECT_HOST + ":" + CONNECT_PORT + TUNNEL_PATH;
            URI uri = new URI(wsUrl);

            // SSL 配置
            SslContext sslCtx = SslContextBuilder.forClient()
                    .trustManager(InsecureTrustManagerFactory.INSTANCE)
                    .build();

            // WebSocket 请求头（显式传入 Host 头部，满足 Cloudflare 校验）
            DefaultHttpHeaders headers = new DefaultHttpHeaders();
            headers.add("Host", SNI_HOST);
            headers.add("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36");

            WebSocketClientHandshaker handshaker = WebSocketClientHandshakerFactory.newHandshaker(
                    uri, WebSocketVersion.V13, null, true, headers);

            Bootstrap b = new Bootstrap();
            b.group(group)
                    .channel(NioSocketChannel.class)
                    .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 10000)
                    .option(ChannelOption.TCP_NODELAY, true)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ChannelPipeline p = ch.pipeline();

                            // 1. 显式配置 SNI 的 SSL 处理器
                            SSLEngine sslEngine = sslCtx.newEngine(ch.alloc(), SNI_HOST, CONNECT_PORT);
                            SSLParameters sslParams = sslEngine.getSSLParameters();
                            sslParams.setEndpointIdentificationAlgorithm("HTTPS");
                            sslEngine.setSSLParameters(sslParams);

                            p.addLast(new SslHandler(sslEngine));
                            p.addLast(new HttpClientCodec());
                            p.addLast(new HttpObjectAggregator(65536));
                            p.addLast(new IdleStateHandler(25, 25, 0)); // 25s 发送 Ping 心跳
                            p.addLast(new OutboundTunnelHandler(handshaker));
                        }
                    });

            b.connect(CONNECT_HOST, CONNECT_PORT).addListener((ChannelFutureListener) future -> {
                if (!future.isSuccess()) {
                    logErr("TCP/TLS 建立失败 (" + CONNECT_HOST + ":" + CONNECT_PORT + "): " + future.cause().getMessage(), null);
                    schedulePoolCheck(3);
                }
            });
        } catch (Exception e) {
            logErr("构建连接时发生错误: " + e.getMessage(), e);
            schedulePoolCheck(3);
        }
    }

    private static void schedulePoolCheck(long delaySeconds) {
        if (!RUNNING.get() || group == null) return;
        group.schedule(App::maintainPool, delaySeconds, TimeUnit.SECONDS);
    }

    static class OutboundTunnelHandler extends SimpleChannelInboundHandler<Object> {
        private final WebSocketClientHandshaker handshaker;
        private final WebSocketProxyHandler proxyHandler = new WebSocketProxyHandler();
        private boolean isStandbyCounted = false;

        public OutboundTunnelHandler(WebSocketClientHandshaker handshaker) {
            this.handshaker = handshaker;
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) {
            log("TCP/TLS 连接成功，发起 WebSocket 握手请求 -> " + TUNNEL_PATH);
            handshaker.handshake(ctx.channel());
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
            if (!handshaker.isHandshakeComplete()) {
                handshaker.finishHandshake(ctx.channel(), (FullHttpResponse) msg);
                STANDBY_COUNT.incrementAndGet();
                isStandbyCounted = true;
                log("WebSocket 握手成功！隧道已ready并入池，当前池大小: " + STANDBY_COUNT.get());
                maintainPool();
                return;
            }

            if (msg instanceof WebSocketFrame) {
                WebSocketFrame frame = (WebSocketFrame) msg;
                if (frame instanceof BinaryWebSocketFrame) {
                    if (isStandbyCounted && !proxyHandler.isAssigned()) {
                        proxyHandler.setAssigned(true);
                        STANDBY_COUNT.decrementAndGet();
                        isStandbyCounted = false;
                        log("收到客户端数据，隧道激活脱离预热池！剩余预热隧道数: " + STANDBY_COUNT.get());
                        maintainPool();
                    }
                    proxyHandler.handleFrame(ctx, (BinaryWebSocketFrame) frame);
                } else if (frame instanceof PingWebSocketFrame) {
                    ctx.writeAndFlush(new PongWebSocketFrame(frame.content().retain()));
                } else if (frame instanceof CloseWebSocketFrame) {
                    log("收到 Worker 发来的 Close 帧");
                    ctx.close();
                }
            }
        }

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
            if (evt instanceof IdleStateEvent) {
                if (ctx.channel().isActive()) {
                    ctx.writeAndFlush(new PingWebSocketFrame());
                }
            } else {
                super.userEventTriggered(ctx, evt);
            }
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            if (isStandbyCounted) {
                STANDBY_COUNT.decrementAndGet();
                isStandbyCounted = false;
                log("预热隧道断开，当前池大小: " + STANDBY_COUNT.get());
            } else {
                log("活跃数据隧道断开");
            }
            proxyHandler.clearPendingQueue();
            maintainPool();
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            logErr("隧道异常: " + cause.getMessage(), cause);
            if (isStandbyCounted) {
                STANDBY_COUNT.decrementAndGet();
                isStandbyCounted = false;
            }
            proxyHandler.clearPendingQueue();
            ctx.close();
        }
    }

    static class WebSocketProxyHandler {
        private enum State { UNCONNECTED, CONNECTING, CONNECTED }

        private State state = State.UNCONNECTED;
        private boolean assigned = false;
        private Channel targetChannel;
        private final Queue<ByteBuf> pendingQueue = new LinkedList<>();

        public boolean isAssigned() {
            return assigned;
        }

        public void setAssigned(boolean assigned) {
            this.assigned = assigned;
        }

        public synchronized void handleFrame(ChannelHandlerContext ctx, BinaryWebSocketFrame frame) {
            ByteBuf buf = frame.content();

            // 1. TCP 连接已建好，直接转发
            if (state == State.CONNECTED) {
                if (targetChannel != null && targetChannel.isActive()) {
                    targetChannel.writeAndFlush(buf.retain());
                }
                return;
            }

            // 2. 正在建立 TCP 连接，挂起后续数据包
            if (state == State.CONNECTING) {
                pendingQueue.add(buf.retain());
                return;
            }

            // 3. UNCONNECTED 状态：解析 VLESS 报文头
            if (buf.readableBytes() < 18) {
                logErr("VLESS 头部数据长度不足 18 字节，丢弃", null);
                return;
            }

            int markIdx = buf.readerIndex();

            byte version = buf.readByte();
            byte[] clientUuid = new byte[16];
            buf.readBytes(clientUuid);

            if (!Arrays.equals(clientUuid, UUID_BYTES)) {
                logErr("UUID 校验失败，来自非法请求！关闭连接", null);
                ctx.close();
                return;
            }

            if (buf.readableBytes() < 1) { buf.readerIndex(markIdx); return; }
            byte addonLen = buf.readByte();
            if (buf.readableBytes() < (addonLen & 0xFF)) { buf.readerIndex(markIdx); return; }
            if (addonLen > 0) buf.skipBytes(addonLen & 0xFF);

            if (buf.readableBytes() < 4) { buf.readerIndex(markIdx); return; }
            byte command = buf.readByte(); // 0x01: TCP
            int port = buf.readUnsignedShort();
            byte addressType = buf.readByte();

            String targetHost = "";
            if (addressType == 0x01) { // IPv4
                if (buf.readableBytes() < 4) { buf.readerIndex(markIdx); return; }
                byte[] ip = new byte[4];
                buf.readBytes(ip);
                targetHost = (ip[0] & 0xFF) + "." + (ip[1] & 0xFF) + "." + (ip[2] & 0xFF) + "." + (ip[3] & 0xFF);
            } else if (addressType == 0x02) { // Domain
                if (buf.readableBytes() < 1) { buf.readerIndex(markIdx); return; }
                int domainLen = buf.readByte() & 0xFF;
                if (buf.readableBytes() < domainLen) { buf.readerIndex(markIdx); return; }
                byte[] domain = new byte[domainLen];
                buf.readBytes(domain);
                targetHost = new String(domain, StandardCharsets.UTF_8);
            } else if (addressType == 0x03) { // IPv6
                if (buf.readableBytes() < 16) { buf.readerIndex(markIdx); return; }
                byte[] ip = new byte[16];
                buf.readBytes(ip);
                try {
                    targetHost = java.net.InetAddress.getByAddress(ip).getHostAddress();
                } catch (Exception e) {
                    targetHost = "127.0.0.1";
                }
            } else {
                logErr("不支持的地址类型: " + addressType, null);
                ctx.close();
                return;
            }

            final String host = targetHost;
            final byte reqVersion = version;

            log("解析 VLESS 头部成功 -> 目标地址: " + host + ":" + port + "，正在建立本地/目标 TCP 连接...");

            if (buf.readableBytes() > 0) {
                pendingQueue.add(buf.readBytes(buf.readableBytes()));
            }

            state = State.CONNECTING;

            Bootstrap b = new Bootstrap();
            b.group(ctx.channel().eventLoop())
                    .channel(NioSocketChannel.class)
                    .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 10000)
                    .option(ChannelOption.TCP_NODELAY, true)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ch.pipeline().addLast(new SimpleChannelInboundHandler<ByteBuf>() {
                                private boolean isFirstRead = true;

                                @Override
                                protected void channelRead0(ChannelHandlerContext targetCtx, ByteBuf msg) {
                                    if (ctx.channel().isActive()) {
                                        ByteBuf response = ctx.alloc().buffer();
                                        if (isFirstRead) {
                                            response.writeByte(reqVersion); // VLESS 响应头 version
                                            response.writeByte(0);          // addon length 0
                                            isFirstRead = false;
                                        }
                                        response.writeBytes(msg);
                                        ctx.channel().writeAndFlush(new BinaryWebSocketFrame(response));
                                    }
                                }

                                @Override
                                public void channelInactive(ChannelHandlerContext targetCtx) {
                                    log("目标 TCP 连接断开: " + host + ":" + port);
                                    ctx.close();
                                }

                                @Override
                                public void exceptionCaught(ChannelHandlerContext targetCtx, Throwable cause) {
                                    logErr("目标 TCP 连接异常 (" + host + ":" + port + "): " + cause.getMessage(), null);
                                    targetCtx.close();
                                    ctx.close();
                                }
                            });
                        }
                    });

            b.connect(host, port).addListener((ChannelFutureListener) future -> {
                synchronized (WebSocketProxyHandler.this) {
                    if (future.isSuccess()) {
                        targetChannel = future.channel();
                        state = State.CONNECTED;
                        log("连接目标成功 (" + host + ":" + port + ")！开始双向转发数据");
                        while (!pendingQueue.isEmpty()) {
                            targetChannel.write(pendingQueue.poll());
                        }
                        targetChannel.flush();
                    } else {
                        logErr("连接目标失败 (" + host + ":" + port + "): " + future.cause().getMessage(), null);
                        clearPendingQueue();
                        ctx.close();
                    }
                }
            });
        }

        public synchronized void clearPendingQueue() {
            while (!pendingQueue.isEmpty()) {
                ByteBuf buf = pendingQueue.poll();
                if (buf != null && buf.refCnt() > 0) {
                    buf.release();
                }
            }
        }
    }

    private static void log(String msg) {
        if (DEBUG) {
            System.out.println("[AgentTunnel] " + msg);
        }
    }

    private static void logErr(String msg, Throwable t) {
        if (DEBUG) {
            System.err.println("[AgentTunnel Error] " + msg);
            if (t != null) t.printStackTrace();
        }
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
