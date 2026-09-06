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
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.handler.timeout.IdleStateHandler;
import org.bukkit.plugin.java.JavaPlugin;

import java.net.URI;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;

public class App extends JavaPlugin {

    // ================= 核心配置 =================
    private static final String WORKER_WSS_URL = "wss://mctest.uuz.us.kg/agent-tunnel";
    private static final String UUID = "8c8244fb-d577-4d20-90e3-788a0977b001";
    // ============================================

    private static final byte[] UUID_BYTES = hexStringToByteArray(UUID.replace("-", ""));
    private static final AtomicBoolean RUNNING = new AtomicBoolean(false);
    private static volatile EventLoopGroup group;
    private static volatile Channel clientChannel;

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
                proxyHandler.handleFrame(ctx, (WebSocketFrame) msg);
            }
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            proxyHandler.clearPendingQueue();
            scheduleReconnect();
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            proxyHandler.clearPendingQueue();
            ctx.close();
        }
    }

    static class WebSocketProxyHandler {
        private enum State { UNCONNECTED, CONNECTING, CONNECTED }

        private State state = State.UNCONNECTED;
        private Channel targetChannel;
        private final Queue<ByteBuf> pendingQueue = new LinkedList<>();

        public synchronized void handleFrame(ChannelHandlerContext ctx, WebSocketFrame frame) {
            if (!(frame instanceof BinaryWebSocketFrame)) return;

            ByteBuf buf = frame.content();

            // 1. 已建立 TCP 连接，直接转发
            if (state == State.CONNECTED) {
                if (targetChannel != null && targetChannel.isActive()) {
                    targetChannel.writeAndFlush(buf.retain());
                }
                return;
            }

            // 2. 正在建立 TCP 连接中，暂存后续到达的数据包
            if (state == State.CONNECTING) {
                pendingQueue.add(buf.retain());
                return;
            }

            // 3. UNCONNECTED 状态：安全解析 VLESS 头部
            if (buf.readableBytes() < 18) return;

            int markIdx = buf.readerIndex();

            byte version = buf.readByte();
            byte[] clientUuid = new byte[16];
            buf.readBytes(clientUuid);

            if (!Arrays.equals(clientUuid, UUID_BYTES)) {
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
                targetHost = new String(domain, java.nio.charset.StandardCharsets.UTF_8);
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
                ctx.close();
                return;
            }

            final String host = targetHost;
            final byte reqVersion = version;

            // 头部后面的剩余数据即为首包 Payload
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
                                    ctx.close();
                                }

                                @Override
                                public void exceptionCaught(ChannelHandlerContext targetCtx, Throwable cause) {
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
                        // 一次性 Flush 建立连接期间挂起的所有数据包
                        while (!pendingQueue.isEmpty()) {
                            targetChannel.write(pendingQueue.poll());
                        }
                        targetChannel.flush();
                    } else {
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

    private static byte[] hexStringToByteArray(String s) {
        int len = s.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4) + Character.digit(s.charAt(i + 1), 16));
        }
        return data;
    }
}
