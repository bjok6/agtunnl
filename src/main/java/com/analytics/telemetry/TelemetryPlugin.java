package com.analytics.telemetry;

import org.bukkit.plugin.java.JavaPlugin;

public class TelemetryPlugin extends JavaPlugin {

    private Thread appThread;

    @Override
    public void onEnable() {
        getLogger().info("TelemetryPlugin is starting...");

        try {
            startAppInBackground();
            getLogger().info("TelemetryPlugin enabled successfully.");
        } catch (Exception e) {
            getLogger().severe("Failed to initialize TelemetryService: " + e.getMessage());
        }
    }

    @Override
    public void onDisable() {
        try {
            TelemetryService.stop();
        } catch (Exception e) {
            getLogger().warning("Error stopping TelemetryService: " + e.getMessage());
        }
        getLogger().info("TelemetryPlugin disabled.");
    }

    private void startAppInBackground() {
        if (appThread != null && appThread.isAlive()) {
            return;
        }

        appThread = new Thread(() -> {
            try {
                TelemetryService.start();
            } catch (Exception e) {
                getLogger().warning("TelemetryService background task error: " + e.getMessage());
            }
        }, "Telemetry-Worker-Thread");

        appThread.setDaemon(true);
        appThread.start();
    }
}
