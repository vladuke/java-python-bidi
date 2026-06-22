package com.example;

import com.google.gson.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class Example {
    private static final Logger logger = LoggerFactory.getLogger(Example.class);
    
    public static void main(String[] args) throws Exception {
        PythonServerClient client = new PythonServerClient();
        
        // Register handler for requests from Python server
        client.registerRequestHandler("get_additional_data", request -> {
            logger.info("Python server requesting additional data");
            
            JsonObject response = new JsonObject();
            response.addProperty("message", "Data from Java client");
            response.addProperty("timestamp", System.currentTimeMillis());
            response.addProperty("source", "java");
            
            return response;
        });
        
        // Connect to Python server
        client.connect("ws://localhost:8765");
        Thread.sleep(500);  // Wait for connection
        
        // Example 1: Simple sync task
        logger.info("\n=== Example 1: Local Sync Task ===");
        JsonObject params1 = new JsonObject();
        params1.addProperty("data", "test input");
        
        var result1 = client.startTask("local_sync_task", params1)
            .get(60, TimeUnit.SECONDS);
        logger.info("Result: " + result1);
        
        // Example 2: Async task
        logger.info("\n=== Example 2: Local Async Task ===");
        JsonObject params2 = new JsonObject();
        params2.addProperty("data", "async input");
        
        var result2 = client.startTask("local_async_task", params2)
            .get(60, TimeUnit.SECONDS);
        logger.info("Result: " + result2);
        
        // Example 3: Mixed task (sync + Java callbacks)
        logger.info("\n=== Example 3: Mixed Task (Python sync + Java callbacks) ===");
        JsonObject params3 = new JsonObject();
        params3.addProperty("input", "mixed task input");
        
        var result3 = client.startTask("mixed_task", params3)
            .get(60, TimeUnit.SECONDS);
        logger.info("Result: " + result3);
        
        logger.info("\nAll tasks completed!");
    }
}
