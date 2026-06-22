package com.example;

import org.java_websocket.WebSocket;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;
import com.google.gson.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.concurrent.*;
import java.util.*;

public class PythonServerClient extends WebSocketClient {
    private static final Logger logger = LoggerFactory.getLogger(PythonServerClient.class);
    
    private final ConcurrentHashMap<String, CompletableFuture<JsonObject>> pendingTasks = 
        new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, RequestHandler> pendingRequests = 
        new ConcurrentHashMap<>();
    private final Gson gson = new Gson();
    private CompletableFuture<Void> connectionFuture = new CompletableFuture<>();
    
    public interface RequestHandler {
        JsonObject handle(JsonObject request);
    }
    
    public PythonServerClient(String serverUri) throws Exception {
        super(new URI(serverUri));
    }
    
    @Override
    public void onOpen(ServerHandshake handshakedata) {
        logger.info("Connected to Python server");
        connectionFuture.complete(null);
    }
    
    @Override
    public void onMessage(String message) {
        try {
            JsonObject json = JsonParser.parseString(message).getAsJsonObject();
            String type = json.get("type").getAsString();
            
            switch (type) {
                case "request":
                    handleServerRequest(json);
                    break;
                case "task_result":
                    handleTaskResult(json);
                    break;
                default:
                    logger.warn("Unknown message type: " + type);
            }
        } catch (Exception e) {
            logger.error("Error processing message: " + message, e);
        }
    }
    
    @Override
    public void onClose(int code, String reason, boolean remote) {
        logger.info("Disconnected from Python server: " + reason);
    }
    
    @Override
    public void onError(Exception ex) {
        logger.error("WebSocket error", ex);
    }
    
    /**
     * Wait for connection to be established
     */
    public void waitForConnection(long timeout, TimeUnit unit) throws Exception {
        connectionFuture.get(timeout, unit);
    }
    
    /**
     * Start a long-running task on the Python server
     */
    public CompletableFuture<JsonObject> startTask(String taskType, JsonObject params) 
            throws Exception {
        String taskId = UUID.randomUUID().toString();
        
        JsonObject request = new JsonObject();
        request.addProperty("type", "task");
        request.addProperty("task_id", taskId);
        request.addProperty("task_type", taskType);
        request.add("params", params);
        
        CompletableFuture<JsonObject> future = new CompletableFuture<>();
        pendingTasks.put(taskId, future);
        
        logger.info("Starting task: " + taskId + " (type: " + taskType + ")");
        this.send(request.toString());
        
        return future;
    }
    
    /**
     * Register a handler for specific request types from the Python server
     */
    public void registerRequestHandler(String action, RequestHandler handler) {
        pendingRequests.put(action, handler);
    }
    
    private void handleServerRequest(JsonObject json) {
        // Server is asking for information
        String requestId = json.get("request_id").getAsString();
        String action = json.get("action").getAsString();
        JsonObject data = json.getAsJsonObject("data");
        
        try {
            logger.info("Received request from server: " + requestId + " (action: " + action + ")");
            
            RequestHandler handler = pendingRequests.get(action);
            if (handler == null) {
                throw new Exception("No handler registered for action: " + action);
            }
            
            JsonObject responseData = handler.handle(data);
            
            JsonObject response = new JsonObject();
            response.addProperty("type", "response");
            response.addProperty("request_id", requestId);
            response.add("data", responseData);
            
            this.send(response.toString());
            logger.info("Sent response for request: " + requestId);
        } catch (Exception e) {
            logger.error("Error handling request: " + requestId, e);
            try {
                JsonObject error = new JsonObject();
                error.addProperty("type", "response");
                error.addProperty("request_id", requestId);
                error.addProperty("error", e.getMessage());
                this.send(error.toString());
            } catch (Exception sendError) {
                logger.error("Failed to send error response", sendError);
            }
        }
    }
    
    private void handleTaskResult(JsonObject json) {
        String taskId = json.get("task_id").getAsString();
        CompletableFuture<JsonObject> future = pendingTasks.remove(taskId);
        
        if (future != null) {
            String status = json.get("status").getAsString();
            
            if ("completed".equals(status)) {
                future.complete(json);
                logger.info("Task completed: " + taskId);
            } else if ("error".equals(status)) {
                String error = json.get("error").getAsString();
                future.completeExceptionally(new Exception(error));
                logger.error("Task failed: " + taskId + " - " + error);
            }
        } else {
            logger.warn("Received result for unknown task: " + taskId);
        }
    }
    
    public boolean isOpen() {
        return super.isOpen();
    }
}
