# Java-Python Bidirectional Communication via WebSocket

A framework for bidirectional communication between Java clients and a Python server using WebSockets, with support for long-running tasks, synchronous method execution, and request-response patterns.

## Architecture

```
┌──────────────────────────┐           WebSocket           ┌─────────────────────────────────┐
│   Java Client            │◄──────────────────────────────►│  Python Server                  │
│ (Java-WebSocket lib)     │                                │  (AsyncIO + ThreadPool)         │
└──────────────────────────┘                                └─────────────────────────────────┘
     │                                                              │
     │                                                              ├─ Sync methods (ThreadPool)
     │                                                              │
     │◄─────────── Request: get_additional_data ─────────────────┤
     │                                                              │
     ├──────────── Response: {...data...} ──────────────────────►│
     │                                                              │
```

## Key Features

### Python Server
- **Async WebSocket Server**: Built with `websockets` library
- **Mixed Execution Model**: 
  - Async methods for I/O operations
  - Sync methods (without `async` declaration) executed in ThreadPoolExecutor
  - Automatic context switching between async and sync code
- **Bidirectional Communication**:
  - Server can send tasks to clients
  - Clients can respond with data
  - Server can request additional information during task execution
- **Long-Running Task Support**: Tasks execute asynchronously without blocking the event loop

### Java Client
- **Standalone WebSocket Client**: Uses Java-WebSocket library (no external container needed)
- **Async Task Management**: Uses `CompletableFuture` for non-blocking operations
- **Request Handler Registration**: Register custom handlers for server requests
- **Type-Safe JSON**: Uses Gson for JSON serialization

## Installation

### Python
```bash
pip install -r requirements.txt
```

### Java
```bash
cd java
# Using Gradle
gradle build

# Or using Maven (if you prefer pom.xml)
mvn clean package
```

## Usage

### Starting the Python Server

```bash
python python_server.py
```

Server listens on `ws://0.0.0.0:8765`

### Running the Java Client

```bash
# Using Gradle
cd java
gradle run

# Or with JAR
gradle fatJar
java -jar build/libs/java-python-bidi-fat.jar
```

## Task Types

### 1. Local Sync Task
Executes synchronous Python code without blocking the event loop:

```python
async def execute_sync_in_executor(self, params: Dict):
    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(
        self.executor,
        self._blocking_operation,  # NO async required
        params.get('data')
    )
    return result
```

**Java Usage:**
```java
JsonObject params = new JsonObject();
params.addProperty("data", "test input");

var result = client.startTask("local_sync_task", params)
    .get(60, TimeUnit.SECONDS);
```

### 2. Local Async Task
Executes async Python code:

```python
async def execute_async_code(self, params: Dict):
    await asyncio.sleep(1)
    return {'result': 'completed', 'data': params.get('data')}
```

### 3. Mixed Task
Combines sync operations, async operations, and Java client callbacks:

```python
async def execute_mixed_task(self, params: Dict, client_id):
    # Step 1: Execute sync code
    step1 = await loop.run_in_executor(self.executor, self._step1_sync_processing, ...)
    
    # Step 2: Request data from Java
    step2 = await self.request_java_client(client_id, {'action': 'get_additional_data', ...})
    
    # Step 3: More sync code with Java response
    step3 = await loop.run_in_executor(self.executor, self._step3_final_processing, ...)
    
    return {'step1': step1, 'step2': step2, 'step3': step3}
```

## Protocol

### Task Request (Java → Python)
```json
{
  "type": "task",
  "task_id": "uuid",
  "task_type": "local_sync_task",
  "params": {
    "data": "input data"
  }
}
```

### Task Result (Python → Java)
```json
{
  "task_id": "uuid",
  "type": "task_result",
  "status": "completed",
  "result": { ... }
}
```

### Server Request (Python → Java)
```json
{
  "type": "request",
  "request_id": "uuid",
  "action": "get_additional_data",
  "data": { ... }
}
```

### Client Response (Java → Python)
```json
{
  "type": "response",
  "request_id": "uuid",
  "data": { ... }
}
```

## Extending with Custom Tasks

### Adding a Custom Task on Python

```python
async def run_long_task(self, task_type: str, params: Dict, client_id):
    # ... existing task types ...
    elif task_type == 'custom_task':
        return await self.execute_custom_task(params, client_id)

async def execute_custom_task(self, params: Dict, client_id):
    # Your custom logic here
    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(
        self.executor,
        self._custom_sync_operation,
        params
    )
    return result

@staticmethod
def _custom_sync_operation(params):
    # Blocking code - can use any library
    import expensive_library
    return expensive_library.process(params)
```

### Calling Custom Task from Java

```java
JsonObject params = new JsonObject();
params.addProperty("key", "value");

var result = client.startTask("custom_task", params)
    .get(60, TimeUnit.SECONDS);
```

## Registering Request Handlers on Java

```java
client.registerRequestHandler("get_data", request -> {
    JsonObject response = new JsonObject();
    response.addProperty("status", "ready");
    response.addProperty("data", processRequest(request));
    return response;
});
```

## Error Handling

### Python Server
```python
try:
    result = await self.run_long_task(task_type, params, client_id)
except Exception as e:
    await self.send_to_client(
        client_id,
        {'task_id': task_id, 'type': 'task_result', 'status': 'error', 'error': str(e)}
    )
```

### Java Client
```java
var result = client.startTask("task_type", params)
    .exceptionally(ex -> {
        logger.error("Task failed: " + ex.getMessage());
        return null;
    })
    .get(60, TimeUnit.SECONDS);
```

## Testing

### Terminal 1: Start Python Server
```bash
python python_server.py
```

### Terminal 2: Run Java Client
```bash
cd java
gradle run
```

Expected output shows all three task types completing successfully with data flowing bidirectionally.

## Project Structure

```
.
├── python_server.py              # Main Python server
├── requirements.txt              # Python dependencies
├── java/
│   ├── build.gradle             # Gradle build config (no external container)
│   ├── src/main/java/com/example/
│   │   ├── PythonServerClient.java    # Java WebSocket client
│   │   └── Example.java              # Usage example
│   └── build/                   # Build artifacts
└── README.md                    # This file
```

## Architecture Benefits

1. **Non-blocking I/O**: Python asyncio doesn't block on sync operations
2. **Flexible Execution**: Mix async, sync, and callback-based code seamlessly
3. **Resource Efficient**: ThreadPoolExecutor handles blocking operations
4. **Type-Safe**: Java strong typing with Gson JSON support
5. **Bidirectional**: Either side can initiate requests
6. **Timeout Protection**: Configurable timeouts prevent hanging
7. **Standalone Java**: No application server or container needed

## Dependencies

### Python
- `websockets` 12.0+ - WebSocket server

### Java
- `Java-WebSocket` 1.5.4 - WebSocket client (no external container)
- `Gson` 2.10.1 - JSON serialization
- `SLF4J` 2.0.5 - Logging API
- `Logback` 1.4.6 - Logging implementation

## Build Options

### Gradle (Recommended)
```bash
cd java
gradle build                    # Build JAR
gradle run                      # Run directly
gradle fatJar                   # Create fat JAR with all dependencies
```

### Maven (Legacy)
If pom.xml is available:
```bash
cd java
mvn clean package
mvn exec:java -Dexec.mainClass="com.example.Example"
```

## License

MIT
