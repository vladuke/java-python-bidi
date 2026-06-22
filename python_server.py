import asyncio
import websockets
import json
from typing import Callable, Dict, Any
from concurrent.futures import ThreadPoolExecutor
import uuid
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TaskServer:
    def __init__(self, host='0.0.0.0', port=8765):
        self.host = host
        self.port = port
        self.clients = {}
        self.tasks = {}
        self.pending_responses = {}
        # Thread pool for blocking I/O operations
        self.executor = ThreadPoolExecutor(max_workers=10)
    
    async def handle_client(self, websocket, path):
        client_id = id(websocket)
        self.clients[client_id] = websocket
        logger.info(f"Client connected: {client_id}")
        
        try:
            async for message in websocket:
                data = json.loads(message)
                await self.process_request(client_id, data)
        except websockets.exceptions.ConnectionClosed:
            logger.info(f"Client disconnected: {client_id}")
        finally:
            del self.clients[client_id]
    
    async def process_request(self, client_id: str, request: Dict[str, Any]):
        """Process incoming request from Java client"""
        message_type = request.get('type')
        
        if message_type == 'task':
            task_id = request.get('task_id')
            task_type = request.get('task_type')
            params = request.get('params', {})
            
            # Execute long-running task in background
            task = asyncio.create_task(
                self.execute_task(client_id, task_id, task_type, params)
            )
            self.tasks[task_id] = task
        
        elif message_type == 'response':
            # Handle response from Java client
            await self.handle_java_response(client_id, request)
    
    async def execute_task(self, client_id, task_id, task_type, params):
        """Execute task - may call sync methods or request Java callbacks"""
        try:
            result = await self.run_long_task(task_type, params, client_id)
            
            await self.send_to_client(
                client_id,
                {'task_id': task_id, 'type': 'task_result', 'status': 'completed', 'result': result}
            )
        except Exception as e:
            logger.error(f"Task {task_id} failed: {e}", exc_info=True)
            await self.send_to_client(
                client_id,
                {'task_id': task_id, 'type': 'task_result', 'status': 'error', 'error': str(e)}
            )
    
    async def run_long_task(self, task_type: str, params: Dict, client_id):
        """
        Run long-running task.
        May invoke:
        1. Sync methods via executor (non-async code)
        2. Async methods (await)
        3. Java client callbacks (request/response pattern)
        """
        if task_type == 'local_sync_task':
            # Execute blocking/sync code in thread pool
            return await self.execute_sync_in_executor(params)
        
        elif task_type == 'local_async_task':
            # Execute async code
            return await self.execute_async_code(params)
        
        elif task_type == 'mixed_task':
            # Combination: sync, async, and Java callbacks
            return await self.execute_mixed_task(params, client_id)
        
        else:
            raise ValueError(f"Unknown task type: {task_type}")
    
    async def execute_sync_in_executor(self, params: Dict):
        """
        Run synchronous/blocking code without blocking event loop.
        Uses ThreadPoolExecutor to run blocking operations.
        """
        loop = asyncio.get_event_loop()
        
        # Run blocking function in thread pool
        result = await loop.run_in_executor(
            self.executor,
            self._blocking_operation,  # Sync method (no async)
            params.get('data')
        )
        
        return result
    
    @staticmethod
    def _blocking_operation(data):
        """
        Synchronous method - NO async declaration.
        Can call any blocking I/O, libraries, etc.
        """
        import time
        # Example: CPU-intensive or blocking I/O
        logger.info(f"Executing blocking operation with data: {data}")
        time.sleep(2)  # Simulate long operation
        return {'processed': data, 'status': 'success'}
    
    async def execute_async_code(self, params: Dict):
        """Execute async code"""
        logger.info(f"Executing async operation with params: {params}")
        await asyncio.sleep(1)
        return {'result': 'async operation completed', 'data': params.get('data')}
    
    async def execute_mixed_task(self, params: Dict, client_id):
        """
        Execute mix of:
        - Sync code (via executor)
        - Async code
        - Java callbacks (request/response)
        """
        # Step 1: Execute sync code
        loop = asyncio.get_event_loop()
        step1_result = await loop.run_in_executor(
            self.executor,
            self._step1_sync_processing,
            params.get('input')
        )
        logger.info(f"Step 1 (sync) result: {step1_result}")
        
        # Step 2: Call Java client for more information
        step2_result = await self.request_java_client(
            client_id,
            {
                'action': 'get_additional_data',
                'step1_output': step1_result
            }
        )
        logger.info(f"Step 2 (Java callback) result: {step2_result}")
        
        # Step 3: Execute more sync code with Java response
        step3_result = await loop.run_in_executor(
            self.executor,
            self._step3_final_processing,
            step1_result,
            step2_result
        )
        logger.info(f"Step 3 (sync) result: {step3_result}")
        
        return {
            'step1': step1_result,
            'step2': step2_result,
            'step3': step3_result,
            'final_status': 'completed'
        }
    
    @staticmethod
    def _step1_sync_processing(input_data):
        """Sync method - can use any libraries, blocking I/O, etc."""
        import time
        logger.info(f"Step 1: Processing {input_data}")
        time.sleep(1)
        return {'processed_by_python': input_data, 'timestamp': str(time.time())}
    
    @staticmethod
    def _step3_final_processing(step1_data, step2_data):
        """Another sync method using results from previous steps"""
        logger.info(f"Step 3: Combining results")
        combined = {
            'step1': step1_data,
            'step2_response': step2_data,
            'combined_result': f"{step1_data}|{step2_data}"
        }
        return combined
    
    async def request_java_client(self, client_id: str, request_params: Dict):
        """
        Request information from Java client.
        Sends message and waits for response.
        """
        request_id = str(uuid.uuid4())
        websocket = self.clients.get(client_id)
        
        if not websocket:
            raise Exception(f"Client {client_id} not connected")
        
        # Create future to wait for response
        response_future = asyncio.Future()
        self.pending_responses[request_id] = response_future
        
        # Send request to Java client
        await websocket.send(json.dumps({
            'type': 'request',
            'request_id': request_id,
            'action': request_params.get('action'),
            'data': request_params
        }))
        
        logger.info(f"Sent request to Java client: {request_id}")
        
        try:
            # Wait for Java client response (with timeout)
            result = await asyncio.wait_for(response_future, timeout=30.0)
            logger.info(f"Received response for request {request_id}")
            return result
        except asyncio.TimeoutError:
            del self.pending_responses[request_id]
            raise Exception(f"Java client response timeout for request {request_id}")
    
    async def handle_java_response(self, client_id, message: Dict):
        """Handle response from Java client"""
        request_id = message.get('request_id')
        response_future = self.pending_responses.pop(request_id, None)
        
        if response_future:
            response_future.set_result(message.get('data'))
            logger.info(f"Processed response for request {request_id}")
    
    async def send_to_client(self, client_id, message):
        """Send message to specific client"""
        websocket = self.clients.get(client_id)
        if websocket:
            try:
                await websocket.send(json.dumps(message))
            except Exception as e:
                logger.error(f"Failed to send to client {client_id}: {e}")
    
    async def broadcast(self, message):
        """Send message to all connected clients"""
        for client_id, websocket in self.clients.items():
            try:
                await websocket.send(json.dumps(message))
            except Exception as e:
                logger.error(f"Failed to broadcast to {client_id}: {e}")
    
    async def start(self):
        """Start the WebSocket server"""
        async with websockets.serve(self.handle_client, self.host, self.port):
            logger.info(f"WebSocket server started on ws://{self.host}:{self.port}")
            await asyncio.Future()  # run forever


async def main():
    server = TaskServer()
    await server.start()


if __name__ == "__main__":
    asyncio.run(main())
