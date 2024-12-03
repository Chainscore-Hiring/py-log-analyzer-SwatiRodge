import asyncio
import argparse
import aiofiles
import aiohttp
from datetime import datetime
from typing import Dict

class Worker:
    """Processes log chunks and reports results"""
    
    def __init__(self, port: int, worker_id: str, coordinator_url: str):
        self.worker_id = worker_id                             
        self.coordinator_url = coordinator_url
        self.port = port
    
    def start(self) -> None:
        """Start worker server and heartbeat task"""
        loop = asyncio.get_event_loop()
        loop.create_task(self.start_server())
        loop.create_task(self.report_health())
        loop.run_forever()

    async def start_server(self):
        """Start a basic server that listens on the worker's port"""
        print(f"Starting worker {self.worker_id} on port {self.port}...")
        server = await asyncio.start_server(self.handle_client, 'localhost', self.port)
        addr = server.sockets[0].getsockname()
        print(f"Serving on {addr}")
        await server.serve_forever()

    async def handle_client(self, reader, writer):
        """Handle incoming communication from the coordinator"""
        data = await reader.read(100)
        message = data.decode()
        addr = writer.get_extra_info('peername')
        print(f"Received {message} from {addr}")
        writer.close()
        await writer.wait_closed()

    async def process_chunk(self, filepath: str, start: int, size: int) -> Dict:
        """Process a chunk of log file and return metrics"""
        metrics = {
            "error_rate": 0,
            "avg_response_time": 0,
            "request_count": 0,
            "resource_usage": 0
        }
        
        # Reading the log chunk asynchronously
        async with aiofiles.open(filepath, mode='r') as file:
            file.seek(start)
            chunk = await file.read(size)
            lines = chunk.splitlines()
            
            for line in lines:
                log_entry = self.parse_log_line(line)
                self.update_metrics(metrics, log_entry)

        return metrics

    def parse_log_line(self, line: str) -> Dict:
        """Parse a single log line into timestamp, level, and message"""
        timestamp_str, level, message = line.split(" ", 2)
        timestamp = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S.%f")
        return {"timestamp": timestamp, "level": level, "message": message}
    
    def update_metrics(self, metrics: Dict, log_entry: Dict) -> None:
        """Update metrics based on the log entry"""
        if log_entry["level"] == "ERROR":
            metrics["error_rate"] += 1
        if "ms" in log_entry["message"]:
            response_time = int(log_entry["message"].split(" in ")[1][:-2])
            metrics["avg_response_time"] += response_time
        if "Request" in log_entry["message"]:
            metrics["request_count"] += 1

    async def report_health(self) -> None:
        """Send heartbeat to coordinator"""
        while True:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.post(self.coordinator_url, json={"worker_id": self.worker_id, "status": "healthy"}) as response:
                        print(f"Heartbeat sent from {self.worker_id}. Response: {response.status}")
            except Exception as e:
                print(f"Failed to send heartbeat: {e}")
            await asyncio.sleep(5)  # Send heartbeat every 5 seconds

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Log Analyzer Worker")
    parser.add_argument("--port", type=int, default=8001, help="Worker port")
    parser.add_argument("--id", type=str, default="worker1", help="Worker ID")
    parser.add_argument("--coordinator", type=str, default="http://localhost:8000", help="Coordinator URL")
    args = parser.parse_args()

    worker = Worker(port=args.port, worker_id=args.id, coordinator_url=args.coordinator)
    worker.start()
