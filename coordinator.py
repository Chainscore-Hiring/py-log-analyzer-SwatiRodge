import asyncio
import argparse
import os
from typing import Dict
from aiohttp import web


class Coordinator:
    """Manages workers and aggregates results"""
    
    def __init__(self, port: int):
        print(f"Starting coordinator on port {port}")
        self.workers = {}  # Map of worker ID to worker info
        self.results = {}  # Store results from each worker
        self.port = port

    def start(self) -> None:
        """Start coordinator server"""
        loop = asyncio.get_event_loop()
        loop.create_task(self.start_server())
        loop.run_forever()

    async def start_server(self):
        """Start an HTTP server to handle communication with workers"""
        app = web.Application()
        app.router.add_post('/assign_work', self.assign_work)
        app.router.add_post('/worker_health', self.report_worker_health)
        app.router.add_post('/send_results', self.receive_results)
        
        print(f"Coordinator listening on port {self.port}...")
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, 'localhost', self.port)
        await site.start()

    async def assign_work(self, request):
        """Handle requests from workers to assign log chunks"""
        data = await request.json()
        worker_id = data['worker_id']
        filepath = data['filepath']

        # Assign a chunk of the log file to the worker
        chunk_size = 1024 * 1024  # 1MB chunks (you can adjust as needed)
        file_size = os.path.getsize(filepath)
        start = 0
        size = min(chunk_size, file_size)
        
        # Send assigned chunk to the worker (for simplicity, just a response)
        self.workers[worker_id] = {'status': 'working', 'assigned_chunk': (start, size)}
        return web.json_response({"status": "assigned", "start": start, "size": size})

    async def report_worker_health(self, request):
        """Handle heartbeat from workers to check health"""
        data = await request.json()
        worker_id = data['worker_id']
        status = data['status']

        if status == 'healthy':
            self.workers[worker_id]['status'] = 'healthy'
        else:
            self.workers[worker_id]['status'] = 'failed'
        
        return web.json_response({"status": "received"})

    async def receive_results(self, request):
        """Receive results from workers and aggregate them"""
        data = await request.json()
        worker_id = data['worker_id']
        worker_results = data['results']

        # Store the results from the worker
        self.results[worker_id] = worker_results
        print(f"Received results from {worker_id}: {worker_results}")

        # Check if all workers are done
        if len(self.results) == len(self.workers):
            # Aggregate results
            aggregated_results = self.aggregate_results()
            print(f"All work completed. Aggregated results: {aggregated_results}")
        
        return web.json_response({"status": "received"})

    def aggregate_results(self) -> Dict:
        """Aggregate results from all workers"""
        aggregated = {
            "error_rate": 0,
            "avg_response_time": 0,
            "request_count": 0,
            "resource_usage": 0
        }

        for worker_results in self.results.values():
            for key, value in worker_results.items():
                aggregated[key] += value

        return aggregated


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Log Analyzer Coordinator")
    parser.add_argument("--port", type=int, default=8000, help="Coordinator port")
    args = parser.parse_args()

    coordinator = Coordinator(port=args.port)
    coordinator.start()
