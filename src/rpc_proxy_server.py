#!/usr/bin/env python3
"""
HTTP Proxy Server using RPC
[UPGRADED] With Access Logging for MapReduce Analysis

- Scalability: Load balances across multiple RPC workers
- Consistency: Uses shared file cache (DFS)
- Analytics: Logs all requests for MapReduce processing
"""

from http.server import HTTPServer, BaseHTTPRequestHandler
import xmlrpc.client
import threading
import datetime
import os
import sys

# ============================================
# CONFIGURATION
# ============================================
# List of RPC workers (scalability)
WORKERS = os.environ.get('WORKERS', 'http://localhost:8001,http://localhost:8002,http://localhost:8003').split(',')

# Log directory for MapReduce
LOG_DIR = os.environ.get('LOG_DIR', './logs')
LOG_FILE = os.path.join(LOG_DIR, 'access.log')

# Ensure log directory exists
os.makedirs(LOG_DIR, exist_ok=True)

# Track active workers
active_workers = []
worker_lock = threading.Lock()
log_lock = threading.Lock()

class LoadBalancer:
    """Simple round-robin load balancer"""
    def __init__(self):
        self.index = 0
        self.lock = threading.Lock()
    
    def get_worker(self):
        with self.lock:
            if not active_workers:
                return None
            worker = active_workers[self.index % len(active_workers)]
            self.index += 1
            return worker

load_balancer = LoadBalancer()

def check_workers():
    """Check which workers are alive"""
    global active_workers
    alive = []
    
    for worker_url in WORKERS:
        worker_url = worker_url.strip()
        if not worker_url:
            continue
        try:
            proxy = xmlrpc.client.ServerProxy(worker_url)
            result = proxy.health_check()
            if result['status'] == 'ok':
                alive.append(worker_url)
                print(f"[Proxy] Worker OK: {worker_url} (Cache: {result.get('cache_entries', 0)} entries)")
        except Exception as e:
            print(f"[Proxy] Worker DOWN: {worker_url}")
    
    with worker_lock:
        active_workers = alive
    
    return len(alive)

# ============================================
#  MAPREDUCE - Access Logging
# ============================================
def log_access(client_ip, method, url, status, worker, cached, response_time):
    """
    Log access for MapReduce analysis
    Format: timestamp|client_ip|method|url|domain|status|worker|cached|response_time_ms
    """
    try:
        # Extract domain from URL
        from urllib.parse import urlparse
        parsed = urlparse(url)
        domain = parsed.netloc or 'unknown'
        
        timestamp = datetime.datetime.now().isoformat()
        
        log_line = f"{timestamp}|{client_ip}|{method}|{url}|{domain}|{status}|{worker}|{cached}|{response_time:.2f}\n"
        
        with log_lock:
            with open(LOG_FILE, 'a') as f:
                f.write(log_line)
                
    except Exception as e:
        print(f"[Proxy] Log error: {e}")

class ProxyHandler(BaseHTTPRequestHandler):
    """HTTP Request Handler - forwards requests via RPC"""
    
    def do_GET(self):
        import time
        start_time = time.time()
        
        # Build target URL
        if self.path.startswith("http"):
            url = self.path
        else:
            url = f"http://example.com{self.path}"
        
        # Get client IP
        client_ip = self.client_address[0]
        
        # Get worker from load balancer
        worker_url = load_balancer.get_worker()
        
        if not worker_url:
            self.send_error(503, "No RPC workers available")
            log_access(client_ip, "GET", url, 503, "none", False, 0)
            return
        
        try:
            # Make RPC call to worker
            proxy = xmlrpc.client.ServerProxy(worker_url)
            result = proxy.fetch_url(url)
            
            response_time = (time.time() - start_time) * 1000  # ms
            
            # Send response
            self.send_response(result['status'])
            
            # Forward headers
            for key, value in result.get('headers', {}).items():
                if key.lower() not in ['transfer-encoding', 'connection']:
                    self.send_header(key, value)
            
            # Add proxy headers
            self.send_header('X-Proxy', 'RPC-Proxy')
            self.send_header('X-Worker', result.get('worker', worker_url))
            self.send_header('X-Cached', str(result.get('cached', False)))
            self.send_header('X-Response-Time', f"{response_time:.2f}ms")
            self.end_headers()
            
            # Send content
            content = result['content']
            if hasattr(content, 'data'):
                self.wfile.write(content.data)
            else:
                self.wfile.write(content)
            
            # Log for MapReduce
            log_access(
                client_ip, 
                "GET", 
                url, 
                result['status'], 
                result.get('worker', worker_url),
                result.get('cached', False),
                response_time
            )
            
            # Console log
            cached = "CACHED" if result.get('cached') else "FRESH"
            print(f"[Proxy] GET {url} -> {result['status']} [{cached}] {response_time:.0f}ms via {result.get('worker', 'unknown')}")
            
        except Exception as e:
            response_time = (time.time() - start_time) * 1000
            self.send_error(502, f"RPC Error: {str(e)}")
            log_access(client_ip, "GET", url, 502, worker_url, False, response_time)
    
    def do_CONNECT(self):
        """Handle HTTPS CONNECT (tunnel)"""
        self.send_error(501, "HTTPS tunneling not implemented")
    
    def log_message(self, format, *args):
        pass  # Suppress default logging (we have our own)

class ThreadedHTTPServer(HTTPServer):
    """Handle requests in threads for scalability"""
    def process_request(self, request, client_address):
        thread = threading.Thread(target=self.process_request_thread, 
                                  args=(request, client_address))
        thread.daemon = True
        thread.start()
    
    def process_request_thread(self, request, client_address):
        try:
            self.finish_request(request, client_address)
        except:
            self.handle_error(request, client_address)
        finally:
            self.shutdown_request(request)

def worker_health_monitor():
    """Background thread to monitor worker health"""
    import time
    while True:
        check_workers()
        time.sleep(10)

if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8080
    
    # For local testing
    if not os.path.exists(LOG_DIR):
        LOG_DIR = './logs'
        LOG_FILE = os.path.join(LOG_DIR, 'access.log')
        os.makedirs(LOG_DIR, exist_ok=True)
    
    print("=" * 60)
    print("HTTP PROXY SERVER (RPC-based)")
    print("=" * 60)
    print(f"[Proxy] Starting on port {port}")
    print(f"[Proxy] Workers: {WORKERS}")
    print(f"[Proxy] Log File: {LOG_FILE}")
    
    # Initial worker check
    alive = check_workers()
    print(f"[Proxy] Active workers: {alive}/{len(WORKERS)}")
    
    if alive == 0:
        print("[Proxy] WARNING: No workers available!")
        print("[Proxy] Start workers with: python rpc_worker.py 8001")
    
    # Start health monitor thread
    monitor = threading.Thread(target=worker_health_monitor, daemon=True)
    monitor.start()
    
    # Start proxy server
    server = ThreadedHTTPServer(("0.0.0.0", port), ProxyHandler)
    print(f"\n[Proxy] Ready! Use: curl -x http://localhost:{port} http://example.com")
    print("[Proxy] Press Ctrl+C to stop\n")
    
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n[Proxy] Shutting down...")
        server.shutdown()
