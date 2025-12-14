#!/usr/bin/env python3
"""
RPC Worker - Fetches HTTP content on behalf of proxy
[UPGRADED] Uses File-based Cache (Distributed File System simulation)

Scalable: Can run multiple workers on different ports
Consistent: Shared cache directory accessible by all workers
"""

from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.client import Binary
import urllib.request
import urllib.error
import hashlib
import threading
import json
import sys
import time
import os

# ============================================
# DISTRIBUTED FILE SYSTEM - Shared Cache
# ============================================
CACHE_DIR = os.environ.get('CACHE_DIR', './cache_data')
CACHE_TTL = 60  # seconds

# Ensure cache directory exists
os.makedirs(CACHE_DIR, exist_ok=True)

cache_lock = threading.Lock()

def get_cache_key(url):
    """Generate cache filename from URL hash"""
    return hashlib.md5(url.encode()).hexdigest()

def get_cache_path(cache_key):
    """Get full path to cache file"""
    return os.path.join(CACHE_DIR, f"{cache_key}.json")

def read_from_cache(url):
    """
    [DFS] Read cache from shared file system
    All workers can access this cache
    """
    cache_key = get_cache_key(url)
    cache_path = get_cache_path(cache_key)
    
    try:
        if os.path.exists(cache_path):
            # Check TTL
            file_age = time.time() - os.path.getmtime(cache_path)
            if file_age < CACHE_TTL:
                with open(cache_path, 'r') as f:
                    data = json.load(f)
                # Convert content back to Binary
                data['content'] = Binary(bytes.fromhex(data['content_hex']))
                return data
            else:
                # Cache expired, remove file
                os.remove(cache_path)
    except Exception as e:
        print(f"[Worker] Cache read error: {e}")
    
    return None

def write_to_cache(url, data):
    """
    [DFS] Write cache to shared file system
    Other workers can read this cache
    """
    cache_key = get_cache_key(url)
    cache_path = get_cache_path(cache_key)
    
    try:
        # Convert Binary content to hex string for JSON
        cache_data = {
            'url': url,
            'status': data['status'],
            'headers': data['headers'],
            'content_hex': data['content'].data.hex(),
            'timestamp': time.time()
        }
        
        with cache_lock:
            with open(cache_path, 'w') as f:
                json.dump(cache_data, f)
        
        print(f"[Worker] Cached to DFS: {cache_key[:8]}...")
    except Exception as e:
        print(f"[Worker] Cache write error: {e}")

def fetch_url(url):
    """
    Fetch URL content via RPC
    [DFS] Uses shared file cache for consistency across workers
    """
    worker_id = os.environ.get('WORKER_ID', 'local')
    
    # Check shared cache first (DFS - all workers can access)
    cached = read_from_cache(url)
    if cached:
        print(f"[Worker {worker_id}] Cache HIT (DFS): {url}")
        return {
            'status': cached['status'],
            'headers': cached['headers'],
            'content': cached['content'],
            'cached': True,
            'worker': worker_id
        }
    
    # Fetch from internet
    print(f"[Worker {worker_id}] Fetching: {url}")
    try:
        req = urllib.request.Request(url, headers={
            'User-Agent': 'Mozilla/5.0 (RPC Proxy Worker)'
        })
        
        with urllib.request.urlopen(req, timeout=10) as response:
            content = response.read()
            status = response.status
            headers = dict(response.headers)
            
            result = {
                'status': status,
                'headers': headers,
                'content': Binary(content),
                'cached': False,
                'worker': worker_id
            }
            
            # Write to shared cache (DFS)
            write_to_cache(url, result)
            
            return result
            
    except urllib.error.HTTPError as e:
        return {
            'status': e.code,
            'headers': {},
            'content': Binary(str(e).encode()),
            'cached': False,
            'error': str(e),
            'worker': worker_id
        }
    except Exception as e:
        return {
            'status': 500,
            'headers': {},
            'content': Binary(str(e).encode()),
            'cached': False,
            'error': str(e),
            'worker': worker_id
        }

def health_check():
    """Health check for load balancer"""
    worker_id = os.environ.get('WORKER_ID', 'local')
    
    # Count cache files
    cache_files = [f for f in os.listdir(CACHE_DIR) if f.endswith('.json')]
    
    return {
        'status': 'ok',
        'worker_id': worker_id,
        'cache_type': 'DFS (File-based)',
        'cache_dir': CACHE_DIR,
        'cache_entries': len(cache_files),
        'timestamp': time.time()
    }

def clear_cache():
    """Clear shared cache (DFS)"""
    with cache_lock:
        for f in os.listdir(CACHE_DIR):
            if f.endswith('.json'):
                os.remove(os.path.join(CACHE_DIR, f))
    return True

def get_stats():
    """Get worker statistics"""
    cache_files = [f for f in os.listdir(CACHE_DIR) if f.endswith('.json')]
    
    return {
        'worker_id': os.environ.get('WORKER_ID', 'local'),
        'cache_type': 'DFS',
        'cache_dir': CACHE_DIR,
        'cache_entries': len(cache_files),
        'cache_files': cache_files[:10]  # First 10
    }

if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8001
    worker_id = os.environ.get('WORKER_ID', f'worker-{port}')
    
    # For local testing, use local cache dir
    if not os.path.exists(CACHE_DIR):
        CACHE_DIR = './cache_data'
        os.makedirs(CACHE_DIR, exist_ok=True)
    
    server = SimpleXMLRPCServer(("0.0.0.0", port), allow_none=True)
    
    server.register_function(fetch_url, "fetch_url")
    server.register_function(health_check, "health_check")
    server.register_function(clear_cache, "clear_cache")
    server.register_function(get_stats, "get_stats")
    
    print(f"[RPC Worker {worker_id}] Running on port {port}")
    print(f"[RPC Worker {worker_id}] Cache Dir (DFS): {CACHE_DIR}")
    print("[RPC Worker] Functions: fetch_url, health_check, clear_cache, get_stats")
    
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print(f"\n[RPC Worker {worker_id}] Shutting down...")
