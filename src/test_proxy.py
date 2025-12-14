#!/usr/bin/env python3
"""
Test client for HTTP RPC Proxy
Tests: Scalability, Consistency, Cache
"""

import urllib.request
import time
import sys

PROXY_URL = "http://localhost:8080"

def test_proxy(url):
    """Test fetching URL through proxy"""
    print(f"\n[Test] Fetching: {url}")
    
    proxy_handler = urllib.request.ProxyHandler({'http': PROXY_URL})
    opener = urllib.request.build_opener(proxy_handler)
    
    try:
        start = time.time()
        response = opener.open(url, timeout=10)
        elapsed = time.time() - start
        
        print(f"[Test] Status: {response.status}")
        print(f"[Test] Time: {elapsed:.3f}s")
        print(f"[Test] X-Cached: {response.headers.get('X-Cached', 'N/A')}")
        print(f"[Test] X-Worker: {response.headers.get('X-Worker', 'N/A')}")
        print(f"[Test] Content-Length: {len(response.read())} bytes")
        return True
        
    except Exception as e:
        print(f"[Test] Error: {e}")
        return False

def test_cache_consistency():
    """Test DFS cache consistency across workers"""
    url = "http://example.com"
    
    print("\n" + "=" * 50)
    print("TEST 1: CACHE CONSISTENCY (DFS)")
    print("=" * 50)
    print("All workers share the same cache directory.")
    print("First request = FRESH, subsequent = CACHED")
    
    for i in range(1, 4):
        print(f"\n--- Request {i} ---")
        test_proxy(url)
        time.sleep(0.3)

def test_load_balancing():
    """Test load balancing across workers"""
    print("\n" + "=" * 50)
    print("TEST 2: LOAD BALANCING (Scalability)")
    print("=" * 50)
    print("Requests should be distributed across workers.")
    
    urls = [
        "http://httpbin.org/ip",
        "http://httpbin.org/headers", 
        "http://httpbin.org/user-agent",
        "http://httpbin.org/get",
    ]
    
    for url in urls:
        test_proxy(url)
        time.sleep(0.5)

def test_fault_tolerance():
    """Test fault tolerance"""
    print("\n" + "=" * 50)
    print("TEST 3: FAULT TOLERANCE")
    print("=" * 50)
    print("Stop one worker and test if proxy still works.")
    print("(Manual test - stop a worker container)")
    
    test_proxy("http://example.com")

if __name__ == "__main__":
    print("=" * 50)
    print("HTTP RPC PROXY - TEST SUITE")
    print("=" * 50)
    print(f"Proxy: {PROXY_URL}")
    
    if len(sys.argv) > 1:
        test_proxy(sys.argv[1])
    else:
        test_cache_consistency()
        test_load_balancing()
        test_fault_tolerance()
    
    print("\n[Test] All tests completed!")
    print("[Test] Run 'docker-compose --profile tools run analytics' for MapReduce report")
