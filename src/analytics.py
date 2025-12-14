#!/usr/bin/env python3
"""
[CHAPTER 4] MapReduce Log Analytics
Analyzes proxy access logs using MapReduce pattern

Input: access.log
Output: Statistics about domain access, cache hits, response times
"""

import os
import sys
from collections import defaultdict
from datetime import datetime

LOG_DIR = os.environ.get('LOG_DIR', './logs')
LOG_FILE = os.path.join(LOG_DIR, 'access.log')

# ============================================
# MAPREDUCE IMPLEMENTATION
# ============================================

def mapper(line):
    """
    MAP phase: Extract key-value pairs from log line
    """
    try:
        parts = line.strip().split('|')
        if len(parts) < 9:
            return []
        
        timestamp, client_ip, method, url, domain, status, worker, cached, response_time = parts
        
        results = [
            (('domain', domain), 1),
            (('cache', cached), 1),
            (('worker', worker), 1),
            (('status', status), 1),
            (('response_time', 'total'), float(response_time)),
            (('response_time', 'count'), 1),
            (('hour', timestamp[:13]), 1),  
        ]
        
        return results
        
    except Exception as e:
        return []

def reducer(key, values):
    """
    REDUCE phase: Aggregate values for each key
    """
    if key[0] == 'response_time' and key[1] == 'total':
        return sum(values)  # Sum for average calculation
    else:
        return sum(values)  # Count

def shuffle(mapped_data):
    """
    SHUFFLE phase: Group values by key
    """
    grouped = defaultdict(list)
    for key, value in mapped_data:
        grouped[key].append(value)
    return grouped

def run_mapreduce(log_file):
    """
    Execute MapReduce pipeline
    """
    print("=" * 60)
    print("[MapReduce] Starting Log Analysis")
    print("=" * 60)
    
    # Check if log file exists
    if not os.path.exists(log_file):
        print(f"[MapReduce] Log file not found: {log_file}")
        print("[MapReduce] Run some proxy requests first to generate logs.")
        return None
    
    # Read log file
    with open(log_file, 'r') as f:
        lines = f.readlines()
    
    print(f"[MapReduce] Processing {len(lines)} log entries...")
    
    # ========== MAP PHASE ==========
    print("\n[MAP] Parsing log entries...")
    mapped_data = []
    for line in lines:
        mapped_data.extend(mapper(line))
    print(f"[MAP] Generated {len(mapped_data)} key-value pairs")
    
    # ========== SHUFFLE PHASE ==========
    print("\n[SHUFFLE] Grouping by keys...")
    shuffled = shuffle(mapped_data)
    print(f"[SHUFFLE] Created {len(shuffled)} groups")
    
    # ========== REDUCE PHASE ==========
    print("\n[REDUCE] Aggregating values...")
    results = {}
    for key, values in shuffled.items():
        results[key] = reducer(key, values)
    
    return results

def print_report(results):
    """
    Print analytics report
    """
    if not results:
        return
    
    print("\n" + "=" * 60)
    print("ANALYTICS REPORT")
    print("=" * 60)
    
    # ========== TOP DOMAINS ==========
    print("\n📊 TOP 10 DOMAINS (Most Accessed):")
    print("-" * 40)
    domains = [(k[1], v) for k, v in results.items() if k[0] == 'domain']
    domains.sort(key=lambda x: x[1], reverse=True)
    
    for i, (domain, count) in enumerate(domains[:10], 1):
        bar = '█' * min(count * 2, 30)
        print(f"  {i:2}. {domain:30} {count:5} {bar}")
    
    # ========== CACHE STATISTICS ==========
    print("\n📦 CACHE STATISTICS:")
    print("-" * 40)
    cache_hits = results.get(('cache', 'True'), 0)
    cache_misses = results.get(('cache', 'False'), 0)
    total = cache_hits + cache_misses
    
    if total > 0:
        hit_rate = (cache_hits / total) * 100
        print(f"  Cache Hits:   {cache_hits:5} ({hit_rate:.1f}%)")
        print(f"  Cache Misses: {cache_misses:5} ({100-hit_rate:.1f}%)")
        print(f"  Total:        {total:5}")
        
        # Visual bar
        hit_bar = '🟢' * int(hit_rate / 10)
        miss_bar = '🔴' * int((100 - hit_rate) / 10)
        print(f"  [{hit_bar}{miss_bar}]")
    
    # ========== WORKER DISTRIBUTION ==========
    print("\n⚙️ WORKER DISTRIBUTION (Load Balancing):")
    print("-" * 40)
    workers = [(k[1], v) for k, v in results.items() if k[0] == 'worker']
    workers.sort(key=lambda x: x[1], reverse=True)
    
    for worker, count in workers:
        bar = '█' * min(count * 2, 30)
        print(f"  {worker:35} {count:5} {bar}")
    
    # ========== RESPONSE TIME ==========
    print("\n⏱️ RESPONSE TIME:")
    print("-" * 40)
    total_time = results.get(('response_time', 'total'), 0)
    count = results.get(('response_time', 'count'), 1)
    avg_time = total_time / count if count > 0 else 0
    
    print(f"  Average Response Time: {avg_time:.2f} ms")
    print(f"  Total Requests:        {count}")
    
    # ========== STATUS CODES ==========
    print("\n📈 HTTP STATUS CODES:")
    print("-" * 40)
    statuses = [(k[1], v) for k, v in results.items() if k[0] == 'status']
    statuses.sort(key=lambda x: x[0])
    
    for status, count in statuses:
        emoji = '✅' if status.startswith('2') else '⚠️' if status.startswith('3') else '❌'
        print(f"  {emoji} {status}: {count}")
    
    print("\n" + "=" * 60)

def export_csv(results, output_file='analytics_report.csv'):
    """Export results to CSV"""
    import csv
    
    with open(output_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['Category', 'Key', 'Value'])
        
        for (category, key), value in sorted(results.items()):
            writer.writerow([category, key, value])
    
    print(f"[MapReduce] Report exported to: {output_file}")

if __name__ == "__main__":
    # Allow custom log file path
    log_file = sys.argv[1] if len(sys.argv) > 1 else LOG_FILE
    
    print(f"[MapReduce] Log file: {log_file}")
    
    # Run MapReduce
    results = run_mapreduce(log_file)
    
    # Print report
    if results:
        print_report(results)
        
        # Export to CSV
        export_csv(results, 'logs/analytics_report.csv')

