import socket
import threading
import time
import subprocess
import os
import signal
import sys

BINARY = "./build/AetherDB"
HOST = "127.0.0.1"
PORT = 8080
NUM_THREADS = 100
OPS_PER_THREAD = 100

def cleanup():
    for f in ["aether.wal", "aether.snapshot", "aether.snapshot.tmp"]:
        if os.path.exists(f):
            try:
                os.remove(f)
            except Exception:
                pass

def worker(thread_id):
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((HOST, PORT))
        for op in range(OPS_PER_THREAD):
            key = f"key_{thread_id}_{op}"
            val = f"val_{thread_id}_{op}"
            cmd = f"SET {key} {val}\n"
            s.sendall(cmd.encode())
            resp = s.recv(1024).decode()
            if resp != "OK\n":
                print(f"Error on thread {thread_id} op {op}: {resp}")
        s.close()
    except Exception as e:
        print(f"Thread {thread_id} failed: {e}")

def run_benchmark(shards):
    cleanup()
    print(f"\n--- Launching AetherDB with {shards} shard(s) ---")
    proc = subprocess.Popen([BINARY, "--shards", str(shards)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(1.5) # Allow server to bind and load
    
    threads = []
    start = time.time()
    for i in range(NUM_THREADS):
        t = threading.Thread(target=worker, args=(i,))
        threads.append(t)
        t.start()
        
    for t in threads:
        t.join()
        
    elapsed = time.time() - start
    total_ops = NUM_THREADS * OPS_PER_THREAD
    throughput = total_ops / elapsed
    
    print(f"Elapsed: {elapsed:.4f} seconds")
    print(f"Total Ops: {total_ops}")
    print(f"Throughput: {throughput:.2f} ops/sec")
    
    # Graceful shutdown
    proc.send_signal(signal.SIGINT)
    proc.wait()
    time.sleep(0.5)
    return throughput

def main():
    if not os.path.exists(BINARY):
        print(f"Error: Binary {BINARY} not found. Please compile the project first.")
        sys.exit(1)
        
    print(f"Running AetherDB Benchmarks ({NUM_THREADS} threads, {OPS_PER_THREAD} ops/thread)...")
    
    # Run Round 1: Single global mutex (1 shard)
    tp_global = run_benchmark(1)
    
    # Run Round 2: Sharded map (16 shards)
    tp_sharded = run_benchmark(16)
    
    print("\n================ BENCHMARK RESULT COMPARISON ================")
    print(f"Global Mutex (1 shard):   {tp_global:.2f} ops/sec")
    print(f"Sharded Locks (16 shards): {tp_sharded:.2f} ops/sec")
    speedup = (tp_sharded / tp_global) if tp_global > 0 else 0
    print(f"Performance Speedup:      {speedup:.2f}x")
    print("=============================================================")

if __name__ == "__main__":
    main()
