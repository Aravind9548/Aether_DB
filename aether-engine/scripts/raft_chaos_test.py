#!/usr/bin/env python3
import subprocess
import time
import socket
import sys
import shutil
import os

# Clean up directories
for d in ['node1', 'node2', 'node3']:
    if os.path.exists(d):
        shutil.rmtree(d)
    os.makedirs(d)

# Start 3 nodes
print("Starting 3-node Raft cluster...")
p1 = subprocess.Popen(["./build/AetherDB", "--port", "8080", "--node-id", "1", "--peers", "2=127.0.0.1:8081,3=127.0.0.1:8082", "--data-dir", "node1"], stdout=subprocess.DEVNULL)
p2 = subprocess.Popen(["./build/AetherDB", "--port", "8081", "--node-id", "2", "--peers", "1=127.0.0.1:8080,3=127.0.0.1:8082", "--data-dir", "node2"], stdout=subprocess.DEVNULL)
p3 = subprocess.Popen(["./build/AetherDB", "--port", "8082", "--node-id", "3", "--peers", "1=127.0.0.1:8080,2=127.0.0.1:8081", "--data-dir", "node3"], stdout=subprocess.DEVNULL)

time.sleep(4) # Wait for leader election

def send_cmd(port, cmd):
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(2.0)
        s.connect(('127.0.0.1', port))
        s.sendall((cmd + '\n').encode())
        res = s.recv(1024).decode()
        s.close()
        return res
    except Exception as e:
        return f"ERR connection failed: {e}"

# Attempt to write to a node and handle redirection
def write_key_val(key, val):
    ports = [8080, 8081, 8082]
    res = ""
    for p in ports:
        res = send_cmd(p, f"SET {key} {val}")
        if "connection failed" in res:
            continue
        print(f"Write SET {key} {val} response from {p}: {res.strip()}")
        if "ERR NOT_LEADER" in res:
            parts = res.strip().split()
            if len(parts) == 3:
                leader_addr = parts[2]
                leader_port = int(leader_addr.split(':')[1])
                res = send_cmd(leader_port, f"SET {key} {val}")
                print(f"Redirected write to {leader_port}: {res.strip()}")
        break
    return res

# Read key
def read_key(key):
    ports = [8080, 8081, 8082]
    res = ""
    for p in ports:
        res = send_cmd(p, f"GET {key}")
        if "connection failed" in res:
            continue
        if "ERR NOT_LEADER" in res:
            parts = res.strip().split()
            if len(parts) == 3:
                leader_addr = parts[2]
                leader_port = int(leader_addr.split(':')[1])
                res = send_cmd(leader_port, f"GET {key}")
        break
    return res

try:
    # 1. Test basic replicated write
    print("\n--- Test Replicated Write ---")
    res = write_key_val("mykey", "myvalue")
    assert "OK" in res, f"Expected OK, got {res}"
    
    # Wait for replication & commit apply
    time.sleep(2)
    
    # Read from leader
    val = read_key("mykey")
    print(f"Read mykey: {val.strip()}")
    assert "myvalue" in val, f"Expected myvalue, got {val}"

    # 2. Kill the leader and test failover
    print("\n--- Test Leader Kill & Failover ---")
    leader_port = None
    for p in [8080, 8081, 8082]:
        res = send_cmd(p, "GET mykey")
        if "ERR NOT_LEADER" not in res:
            leader_port = p
            break
            
    if leader_port is None:
        print("Could not locate leader!")
        sys.exit(1)
        
    print(f"Current Leader is on port: {leader_port}")
    if leader_port == 8080:
        p1.terminate()
        p1.wait()
    elif leader_port == 8081:
        p2.terminate()
        p2.wait()
    else:
        p3.terminate()
        p3.wait()
        
    print("Leader killed. Waiting for failover election...")
    time.sleep(5) # Wait for new leader election

    # 3. Re-write to new leader
    print("\n--- Test Re-write after Failover ---")
    res = write_key_val("mykey2", "myvalue2")
    assert "OK" in res, f"Expected OK, got {res}"
    
    time.sleep(2)
    
    # Read mykey2
    val = read_key("mykey2")
    print(f"Read mykey2: {val.strip()}")
    assert "myvalue2" in val, f"Expected myvalue2, got {val}"

    print("\nALL RAFT INTEGRATION TESTS PASSED SUCCESSFULLY!")

finally:
    print("Terminating cluster processes...")
    p1.terminate()
    p2.terminate()
    p3.terminate()
    p1.wait()
    p2.wait()
    p3.wait()
    # Clean up data directories
    for d in ['node1', 'node2', 'node3']:
        if os.path.exists(d):
            shutil.rmtree(d)
