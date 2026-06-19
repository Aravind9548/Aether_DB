#!/bin/bash
set -e

echo "=== AetherDB Durability and Crash Recovery Demo ==="

# Ensure we are in the project root containing build directory
cd "$(dirname "$0")/.."

# Clean up files from previous runs
rm -f aether.wal aether.snapshot aether.snapshot.tmp

# Start AetherDB in background
echo "Starting AetherDB server..."
./build/AetherDB &
SERVER_PID=$!
sleep 1 # Wait for server to bind

# Connect and write some keys using netcat
echo "Writing initial keys..."
echo -e "SET key1 value1\nSET key2 value2" | nc -w 1 localhost 8080

# Check they exist
echo "Reading key1..."
echo "GET key1" | nc -w 1 localhost 8080

# Now crash the server using kill -9
echo "Simulating sudden server crash (kill -9)..."
kill -9 $SERVER_PID
sleep 1

# Start the server again to verify recovery
echo "Restarting AetherDB server..."
./build/AetherDB &
SERVER_PID2=$!
sleep 1

# Check if keys are recovered
echo "Querying recovered key1 and key2..."
echo "GET key1" | nc -w 1 localhost 8080
echo "GET key2" | nc -w 1 localhost 8080

# Shut down gracefully
echo "Stopping server gracefully..."
kill -SIGINT $SERVER_PID2
wait $SERVER_PID2 2>/dev/null || true

echo "Demo complete."
