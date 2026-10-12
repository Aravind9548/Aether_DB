// Linearizability checker for histories recorded by scripts/partition_test.py.
//
// Uses Porcupine (github.com/anishathalye/porcupine), the checker used by
// MIT 6.5840's Raft labs and by etcd's robustness tests. The model is a
// key-value store of independent registers, partitioned per key so each
// key's history is checked on its own.
//
// Usage: go run . <history.json> <visualization.html>
// Exit code: 0 = linearizable, 1 = NOT linearizable, 2 = checker timed out / error.
package main

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"runtime"
	"sort"
	"time"

	"github.com/anishathalye/porcupine"
)

// One client operation as recorded by the Python harness. Status "ok" means
// the client got a definite answer. "info" means the outcome is unknown
// (timeout, connection dropped, leader changed mid-commit): the write may or
// may not have happened, so it is given an open-ended return time and
// Porcupine is free to linearize it anywhere after its call, or never observe it.
type recordedOp struct {
	Client int    `json:"client"`
	Kind   string `json:"kind"` // "put" | "get"
	Key    string `json:"key"`
	Value  string `json:"value"` // put: value written; get: value read ("" = absent)
	Call   int64  `json:"call"`
	Return *int64 `json:"ret"`
	Status string `json:"status"`
	Node   int    `json:"node"`
}

type kvInput struct {
	Kind  string
	Key   string
	Value string
}

type kvOutput struct {
	Value   string
	Unknown bool
}

var kvModel = porcupine.Model{
	Partition: func(history []porcupine.Operation) [][]porcupine.Operation {
		byKey := map[string][]porcupine.Operation{}
		for _, op := range history {
			k := op.Input.(kvInput).Key
			byKey[k] = append(byKey[k], op)
		}
		keys := make([]string, 0, len(byKey))
		for k := range byKey {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		out := make([][]porcupine.Operation, 0, len(keys))
		for _, k := range keys {
			out = append(out, byKey[k])
		}
		return out
	},
	Init: func() interface{} { return "" },
	Step: func(state, input, output interface{}) (bool, interface{}) {
		in := input.(kvInput)
		out := output.(kvOutput)
		st := state.(string)
		switch in.Kind {
		case "put":
			return true, in.Value
		case "get":
			return out.Value == st, st
		}
		return false, st
	},
	Equal: func(a, b interface{}) bool { return a.(string) == b.(string) },
	DescribeOperation: func(input, output interface{}) string {
		in := input.(kvInput)
		out := output.(kvOutput)
		switch in.Kind {
		case "put":
			if out.Unknown {
				return fmt.Sprintf("put(%q) -> unknown", in.Value)
			}
			return fmt.Sprintf("put(%q)", in.Value)
		default:
			return fmt.Sprintf("get() -> %q", out.Value)
		}
	},
	DescribeState: func(state interface{}) string { return fmt.Sprintf("%q", state.(string)) },
}

func main() {
	if len(os.Args) != 3 {
		fmt.Fprintln(os.Stderr, "usage: checker <history.json> <visualization.html>")
		os.Exit(2)
	}
	raw, err := os.ReadFile(os.Args[1])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	var recorded []recordedOp
	if err := json.Unmarshal(raw, &recorded); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}

	var history []porcupine.Operation
	ok, unknown, reads := 0, 0, 0
	for _, r := range recorded {
		ret := int64(math.MaxInt64)
		if r.Status == "ok" && r.Return != nil {
			ret = *r.Return
			ok++
		} else if r.Kind == "get" {
			continue // a failed read has no effect and observed nothing
		} else {
			unknown++
		}
		if r.Kind == "get" {
			reads++
		}
		history = append(history, porcupine.Operation{
			ClientId: r.Client,
			Input:    kvInput{Kind: r.Kind, Key: r.Key, Value: r.Value},
			Call:     r.Call,
			Output:   kvOutput{Value: r.Value, Unknown: r.Status != "ok"},
			Return:   ret,
		})
	}
	fmt.Printf("history: %d operations (%d with a definite result, %d writes with unknown outcome, %d reads)\n",
		len(history), ok, unknown, reads)

	start := time.Now()
	// Each key is an independent register, so check every key's history on
	// its own and report each verdict - "LINEARIZABLE" then provably covers
	// every operation, not just whichever partition finished first.
	single := kvModel
	single.Partition = nil
	partitions := kvModel.Partition(history)
	verified, perKeyFailed := 0, 0
	for _, part := range partitions {
		key := part[0].Input.(kvInput).Key
		r := porcupine.CheckOperationsTimeout(single, part, 5*time.Minute)
		verdict := map[porcupine.CheckResult]string{porcupine.Ok: "ok", porcupine.Illegal: "VIOLATION", porcupine.Unknown: "timed out"}[r]
		fmt.Printf("  key %-8s %5d ops: %s\n", key, len(part), verdict)
		if r == porcupine.Ok {
			verified += len(part)
		} else {
			perKeyFailed++
		}
	}
	fmt.Printf("per-key: %d/%d keys verified, %d/%d operations covered\n",
		len(partitions)-perKeyFailed, len(partitions), verified, len(history))

	// Whole-history check (also produces the visualization).
	res, info := porcupine.CheckOperationsVerbose(kvModel, history, 5*time.Minute)
	elapsed := time.Since(start)
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	// Sys is virtual memory the Go runtime reserved - an upper bound on what
	// it used, not resident memory (it can exceed the container's limit
	// without an OOM kill). The authoritative OOM signal is the container's
	// OOMKilled flag, which partition_test.py checks.
	fmt.Printf("checker memory: %d MB reserved by the Go runtime (virtual)\n", ms.Sys/(1<<20))
	// Real resident peak for this container, from the kernel's cgroup v2
	// accounting (includes the `go run` toolchain step as well).
	if b, err := os.ReadFile("/sys/fs/cgroup/memory.peak"); err == nil {
		var peak int64
		fmt.Sscanf(string(b), "%d", &peak)
		fmt.Printf("container memory.peak: %d MB resident (cgroup)\n", peak/(1<<20))
	} else {
		fmt.Printf("container memory.peak: unavailable (%v)\n", err)
	}
	if res == porcupine.Ok && perKeyFailed > 0 {
		res = porcupine.Unknown // the two checks disagree: report no verdict
	}
	if err := porcupine.VisualizePath(kvModel, info, os.Args[2]); err != nil {
		fmt.Fprintln(os.Stderr, "visualization failed:", err)
	}

	switch res {
	case porcupine.Ok:
		fmt.Printf("RESULT: LINEARIZABLE (checked in %v)\n", elapsed.Round(time.Millisecond))
		os.Exit(0)
	case porcupine.Illegal:
		fmt.Printf("RESULT: NOT LINEARIZABLE (checked in %v) - see %s for the offending operations\n",
			elapsed.Round(time.Millisecond), os.Args[2])
		os.Exit(1)
	default:
		fmt.Printf("RESULT: UNKNOWN - checker timed out after %v\n", elapsed.Round(time.Millisecond))
		os.Exit(2)
	}
}
