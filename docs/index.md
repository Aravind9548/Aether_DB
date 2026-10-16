# AetherDB

AetherDB is a distributed key-value database built from scratch. A C++17
storage engine runs as a **3-node cluster** that stays consistent using the
**Raft** consensus algorithm. A **Spring Boot gateway** in front handles
login, keeps each user's data separate, and routes requests to the current
leader. A web dashboard shows the cluster working live.

## What it does

- **Survives a node failure without losing data.** A write is confirmed
  only after 2 of the 3 nodes have it on disk. Kill any one node and the
  cluster keeps serving, with a new leader elected in about 1–2.4 seconds.
- **Gives consistent answers.** Reads are *linearizable*: once a write is
  confirmed, no later read anywhere returns an older value, even during a
  network partition.
- **Bounded disk use.** The log is compacted into snapshots, and a node that
  falls far behind is rebuilt from a snapshot.
- **Multi-tenant.** Each user's keys live in their own namespace; one tenant
  can't read or overwrite another's.

## Key numbers

| | |
|---|---|
| Cluster | 3 nodes, tolerates any 1 failure |
| Leader failover | 999–2,351 ms (measured, varies with host load) |
| Write throughput | 194–376 ops/sec through the gateway, 20 clients (was 90.5 before group commit) |
| Acknowledged writes lost | 0, in every test run |
| Linearizability | 6,922 / 6,922 operations verified under random network partitions |
| WAL size after 1,500 writes | 2,073 bytes (was 41,698 and growing before compaction) |

All measured on a laptop (Docker Desktop on WSL2, 3.9 GB VM). The numbers
matter as before/after comparisons on the same machine.

## The interesting part

By July 2026 the Raft code passed every test written for it, all of them
crash tests. The first time it was tested with **real network partitions
and an independent linearizability checker**, it failed: three real bugs,
including one that violated Raft's core safety property. They were fixed
and re-verified in one clean end-to-end run.
[Read the findings →](./findings.md)

## Where to go next

| If you want to… | Read |
|---|---|
| Run it locally | [Getting started](./getting-started.md) |
| Understand the design | [Architecture](./architecture.md), [Consensus (Raft)](./raft.md), [Request flows](./flows.md) |
| Call the API | [API reference](./api.md) |
| Configure or operate it | [Configuration & operations](./operations.md) |
| See how it was verified | [Testing](./testing.md), [Findings](./findings.md) |
| Know what's missing | [Limitations & roadmap](./limitations.md) |
| Look up a term | [Glossary](./glossary.md) |

## Tech stack

| Layer | Technology |
|---|---|
| Storage engine | C++17, gRPC/protobuf, CMake |
| Consensus | Raft (written from scratch): ReadIndex, CheckQuorum, PreVote, InstallSnapshot |
| Gateway | Java 21, Spring Boot 3, Spring Security (JWT + Google OAuth), Bucket4j |
| Data stores | PostgreSQL (users), Redis (rate-limit buckets) |
| Deployment | Docker Compose |
| Testing | Python harnesses, iptables partitions, Porcupine (Go) linearizability checker |
