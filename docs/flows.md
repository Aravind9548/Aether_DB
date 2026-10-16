# Request flows

## A write (SET)

```mermaid
sequenceDiagram
  participant C as Client
  participant G as Gateway
  participant L as Leader
  participant F as Followers
  C->>G: POST /api/v1/storage?key=k (JWT)
  G->>G: verify JWT, key becomes len:tenant:k
  G->>L: gRPC Set
  L->>L: append to WAL (group-commit fsync)
  L->>F: AppendEntries
  F->>F: append + fsync
  F-->>L: ack
  Note over L: 2 of 3 have it on disk, so it's committed
  L->>L: apply to in-memory map
  L-->>G: OK, shard, replicated 2/3 or 3/3
  G-->>C: 200 "OK" + X-Shard-Id, X-Replicated-Count
```

If the gateway reaches a follower, the follower replies `NOT_LEADER` with
the leader's address and the gateway retries there.

## A read (GET)

```mermaid
sequenceDiagram
  participant C as Client
  participant L as Leader
  participant F as Followers
  C->>L: GET k
  Note over L: 1. an entry from my own term is committed?
  L->>F: heartbeat round (sent after the read began)
  F-->>L: ack from a majority
  Note over L: 2. still the leader, proven
  L-->>C: value of k
  Note over L,F: If cut off, no acks arrive: it returns TIMEOUT, not stale data
```

## A partition, before and after the fix

```mermaid
sequenceDiagram
  participant C as Client
  participant OL as Old leader (cut off)
  participant N as New leader (majority)
  Note over OL,N: the network drops traffic between them
  C->>N: SET x = 2
  N-->>C: OK (committed by the majority)
  C->>OL: GET x
  Note over OL: BEFORE: "I'm the leader", so it answers locally
  OL-->>C: x = 1 (stale)
  Note over OL: AFTER: needs a majority of acks and gets none
  OL-->>C: TIMEOUT (client retries the real leader)
```

## Leader failure

```mermaid
sequenceDiagram
  participant L as Old leader
  participant A as Follower A
  participant B as Follower B
  L--xA: crashes, heartbeats stop
  Note over A: election timeout (800–1,600 ms)
  A->>B: PreVote: would you vote for me?
  B-->>A: yes (hasn't heard from a leader)
  A->>B: RequestVote, term + 1
  B-->>A: vote granted (A's log is up to date)
  Note over A: A is the new leader and appends a no-op entry
  Note over L: Docker restarts it; it rejoins as a follower and catches up
```

Measured from `docker kill` to a new leader confirmed: 999–2,351 ms.

## Kill Leader (Cluster Console)

```mermaid
sequenceDiagram
  participant U as Browser
  participant G as Gateway
  participant L as Leader
  participant D as Docker
  U->>G: POST /api/v1/cluster/kill-leader
  G->>L: Status() on every node, pick the LEADER
  G->>L: KillSelf()
  L-->>G: acknowledged
  Note over L: 150 ms later: kill(getpid(), SIGKILL)
  Note over L: followers elect a new leader
  D->>L: restart: always brings it back as a follower
```

The engine kills itself rather than the gateway calling `docker kill`,
because giving the gateway the Docker socket would be root access to the
host. The container runs with `init: true`: without an init process, the
engine is PID 1, and Linux ignores signals that PID 1 sends to itself.

## Catching up after a long outage

```mermaid
flowchart LR
  A["Follower returns<br/>after a long outage"] --> B{"Does the leader still<br/>have the entries it needs?"}
  B -- "yes" --> C["AppendEntries<br/>(up to 256 entries per RPC)"]
  B -- "no, compacted" --> D["InstallSnapshot:<br/>whole state at index N"]
  D --> C
  C --> E["Caught up"]
```
