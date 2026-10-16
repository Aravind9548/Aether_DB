# AetherDB documentation

These pages read directly on GitHub, and the same files build the docs
website with [VitePress](https://vitepress.dev).

| Page | Covers |
|---|---|
| [Overview](./index.md) | What AetherDB is, key numbers, tech stack |
| [Getting started](./getting-started.md) | Running it locally and using the API |
| [Architecture](./architecture.md) | Components, ports, storage engine, gateway |
| [Consensus (Raft)](./raft.md) | Elections, replication, ReadIndex, CheckQuorum, PreVote, snapshots |
| [Request flows](./flows.md) | Diagrams of writes, reads, failover, partitions |
| [Design decisions](./design-decisions.md) | What was chosen over what, and why |
| [API reference](./api.md) | REST endpoints, engine commands, gRPC services |
| [Configuration & operations](./operations.md) | Settings, health checks, backup and restore |
| [Testing](./testing.md) | Test suites, partition testing, linearizability checking |
| [Findings](./findings.md) | The three Raft bugs partition testing found |
| [Limitations & roadmap](./limitations.md) | What's missing, and what's next |
| [Glossary](./glossary.md) | Terms explained |

## Building the site

```bash
cd docs
npm install
npm run dev      # local preview at http://localhost:5173
npm run build    # static site in docs/.vitepress/dist
```

Deployed on Vercel with `docs` as the project root (see `vercel.json`).
