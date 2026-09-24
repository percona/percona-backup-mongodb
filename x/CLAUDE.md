# CLAUDE.md — pbm-x

Context for working inside `x/` (the experimental next-major-version prototype). The root `CLAUDE.md` covers the legacy `pbm/` codebase; this file is just for `x/`.

**Hard rule:** `x/` is a separate Go module. Do not import from `github.com/percona/percona-backup-mongodb/...` (the legacy module) or vice versa. Reuse ideas, not code.

---

## Vision

<!-- One paragraph: what pbm-x is trying to become and why a clean rewrite. Fill in. -->

_TBD_

## Scope of the current prototype

<!-- What's in scope right now, what's deferred. Update as the prototype grows. -->

- In scope: _TBD_
- Out of scope (for now): _TBD_
- Out of scope (forever / explicit non-goals): _TBD_

## Design principles

<!-- The "how" rules — what should pbm-x be opinionated about? Examples to replace:
     - Prefer X over Y because …
     - Cancellation is always context-driven; no goroutine leaks
     - Storage backends are pure interfaces with no MongoDB knowledge
     - Single binary, subcommand-based — no separate agent/CLI split (or: keep the split, because …)
-->

_TBD_

## Architecture sketch

Same overall shape as legacy PBM — an agent runs alongside each `mongod` — but agents now come in **two roles**:

- **ctrl agent** — owns PBM's control-collection state (the leader-election / command-queue / status that legacy PBM kept inside MongoDB control collections). In pbm-x this state lives in an **etcd** database that the ctrl agent runs/manages.
- **worker agent** — does **not** run etcd. Its core responsibility is executing the actual work: backup and restore.

```
        +-------------------+        control-collection state
        |    ctrl agent     |  runs  (etcd)
        +-------------------+           ^
                                        | etcd client API
                                        | (local or remote)
        +-------------------+           |
        |   worker agent    |  --etcd client--+   also does backup / restore
        +-------------------+
```

**Coordination:** worker agents connect to the ctrl agent's **etcd as clients** to read/write control state — they speak the etcd client API directly, not a separate protocol. A worker may be co-located with the ctrl agent or remote, so etcd's client endpoint must be externally reachable (we bind `0.0.0.0`). The peer API is for etcd↔etcd traffic between ctrl agents.

Open: how many ctrl agents run (single vs. quorum) and etcd deployment topology — see Open questions.

### Code layout

A single `pbmx` binary covers both the CLI and the agent (selected at runtime by `--ctrl-agent` / `--worker-agent`). Two layers, with a one-way dependency:

```
cmd/pbmx/   →   pbm/
  CLI only        all runtime logic
```

- **`cmd/pbmx/`** — CLI wiring *only*: cobra command definitions, flags, env-var bindings, and config-file loading. It parses input and dispatches; it holds no operational logic. The root command's `RunE` reads the role flag and calls into `pbm/`.
- **`pbm/`** (`github.com/percona/percona-backup-mongodb/x/pbm`) — all other logic. Today: `RunCtrlAgent` + embedded-etcd startup/graceful-shutdown (`etcd.go`). Worker-agent, backup/restore, and coordination logic land here too.

Rule: dependency flows one way, `cmd/pbmx/` → `pbm/`. `pbm/` must not import `cmd/pbmx/`. Anything beyond CLI parsing/dispatch belongs in `pbm/`.

## What's intentionally different from legacy PBM

<!-- The deltas that matter for code review. -->

- **Coordination / state store:** legacy PBM keeps control-collection state (locks, command queue, status) inside MongoDB itself. pbm-x moves that state into an **etcd** database, owned by the **ctrl agent**.
- **Agent roles split in two:** legacy PBM runs one uniform agent per node. pbm-x splits responsibilities into **ctrl agents** (manage control state in etcd) and **worker agents** (run backup/restore, no etcd).

## Open questions / decisions to make

<!-- Park unresolved design questions here so I can see them when suggesting code.
     When a question is settled, move the answer into "Design principles" or
     "Architecture sketch" and delete it from this list. -->

- **etcd is currently plaintext + unauthenticated.** The client and peer APIs are served over `http` with no auth, and the client API is bound on `0.0.0.0` (remote workers legitimately need it — see Coordination). This cannot be closed by binding localhost; it's an **auth/TLS** task to tackle later (etcd RBAC and/or client/peer TLS), not a bind-address one.
- How many ctrl agents run — single node vs. a 3/5-member quorum — and the etcd deployment topology that follows from it.

## Glossary

<!-- Only terms that don't mean what they mean in legacy PBM, or that are new. -->

- **ctrl agent** — agent role that owns and manages PBM's control-collection state, backed by an etcd database it runs.
- **worker agent** — agent role that performs backup and restore. Does not run etcd.
- **control-collection state** — in legacy PBM this lived in MongoDB collections; in pbm-x it lives in etcd (managed by the ctrl agent).

## Working preferences for this prototype
- when generating golang code follow following guidelines:
    - https://google.github.io/styleguide/go/
    - https://google.github.io/styleguide/go/guide
    - https://google.github.io/styleguide/go/decisions
    - https://google.github.io/styleguide/go/best-practices
- additionally you can expand with uber's style guide:
    - https://github.com/uber-go/guide/blob/master/style.md
- git
    - never create git commits or pr requests automatically.
- never modify anything out of x directory. If it's necessary ask for confirmation.
<!-- How you want me to behave specifically in x/. These override defaults.
     Examples to replace:
     - "Prototype phase: prefer concrete implementations over interfaces until a second use case appears."
     - "Don't add tests for code I'm still iterating on; ask before writing test files."
     - "When in doubt, ask before adding a new top-level package."
-->

