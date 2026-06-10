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

<!-- High-level shape: packages, boundaries, who talks to whom. A short ASCII diagram or
     a bullet tree is fine. Keep it current — out-of-date diagrams are worse than none. -->

_TBD_

## What's intentionally different from legacy PBM

<!-- The deltas that matter for code review. Examples:
     - Coordination: doing X instead of control collections in Mongo
     - Storage: dropping support for backend Z
     - CLI shape: flat verbs vs. nested
-->

_TBD_

## Open questions / decisions to make

<!-- Park unresolved design questions here so I can see them when suggesting code.
     When a question is settled, move the answer into "Design principles" or
     "Architecture sketch" and delete it from this list. -->

- _TBD_

## Glossary

<!-- Only terms that don't mean what they mean in legacy PBM, or that are new. -->

_TBD_

## References

<!-- Jira tickets, design docs, Slack threads, prior art links. -->

- Jira epic: _TBD_
- Design doc: _TBD_

---

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
How
<!-- How you want me to behave specifically in x/. These override defaults.
     Examples to replace:
     - "Prototype phase: prefer concrete implementations over interfaces until a second use case appears."
     - "Don't add tests for code I'm still iterating on; ask before writing test files."
     - "When in doubt, ask before adding a new top-level package."
-->

