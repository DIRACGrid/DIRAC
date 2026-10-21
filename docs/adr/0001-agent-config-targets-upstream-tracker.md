# Agent-facing configuration lives in-repo and targets the upstream tracker

Engineering skills operating on this repository (issue tracking, triage, ticketing, review) read their configuration from `docs/agents/`, which is committed to the upstream repository `DIRACGrid/DIRAC` — not kept as per-clone setup — and every skill treats the upstream repository's issue tracker as the single source of tracked work. We decided this because agents running in any contributor's clone must behave identically and post to the tracker where the project's issues actually live (the fork clones carry no issues), and because glossary/ADR/tracker conventions only pay off as shared, versioned artifacts.

## Considered options

- **Repoint each clone's `origin` at upstream** (with a separate push remote for the fork): cleanest for `gh`'s default repo inference, but rejected because contributors keep an origin-fork convention across all their repositories.
- **Per-clone local configuration only**: rejected because it defeats the purpose — the conventions would not reach other contributors' agents.

## Consequences

- `docs/agents/issue-tracker.md` documents the upstream-tracker workflow and is generic in the upstream copy.
- Fork clones (where `origin` is the contributor's fork) pin every `gh` call with `-R DIRACGrid/DIRAC`; their local `issue-tracker.md` carries the pin, and once the tracked upstream version is pulled, the local divergence is held with `git update-index --skip-worktree docs/agents/issue-tracker.md`.
- Whether triage labels exist upstream and whether external PRs are a triage surface are maintainer decisions, recorded separately in `docs/agents/triage-labels.md` when adopted.
