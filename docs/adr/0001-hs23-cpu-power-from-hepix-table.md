# D-ADR-001: HS23 CPU power from the HEPiX table instead of per-pilot measurement

## Metadata

- **Created By:** Federico Stagni
- **Date:** 2026-11-10
- **Status:** Draft
- **Decision Maker(s):** TBD

## Decision

The pilot has always measured the worker node with [DB12](https://github.com/DIRACGrid/DB12) at bootstrap and written the result to
`/LocalSite/CPUNormalizationFactor`. We decided to add the possibility to derive the node's CPU power from the HEPiX
HEPScore23 score [table](https://raw.githubusercontent.com/HEPiX-Forum/hepix-forum.github.io/master/_data/HS23scores.csv):
the pilot detects the node's CPU model, SMT state and logical CPU count, and
looks up the per-core score (the `# Meas`-weighted mean of `Score/Ncores` over all the rows matching the
normalized model name and SMT flag) in a **pre-aggregated snapshot bundled with DIRAC**
(`data/HS23scores.json`, part of the release). If HS23 score is used, the DB12 measurement survives only as fallback (no matching
entry, e.g. brand-new hardware), and the provenance of the active value is recorded
(`/LocalSite/CPUNormalizationSource`) and published as a job parameter.

Which units are recorded and which one is used is governed by a single setup-global policy option,
`Operations/JobScheduling/CPUNormalizationUnit`, an **ordered list** (default `["DB12"]`):

- **units**: every unit in the list is recorded (DB12 measured, HS23 looked up) and published;
- **order**: the first entry is the *active* unit, the one consumers honor. `["DB12"]` is pure legacy;
  `["HS23", "DB12"]` records both with HS23 active; `["DB12", "HS23"]` records both with DB12 active.

There is no per-site value or per-site choice: the policy applies to all sites. The HS23 scores' snapshot is
refreshed by a nightly CI job in the DIRAC repository, which runs the generator script maintained in
the DIRACGrid/management repository and opens (or updates) a PR whenever the derived table changed —
new CPU models or score changes. The PR is never merged automatically.

## Considered options

- **Converting DB12 to HS23 with a fixed factor**: rejected — the HS23/HS06 ratio varies per CPU model
  (observed roughly 0.75–1.6), so no constant conversion exists.
- **Preferring the row from the node's own site**: rejected — a fleet-wide `# Meas`-weighted mean over
  all matching rows was chosen, treating the table as a single body of evidence.
- **Fetching the table at pilot bootstrap** (from a configurable list of file paths/URLs): rejected —
  it made every pilot's most fragile moment depend on the network and on GitHub's availability, and
  pinned nothing. Superseded by the bundled snapshot: deterministic, reviewable, versioned with the
  release, refreshed by CI instead of by pilots.
- **A separate `RunDB12Benchmark` flag** to keep measuring DB12 alongside HS23: superseded before merge
  by the ordered `CPUNormalizationUnit` list, whose membership expresses the same thing with one knob.

## Consequences

- When the active unit flips to HS23, `NormCPUTime` (= CPU time × CPU power) becomes HS23-denominated
  while all historical accounting rows are DB12/HS06-denominated: a permanent discontinuity in
  accounting plots. Accepted and to be documented, consistent with the HEPiX Benchmarking WG's 1:1 HS06
  normalization of HS23 (endorsed by the WLCG MB, December 2022).
- DB12's per-slot concurrency correction (parallel benchmark copies, minimum taken) is lost: the table
  has no slot-level data, so the per-core score of the SMT-matching entry is used as-is.
- Provenance is deliberately a per-node record (LocalSite + parameters), not an Operations setting:
  which source produced a value is decided at DIRAC Pilot runtime, while policy is per-setup.
- The bundled snapshot ages within a release: a CPU model released after it can only be matched after
  the table PR is merged and a DIRAC release goes out. Until then those nodes fall back to DB12. There
  is deliberately no per-site escape hatch.
- The CSV-to-JSON aggregation lives in one place, the generator script in DIRACGrid/management, shared
  by the nightly CI and by its own tests; DIRAC only consumes the JSON.
