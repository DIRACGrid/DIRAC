# HS23 CPU power from the HEPiX table instead of per-pilot measurement

Status: accepted

## Decision

The pilot has always measured the worker node with DB12 at bootstrap and written the result to
`/LocalSite/CPUNormalizationFactor`. We decided to instead derive the node's CPU power from the HEPiX
HEPScore23 score table: the pilot detects the node's CPU model, SMT state and logical CPU count, looks up
`Score/Ncores` among the rows matching the (normalized) model string and SMT flag, combines ambiguous rows
with a `# Meas`-weighted mean, and writes the result to a new option. DB12 measurement survives only as
fallback (table fetch failure or no matching row), and the provenance of each node's value is recorded and
published as a job parameter. Adoption is governed by administrator policy in `Operations/JobScheduling/`
(`CPUNormalizationUnit = DB12 | HS23`, and `RunDB12Benchmark` — unset resolves to "run DB12 iff the unit is
DB12"). The table is fetched from an ordered list of administrator-configured locations, tried in sequence,
with the HEPiX GitHub URL as last resort; phase 1 records and reports the HS23 value but no consumer
changes behavior.

## Considered options

- **Running HEPScore23 in the pilot**: rejected — it is a container-based suite (7 workloads × 3 repetitions)
  requiring infrastructure and runtime far beyond pilot bootstrap.
- **Converting DB12 to HS23 with a fixed factor**: rejected — the HS23/HS06 ratio varies per CPU model
  (observed roughly 0.75–1.6), so no constant conversion exists.
- **Preferring the row from the node's own site**: rejected — a fleet-wide `# Meas`-weighted mean over all
  matching rows was chosen, treating the table as a single body of evidence.
- **New LocalSite key for the HS23 value**: accepted — `CPUNormalizationFactor` keeps its current meaning
  and producer; consumers opt into HS23 via the unit flag rather than by silently changing the scale of an
  existing option.

## Consequences

- When the unit flag flips to HS23, `NormCPUTime` (= CPU time × CPU power) becomes HS23-denominated while
  all historical accounting rows are DB12/HS06-denominated: a permanent discontinuity in accounting plots.
  Accepted and to be documented, consistent with the HEPiX Benchmarking WG's 1:1 HS06 normalization of HS23
  (endorsed by the WLCG MB, December 2022).
- DB12's per-slot concurrency correction (parallel benchmark copies, minimum taken) is lost: the table has no
  slot-level data, so `Score/Ncores` of the SMT-matching row is used as-is.
- Thresholds calibrated in the current unit (`StopSig*` work budgets, `MinimumTimeLeft`, LHCb `CPUe`
  per-event constants, the event-timeout `< 10` check) keep their meaning only while the unit is DB12;
  switching units requires recalibrating them. LHCb event budgets adopt the same DIRAC flag, with `CPUe`
  recalibration as a separate measurement campaign.
- Provenance is deliberately a per-node record (LocalSite + job parameter), not an Operations setting:
  which source produced a value is decided at pilot runtime and differs node by node, while policy is
  per-setup.
- The delivery mechanism for the table (bundled snapshot, CS-synced copy, direct URL) is a pluggable input:
  the lookup consumes table rows and does not care where they came from.
