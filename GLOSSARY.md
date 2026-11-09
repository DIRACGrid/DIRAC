# DIRAC Workload Management

How DIRAC sizes, matches, and accounts for CPU consumption of jobs on worker nodes.

## Language

### CPU budget

**CPU power**:
The speed of a worker node (or slot) expressed as a benchmark score, used to convert wall-clock seconds into CPU work and back.
_Avoid_: CPU power value, benchmark factor (unqualified)

**CPU work**:
Wall-clock seconds multiplied by CPU power; the unit in which the pilot slot budget is expressed and spent.
_Avoid_: normalized time, HS06 seconds (ambiguous once the unit is switchable)

**HS06**:
The legacy WLCG benchmark unit (HEP-SPEC06), the reference scale for CPU power in DIRAC.
_Avoid_: HEPSPEC06

**HS23**:
HEPScore23, the current WLCG benchmark unit since April 2023, normalized 1:1 with HS06 on the reference machine.
_Avoid_: HEPScore23 (when referring to the unit)

**HS23 score**:
The per-node benchmark figure published in the HEPiX HS23 table; the per-logical-core value is Score/Ncores of the row matching the node's CPU model and SMT state.
_Avoid_: HS23 factor, HS23 benchmark result (unqualified)

**DB12**:
DIRAC Benchmark 12, the lightweight pure-Python benchmark run at pilot bootstrap, approximating HS06.
_Avoid_: DB12 score (when the HS23 score is meant)

**DB16**:
DIRAC Benchmark 16, the internal unit in which LHCb event budgets (CPUe) are expressed.
_Avoid_: DB16-seconds per event (in glossary context)

**Normalization unit**:
Which benchmark scales (DB12, HS23, or both) are recorded and used on a site, expressed as an ordered list in which the first entry is the active one; chosen by administrator policy, globally for the setup.
_Avoid_: benchmark mode, normalization mode

### Provenance vs policy

**Provenance**:
The per-node record of what actually produced a node's CPU power: the HS23 table or the DB12 fallback. A fact decided at pilot runtime, not by configuration.
_Avoid_: source tag (ambiguous), normalization source (unqualified)

**Source policy**:
The administrator-controlled settings that decide which provenance is preferred and whether DB12 also runs. Lives in configuration; one value per setup, not per node.
_Avoid_: provenance (when the policy is meant)

**Fallback**:
The DB12 measurement used when the HS23 table cannot be fetched or the node's model has no matching row.
_Avoid_: default normalization
