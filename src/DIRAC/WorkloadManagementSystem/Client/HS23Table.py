"""HS23 (HEPScore23) score lookup from the table bundled with DIRAC.

The HEPiX Benchmarking Working Group publishes the HS23 scores of benchmarked
servers as a CSV table (one row per server characterization: CPU model, SMT
state, power policy, etc.). DIRAC ships a pre-aggregated snapshot of it as
``data/HS23scores.json`` (one entry per normalized CPU model and SMT state),
refreshed by a nightly CI job -- see ``docs/adr/0001-hs23-cpu-power-from-hepix-table.md``.

This module lets a pilot find the per-logical-core HS23 score of the worker
node it runs on by table lookup, instead of running a benchmark at bootstrap:
HEPScore23 itself is a container-based suite far too heavy for a pilot, while
DB12 (the historical alternative) is a rough proxy with known inaccuracies on
modern hardware.

The value returned by :func:`lookupScore` is the ``score_per_core`` of the
entry matching the node, i.e. the ``# Meas``-weighted mean of ``Score/Ncores``
over all the table rows for that key, computed when the snapshot was generated.

Units and terminology: see ``GLOSSARY.md`` at the repository root.
"""

import json
import os
import re
from pathlib import Path

from DIRAC import S_ERROR, S_OK, gLogger

#: The pre-aggregated snapshot bundled with this release
DATA_FILE = Path(__file__).parent / "data" / "HS23scores.json"

#: CPU model strings in the table do not always match what /proc/cpuinfo
#: reports. Keys and values are normalized model names (as returned by
#: :func:`normalizeCpuModel`). Extend as new mismatches are discovered.
ALIASES = {}

_LOG = gLogger.getSubLogger("HS23Table")


def normalizeCpuModel(model: str) -> str:
    """Normalize a CPU model string for matching.

    The table mixes formats like ``Intel(R) Xeon(R) CPU E5-2630 v3 @ 2.40GHz``,
    ``INTEL(R) XEON(R) GOLD 6530`` and ``Intel (R) Xeon (R) CPU Max 9462``:
    lower-case everything, drop the frequency suffix, squash the spacing
    variants of ``(R)`` and collapse repeated whitespace. Must stay in sync
    with the generator in the DIRACGrid/management repository.
    """
    model = model.lower()
    model = model.split("@", 1)[0]
    model = re.sub(r"\s*\(r\)\s*", "(r)", model)
    model = re.sub(r"\s+", " ", model).strip()
    return model


def _getCPUInfo() -> dict:
    """Read the CPU model and the number of physical cores from /proc/cpuinfo.

    ``model name`` is the x86 key; other architectures use ``Model``. The number
    of physical cores is the number of unique (``physical id``, ``core id``)
    pairs, 0 when the kernel does not expose them.

    :return: S_OK({"Model", "PhysicalCores"}), S_ERROR if the CPU model
             cannot be determined
    """
    try:
        cpuinfo = Path("/proc/cpuinfo").read_text()
    except OSError as err:
        return S_ERROR(f"Cannot read /proc/cpuinfo: {err}")

    modelName = None
    genericModel = None
    physicalId = None
    coreIds = set()
    for line in cpuinfo.splitlines():
        field, sep, value = line.partition(":")
        if not sep:
            continue
        field = field.strip().lower()
        value = value.strip()
        if field == "model name" and value:
            modelName = value
        elif field == "model" and value and not value.isdigit():
            # "model : 79" on x86 is a numeric family id, not a usable model name
            genericModel = genericModel or value
        elif field == "physical id" and value:
            physicalId = value
        elif field == "core id" and value:
            coreIds.add((physicalId, value))

    model = modelName or genericModel
    if not model:
        return S_ERROR("Cannot determine the CPU model from /proc/cpuinfo")
    return S_OK({"Model": model, "PhysicalCores": len(coreIds)})


def getNodeInfo() -> dict:
    """Detect the CPU model, the SMT state and the number of online logical CPUs.

    The pilot is considered to see the whole node, whatever subset of it its slot
    will use. The CPU model and the physical core count come from /proc/cpuinfo
    (see :func:`_getCPUInfo`); the logical CPU count from ``os.cpu_count()``, i.e. the
    whole node regardless of the slot's affinity mask.

    :return: S_OK({"Model", "SMT", "LogicalCPUs"}), S_ERROR if the CPU model
             cannot be determined. ``SMT`` is None if it cannot be determined.
    """
    result = _getCPUInfo()
    if not result["OK"]:
        return result
    model = result["Value"]["Model"]
    physicalCores = result["Value"]["PhysicalCores"]

    logicalCPUs = os.cpu_count() or 0

    # SMT state: explicit when the kernel exposes it, else derived from the
    # number of physical cores vs logical CPUs
    smt = None
    try:
        smt = Path("/sys/devices/system/cpu/smt/active").read_text().strip() == "1"
    except OSError:
        if physicalCores and logicalCPUs:
            smt = logicalCPUs > physicalCores

    return S_OK({"Model": model, "SMT": smt, "LogicalCPUs": logicalCPUs})


def _loadTable() -> dict:
    """Load the bundled table snapshot.

    :return: S_OK(table dict with ``source``, ``source_hash``, ``data_as_of``
             and ``entries``), S_ERROR if the file is missing or unparsable
    """
    try:
        table = json.loads(DATA_FILE.read_text())
        entries = table["entries"]
        if not entries:
            raise ValueError("empty table")
    except (OSError, KeyError, ValueError) as err:  # json.JSONDecodeError is a ValueError
        return S_ERROR(f"Cannot load the HS23 table snapshot {DATA_FILE}: {err}")
    return S_OK(table)


def lookupScore(entries: list, model: str, smt: bool) -> dict:
    """Find the per-core HS23 score for a CPU model and SMT state.

    Entries are keyed by normalized model name; as a safety net against any
    drift between the generator's normalization and this one, the original
    model strings are matched as a second pass.

    :param entries: table entries as bundled by :func:`_loadTable`
    :param model: CPU model string, e.g. from /proc/cpuinfo
    :param smt: whether SMT/HT is enabled on the node
    :return: dict with ``Score``, ``Model`` and ``SMT``
    """
    key = normalizeCpuModel(model)
    key = ALIASES.get(key, key)

    matches = [entry for entry in entries if entry["model"] == key and entry["smt"] == int(smt)]
    if not matches:
        # safety net against any drift between the generator's normalization and this one
        matches = [
            entry for entry in entries if normalizeCpuModel(entry["original"]) == key and entry["smt"] == int(smt)
        ]
    if not matches:
        raise LookupError(f"No HS23 score for {model!r} with SMT {'enabled' if smt else 'disabled'}")

    return {"Score": matches[0]["score_per_core"], "Model": key, "SMT": smt}


def getHS23Score(nodeInfo: dict = None) -> dict:
    """Look up the per-core HS23 score of this node in the bundled table.

    :param nodeInfo: node info as returned by :func:`getNodeInfo`; detected if not given
    :return: S_OK({"Score", "Model", "SMT"}), S_ERROR on any failure
    """
    if nodeInfo is None:
        result = getNodeInfo()
        if not result["OK"]:
            return result
        nodeInfo = result["Value"]

    if nodeInfo["SMT"] is None:
        return S_ERROR("Cannot determine the SMT state of the node")

    result = _loadTable()
    if not result["OK"]:
        return result

    try:
        score = lookupScore(result["Value"]["entries"], nodeInfo["Model"], nodeInfo["SMT"])
    except LookupError as err:
        return S_ERROR(str(err))

    return S_OK(score)
