"""HS23 (HEPScore23) score lookup from the HEPiX scores table.

The HEPiX Benchmarking Working Group publishes the HS23 scores of benchmarked
servers as a CSV table (one row per server characterization: CPU model, SMT
state, power policy, etc.). This module lets a pilot find the per-logical-core
HS23 score of the worker node it runs on by table lookup, instead of running a
benchmark at bootstrap: HEPScore23 itself is a container-based suite far too
heavy for a pilot, while DB12 (the historical alternative) is a rough proxy
with known inaccuracies on modern hardware.

The value returned by :func:`lookupScore` is ``Score/Ncores`` of the matching
rows, combined with a ``# Meas``-weighted mean when several rows match.

Units and terminology: see ``GLOSSARY.md`` at the repository root, and
``docs/adr/0001-hs23-cpu-power-from-hepix-table.md`` for the rationale.
"""

import csv
import io
import re
from pathlib import Path

import requests

from DIRAC import S_ERROR, S_OK, gLogger

#: Last-resort location of the HEPiX HS23 scores table
DEFAULT_TABLE_URL = "https://raw.githubusercontent.com/HEPiX-Forum/hepix-forum.github.io/master/_data/HS23scores.csv"

#: Configuration option holding the ordered list of table locations (file paths
#: or URLs), tried in sequence. The default URL is always appended as last resort.
TABLE_LOCATIONS_OPTION = "JobScheduling/HS23TableLocations"

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
    variants of ``(R)`` and collapse repeated whitespace.
    """
    model = model.lower()
    model = model.split("@", 1)[0]
    model = re.sub(r"\s*\(r\)\s*", "(r)", model)
    model = re.sub(r"\s+", " ", model).strip()
    return model


def getNodeInfo() -> dict:
    """Detect the CPU model, the SMT state and the number of online logical CPUs.

    The pilot is considered to see the whole node, whatever subset of it its slot
    will use.

    :return: S_OK({"Model", "SMT", "LogicalCPUs"}), S_ERROR if the CPU model
             cannot be determined. ``SMT`` is None if it cannot be determined.
    """
    try:
        cpuinfo = Path("/proc/cpuinfo").read_text()
    except OSError as err:
        return S_ERROR(f"Cannot read /proc/cpuinfo: {err}")

    model = None
    logicalCPUs = 0
    coreIDs = set()
    physicalID = ""
    for line in cpuinfo.splitlines():
        field, _, value = line.partition(":")
        field = field.strip()
        value = value.strip()
        if field == "processor":
            logicalCPUs += 1
        elif field in ("model name", "Model") and model is None:
            model = value
        elif field == "physical id":
            physicalID = value
        elif field == "core id":
            coreIDs.add((physicalID, value))

    if not model:
        return S_ERROR("Cannot determine the CPU model from /proc/cpuinfo")

    # SMT state: explicit when the kernel exposes it, else derived from the
    # number of unique physical cores vs logical CPUs
    smt = None
    try:
        smt = Path("/sys/devices/system/cpu/smt/active").read_text().strip() == "1"
    except OSError:
        if coreIDs and logicalCPUs:
            smt = logicalCPUs > len(coreIDs)

    return S_OK({"Model": model, "SMT": smt, "LogicalCPUs": logicalCPUs})


def parseTable(text: str) -> list:
    """Parse the HS23 scores CSV into a list of dicts.

    Only the columns needed for the lookup are kept. Rows with an unparsable
    score are skipped.
    """
    rows = []
    for row in csv.DictReader(io.StringIO(text)):
        try:
            rows.append(
                {
                    "CPU": row["CPU"],
                    "SMT": int(row["SMT enabled"]),
                    "ScorePerCore": float(row["Score/Ncores"]),
                    "Measurements": int(row["# Meas"] or 0),
                }
            )
        except (KeyError, TypeError, ValueError):
            continue
    return rows


def lookupScore(rows: list, model: str, smt: bool) -> dict:
    """Find the per-core HS23 score for a CPU model and SMT state.

    All rows matching the normalized model name and SMT state are combined with
    a ``# Meas``-weighted mean of ``Score/Ncores`` (rows with no measurement
    count get weight 1). Power policy, NUMA topology and site are deliberately
    ignored: they are part of the natural spread of the measurement.

    :param rows: table rows as returned by :func:`parseTable`
    :param model: CPU model string, e.g. from /proc/cpuinfo
    :param smt: whether SMT/HT is enabled on the node
    :return: dict with ``Score``, ``MatchedRows`` and ``Model``
    """
    key = normalizeCpuModel(model)
    key = ALIASES.get(key, key)

    candidates = [row for row in rows if normalizeCpuModel(row["CPU"]) == key and row["SMT"] == int(smt)]
    if not candidates:
        raise LookupError(f"No HS23 score for {model!r} with SMT {'enabled' if smt else 'disabled'}")

    weightSum = 0.0
    scoreSum = 0.0
    for row in candidates:
        weight = float(row["Measurements"]) or 1.0
        weightSum += weight
        scoreSum += weight * row["ScorePerCore"]

    return {"Score": round(scoreSum / weightSum, 1), "MatchedRows": len(candidates), "Model": key}


def fetchTable(locations: list = None) -> dict:
    """Fetch the table text from the first location that answers.

    Entries may be local file paths or http(s) URLs. Failures are logged and the
    next location is tried.

    :param locations: ordered list of locations; defaults to the CS option
                      ``JobScheduling/HS23TableLocations`` with the HEPiX URL as
                      last resort
    :return: S_OK(text) or S_ERROR if no location answers
    """
    from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations

    if locations is None:
        locations = Operations().getValue(TABLE_LOCATIONS_OPTION, [])
        if DEFAULT_TABLE_URL not in locations:
            locations.append(DEFAULT_TABLE_URL)

    for location in locations:
        try:
            if location.startswith(("http://", "https://")):
                response = requests.get(location, timeout=60)
                response.raise_for_status()
                text = response.text
            else:
                text = Path(location).read_text()
            if text.strip():
                return S_OK(text)
            _LOG.warn("Empty HS23 table at", location)
        except Exception as err:  # pylint: disable=broad-except
            _LOG.warn("Cannot fetch HS23 table from", f"{location}: {err}")
    return S_ERROR("Could not fetch the HS23 scores table from any configured location")


def getHS23Score(nodeInfo: dict = None, locations: list = None) -> dict:
    """Look up the per-core HS23 score of this node.

    :param nodeInfo: node info as returned by :func:`getNodeInfo`; detected if not given
    :param locations: table locations, as for :func:`fetchTable`
    :return: S_OK({"Score", "MatchedRows", "Model", "SMT"}), S_ERROR on any failure
    """
    if nodeInfo is None:
        result = getNodeInfo()
        if not result["OK"]:
            return result
        nodeInfo = result["Value"]

    if nodeInfo["SMT"] is None:
        return S_ERROR("Cannot determine the SMT state of the node")

    result = fetchTable(locations)
    if not result["OK"]:
        return result

    rows = parseTable(result["Value"])
    if not rows:
        return S_ERROR("The HS23 scores table contains no parsable row")

    try:
        score = lookupScore(rows, nodeInfo["Model"], nodeInfo["SMT"])
    except LookupError as err:
        return S_ERROR(str(err))

    score["SMT"] = nodeInfo["SMT"]
    return S_OK(score)
