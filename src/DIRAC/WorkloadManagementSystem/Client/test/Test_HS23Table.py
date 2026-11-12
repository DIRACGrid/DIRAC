"""Unit tests for the HS23 table lookup"""

import pathlib

import pytest

from DIRAC.WorkloadManagementSystem.Client import HS23Table
from DIRAC.WorkloadManagementSystem.Client.HS23Table import (
    _getCPUInfo,
    _loadTable,
    getHS23Score,
    lookupScore,
    normalizeCpuModel,
)

CPUINFO = """processor	: 0
model name	: AMD EPYC 7302 16-Core Processor
physical id	: 0
core id	: 0
processor	: 1
model name	: AMD EPYC 7302 16-Core Processor
physical id	: 0
core id	: 1
"""

# two logical CPUs sharing one physical core (SMT enabled)
CPUINFO_SMT = """processor	: 0
model name	: AMD EPYC 7302 16-Core Processor
physical id	: 0
core id	: 0
processor	: 1
model name	: AMD EPYC 7302 16-Core Processor
physical id	: 0
core id	: 0
"""


@pytest.fixture
def table():
    """The bundled table snapshot"""
    result = _loadTable()
    assert result["OK"], result["Message"]
    return result["Value"]


def test_normalizeCpuModel():
    assert normalizeCpuModel("Intel(R) Xeon(R) CPU E5-2630 v3 @ 2.40GHz") == normalizeCpuModel(
        "INTEL(R) XEON(R) CPU E5-2630 V3 @ 2.40 GHZ"
    )
    assert normalizeCpuModel("Intel (R) Xeon (R) CPU Max 9462") == normalizeCpuModel("Intel(R) Xeon(R) CPU Max 9462")
    assert normalizeCpuModel("Intel(R) Xeon(R) CPU           E5520  @ 2.27GHz") == normalizeCpuModel(
        "Intel(R) Xeon(R) CPU E5520"
    )


def test_bundledTable(table):
    """The snapshot bundled with this release is sane: keys are normalized, scores plausible"""
    assert table["source"].startswith("https://")
    assert table["source_hash"].startswith("sha256:")
    assert table["data_as_of"]
    assert len(table["entries"]) > 100
    for entry in table["entries"]:
        assert normalizeCpuModel(entry["model"]) == entry["model"]  # normalization is idempotent
        assert entry["smt"] in (0, 1)
        assert 1.0 < entry["score_per_core"] < 200.0
        assert entry["measurements"] >= 0 and entry["rows"] >= 1


def test_lookupScore(table):
    result = lookupScore(table["entries"], "AMD EPYC 7302 16-Core Processor", True)
    assert 1.0 < result["Score"] < 200.0
    assert result["Model"] == "amd epyc 7302 16-core processor"
    assert result["SMT"] is True


def test_lookupScore_normalization(table):
    # model strings from /proc/cpuinfo, matched against the table's inconsistent formats
    result = lookupScore(table["entries"], "INTEL(R) XEON(R) GOLD 6530", False)
    assert 1.0 < result["Score"] < 200.0
    result = lookupScore(table["entries"], "Intel(R) Xeon(R) CPU E5520 @ 2.27GHz", False)
    assert 1.0 < result["Score"] < 200.0


def test_lookupScore_alias(table, mocker):
    # an alias maps a model name absent from the table onto one that is present
    mocker.patch.object(HS23Table, "ALIASES", {"amd epyc 7302": "amd epyc 7302 16-core processor"})
    result = lookupScore(table["entries"], "AMD EPYC 7302", True)
    assert result["Model"] == "amd epyc 7302 16-core processor"


def test_lookupScore_noMatch(table):
    with pytest.raises(LookupError):
        lookupScore(table["entries"], "Intel(R) Xeon(R) Platinum 9999", False)
    with pytest.raises(LookupError):
        lookupScore(table["entries"], "Nonexistent CPU", True)


def test_getCPUInfo(mocker):
    mocker.patch.object(pathlib.Path, "read_text", side_effect=[CPUINFO], autospec=True)
    result = _getCPUInfo()
    assert result["OK"]
    assert result["Value"]["Model"] == "AMD EPYC 7302 16-Core Processor"
    assert result["Value"]["PhysicalCores"] == 2


def test_getCPUInfo_noModel(mocker):
    mocker.patch.object(pathlib.Path, "read_text", side_effect=OSError("nope"), autospec=True)
    result = _getCPUInfo()
    assert not result["OK"]
    assert "/proc/cpuinfo" in result["Message"]


def test_getCPUInfo_numericModelIgnored(mocker):
    # x86 /proc/cpuinfo without "model name": the numeric "model" family id is not a model name
    cpuinfo = "processor	: 0\nvendor_id	: GenuineIntel\nmodel	: 79\n"
    mocker.patch.object(pathlib.Path, "read_text", side_effect=[cpuinfo], autospec=True)
    result = _getCPUInfo()
    assert not result["OK"]


def test_getCPUInfo_genericModel(mocker):
    # aarch64 /proc/cpuinfo uses the "Model" key
    cpuinfo = "processor	: 0\nModel	: Cortex-X925\n"
    mocker.patch.object(pathlib.Path, "read_text", side_effect=[cpuinfo], autospec=True)
    result = _getCPUInfo()
    assert result["OK"]
    assert result["Value"]["Model"] == "Cortex-X925"


def test_getNodeInfo(mocker):
    mocker.patch.object(pathlib.Path, "read_text", side_effect=[CPUINFO, "0"], autospec=True)
    mocker.patch("os.cpu_count", return_value=2)
    result = HS23Table.getNodeInfo()
    assert result["OK"]
    assert result["Value"]["Model"] == "AMD EPYC 7302 16-Core Processor"
    assert result["Value"]["SMT"] is False
    assert result["Value"]["LogicalCPUs"] == 2


def test_getNodeInfo_smtFromCoreCounts(mocker):
    # no /sys/devices/system/cpu/smt/active: SMT derived from the physical core count in /proc/cpuinfo
    mocker.patch.object(pathlib.Path, "read_text", side_effect=[CPUINFO_SMT, OSError("nope")], autospec=True)
    mocker.patch("os.cpu_count", return_value=2)
    result = HS23Table.getNodeInfo()
    assert result["OK"]
    assert result["Value"]["SMT"] is True


def test_getHS23Score(table):
    result = getHS23Score(nodeInfo={"Model": "AMD EPYC 7302 16-Core Processor", "SMT": True, "LogicalCPUs": 64})
    assert result["OK"]
    assert 1.0 < result["Value"]["Score"] < 200.0
    assert result["Value"]["SMT"] is True


def test_getHS23Score_unknownSMT(table):
    result = getHS23Score(nodeInfo={"Model": "AMD EPYC 7302 16-Core Processor", "SMT": None, "LogicalCPUs": 64})
    assert not result["OK"]
    assert "SMT" in result["Message"]


def test_loadTable_missing(mocker):
    mocker.patch.object(HS23Table, "DATA_FILE", pathlib.Path("/nonexistent/HS23scores.json"))
    result = _loadTable()
    assert not result["OK"]
