"""Unit tests for the HS23 table lookup"""

import pathlib

import pytest

from DIRAC.WorkloadManagementSystem.Client import HS23Table
from DIRAC.WorkloadManagementSystem.Client.HS23Table import (
    fetchTable,
    getHS23Score,
    lookupScore,
    normalizeCpuModel,
    parseTable,
)

TABLE = """CPU,SMT enabled,Power Policy,# Meas,Score/Ncores,Site
AMD EPYC 7302 16-Core Processor,0,not_available,336,23.9,CERN
AMD EPYC 7302 16-Core Processor,1,performance,182,15.3,CERN
AMD EPYC 7302 16-Core Processor,1,conservative,11,15.8,CC-IN2P3
AMD EPYC 7302 16-Core Processor,1,not_available,0,15.0,CERN
INTEL(R) XEON(R) GOLD 6530,0,performance,21,27.6,IHEP
Intel(R) Xeon(R) CPU           E5520  @ 2.27GHz,0,not_available,6,9.9,IN2P3-SUBATECH
"""

CPUINFO = """processor	: 0
model name	: AMD EPYC 7302 16-Core Processor
physical id	: 0
core id	: 0
processor	: 1
model name	: AMD EPYC 7302 16-Core Processor
physical id	: 0
core id	: 1
"""

CPUINFO_NO_MODEL = """processor	: 0
vendor_id	: GenuineIntel
"""


@pytest.fixture
def tableRows():
    return parseTable(TABLE)


def test_normalizeCpuModel():
    assert normalizeCpuModel("Intel(R) Xeon(R) CPU E5-2630 v3 @ 2.40GHz") == normalizeCpuModel(
        "INTEL(R) XEON(R) CPU E5-2630 V3 @ 2.40 GHZ"
    )
    assert normalizeCpuModel("Intel (R) Xeon (R) CPU Max 9462") == normalizeCpuModel("Intel(R) Xeon(R) CPU Max 9462")
    assert normalizeCpuModel("Intel(R) Xeon(R) CPU           E5520  @ 2.27GHz") == normalizeCpuModel(
        "Intel(R) Xeon(R) CPU E5520"
    )


def test_parseTable(tableRows):
    # the header-only row and the row with no measurement count are kept, broken rows are skipped
    assert len(tableRows) == 6
    assert tableRows[0]["SMT"] == 0
    assert tableRows[0]["ScorePerCore"] == 23.9
    assert tableRows[0]["Measurements"] == 336
    assert tableRows[3]["Measurements"] == 0


def test_parseTable_broken_rows():
    table = "CPU,SMT enabled,# Meas,Score/Ncores\nAMD EPYC 7302 16-Core Processor,1,3,15.3\nBad Model,x,y,z\n"
    rows = parseTable(table)
    assert len(rows) == 1
    assert rows[0]["CPU"] == "AMD EPYC 7302 16-Core Processor"


def test_lookupScore_smt0(tableRows):
    result = lookupScore(tableRows, "AMD EPYC 7302 16-Core Processor", False)
    assert result["Score"] == 23.9
    assert result["MatchedRows"] == 1


def test_lookupScore_weightedMean(tableRows):
    # (182 x 15.3 + 11 x 15.8 + 1 x 15.0) / 194 -- the row with no measurement count weighs 1
    result = lookupScore(tableRows, "AMD EPYC 7302 16-Core Processor", True)
    assert result["Score"] == 15.3
    assert result["MatchedRows"] == 3


def test_lookupScore_normalization(tableRows):
    # model strings from /proc/cpuinfo, matched against the table's inconsistent formats
    result = lookupScore(tableRows, "INTEL(R) XEON(R) GOLD 6530", False)
    assert result["Score"] == 27.6
    result = lookupScore(tableRows, "Intel(R) Xeon(R) CPU E5520 @ 2.27GHz", False)
    assert result["Score"] == 9.9


def test_lookupScore_alias(tableRows, mocker):
    # an alias maps a model name absent from the table onto one that is present
    mocker.patch.object(HS23Table, "ALIASES", {"amd epyc 7302": "amd epyc 7302 16-core processor"})
    result = lookupScore(tableRows, "AMD EPYC 7302", False)
    assert result["Score"] == 23.9
    assert result["Model"] == "amd epyc 7302 16-core processor"


def test_lookupScore_noMatch(tableRows):
    with pytest.raises(LookupError):
        lookupScore(tableRows, "Intel(R) Xeon(R) Gold 6326 CPU @ 2.90GHz", False)
    with pytest.raises(LookupError):
        lookupScore(tableRows, "Nonexistent CPU", True)


def test_getNodeInfo(mocker):
    mocker.patch.object(pathlib.Path, "read_text", side_effect=[CPUINFO, "0"], autospec=True)
    mocker.patch("psutil.cpu_count", side_effect=[2])
    result = HS23Table.getNodeInfo()
    assert result["OK"]
    assert result["Value"]["Model"] == "AMD EPYC 7302 16-Core Processor"
    assert result["Value"]["SMT"] is False
    assert result["Value"]["LogicalCPUs"] == 2


def test_getNodeInfo_noModel(mocker):
    mocker.patch.object(pathlib.Path, "read_text", side_effect=OSError("nope"), autospec=True)
    result = HS23Table.getNodeInfo()
    assert not result["OK"]
    assert "/proc/cpuinfo" in result["Message"]


def test_getNodeInfo_smtFromCoreCounts(mocker):
    # no /sys/devices/system/cpu/smt/active: SMT derived from psutil's core counts
    mocker.patch.object(pathlib.Path, "read_text", side_effect=[CPUINFO, OSError("nope")], autospec=True)
    mocker.patch("psutil.cpu_count", side_effect=[2, 1])
    result = HS23Table.getNodeInfo()
    assert result["OK"]
    assert result["Value"]["SMT"] is True


def test_getCPUModel_numericModelIgnored(mocker):
    # x86 /proc/cpuinfo without "model name": the numeric "model" family id is not a model name
    cpuinfo = "processor	: 0\nvendor_id	: GenuineIntel\nmodel	: 79\n"
    mocker.patch.object(pathlib.Path, "read_text", side_effect=[cpuinfo], autospec=True)
    result = HS23Table.getCPUModel()
    assert not result["OK"]


def test_getCPUModel_genericModel(mocker):
    # aarch64 /proc/cpuinfo uses the "Model" key
    cpuinfo = "processor	: 0\nModel	: Cortex-X925\n"
    mocker.patch.object(pathlib.Path, "read_text", side_effect=[cpuinfo], autospec=True)
    result = HS23Table.getCPUModel()
    assert result["OK"]
    assert result["Value"] == "Cortex-X925"


def test_getHS23Score(tmp_path):
    tableFile = tmp_path / "HS23scores.csv"
    tableFile.write_text(TABLE)
    nodeInfo = {"Model": "AMD EPYC 7302 16-Core Processor", "SMT": True, "LogicalCPUs": 64}
    result = getHS23Score(nodeInfo=nodeInfo, locations=[str(tableFile)])
    assert result["OK"]
    assert result["Value"]["Score"] == 15.3
    assert result["Value"]["SMT"] is True


def test_getHS23Score_unknownSMT():
    nodeInfo = {"Model": "AMD EPYC 7302 16-Core Processor", "SMT": None, "LogicalCPUs": 64}
    result = getHS23Score(nodeInfo=nodeInfo, locations=[])
    assert not result["OK"]
    assert "SMT" in result["Message"]


def test_fetchTable_firstWins(tmp_path):
    first = tmp_path / "first.csv"
    second = tmp_path / "second.csv"
    first.write_text(TABLE)
    second.write_text("CPU,SMT enabled,# Meas,Score/Ncores\nOther CPU,1,1,1.0\n")
    result = fetchTable(locations=[str(first), str(second)])
    assert result["OK"]
    assert "AMD EPYC 7302" in result["Value"]


def test_fetchTable_failSoft(tmp_path, mocker):
    good = tmp_path / "good.csv"
    good.write_text(TABLE)
    mocker.patch("requests.get", side_effect=OSError("network down"))
    result = fetchTable(locations=["https://example.org/HS23scores.csv", str(good)])
    assert result["OK"]
    assert "AMD EPYC 7302" in result["Value"]


def test_fetchTable_allFail(mocker):
    mocker.patch("requests.get", side_effect=OSError("network down"))
    result = fetchTable(locations=["https://example.org/HS23scores.csv", "/nonexistent/path.csv"])
    assert not result["OK"]
