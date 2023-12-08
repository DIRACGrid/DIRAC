""" Test class for LSFResourceUsage utility
"""

import pytest

from DIRAC import S_OK, S_ERROR

from DIRAC.Resources.Computing.BatchSystems.TimeLeft.LSFResourceUsage import LSFResourceUsage

# Sample outputs for LSF batch system commands
LSF_KEK_BQUEUES = """   CPULIMIT
  720.0 min

  RUNLIMIT
  1440.0 min
"""

LSF_LSHOSTS = """ HOST_NAME                       type       model  cpuf ncpus maxmem maxswp server RESOURCES
b66                   SLC6_64 i6_16   2.5    16 29M 19M    Yes (intel share aishare cvmfs wan exe lcg wigner slot15)
  """


def test_getResourceUsageBasic(mocker):
    mocker.patch(
        "DIRAC.Resources.Computing.BatchSystems.TimeLeft.LSFResourceUsage.runCommand",
        side_effect=[S_OK(LSF_KEK_BQUEUES), S_OK(LSF_LSHOSTS)],
    )

    lsfResourceUsage = LSFResourceUsage("1234", {"Host": "b66", "InfoPath": "Unknown"})
    cpuLimitExpected = 720 * 60 / 2.5
    wallClockLimitExpected = 1440 * 60 / 2.5

    # Verify that the calculated limits match the expected values
    assert lsfResourceUsage.cpuLimit == cpuLimitExpected
    assert lsfResourceUsage.wallClockLimit == wallClockLimitExpected


# Additional test data for a more specific setup (e.g., CERN)
LSF_CERN_BQUEUES = """   CPULIMIT
  10080.0 min of KSI2K

  RUNLIMIT
  30240.0 min of KSI2K
"""

LSF_CERN_LSHOSTS_1 = """KSI2K: unknown host name.
"""

LSF_CERN_LSINFO = """MODEL_NAME      CPU_FACTOR      ARCHITECTURE
i6_12_62d7h20_266      3.06
ai_intel_8            2.44
"""


def test_getResourceUsageCern(mocker):
    mocker.patch(
        "DIRAC.Resources.Computing.BatchSystems.TimeLeft.LSFResourceUsage.runCommand",
        side_effect=[S_OK(LSF_CERN_BQUEUES), S_ERROR(LSF_CERN_LSHOSTS_1), S_OK(LSF_CERN_LSINFO), S_OK(LSF_LSHOSTS)],
    )
    mocker.patch("os.path.isfile", return_value=True)
    mocker.patch(
        "DIRAC.Resources.Computing.BatchSystems.TimeLeft.LSFResourceUsage.sourceEnv", return_value=S_ERROR("no lsf.sh")
    )

    lsfResourceUsage = LSFResourceUsage(1234, {"Host": "b66", "InfoPath": "/dev/null"})
    cpuLimitExpected = 241920
    wallClockLimitExpected = 725760
    normrefExpected = 1.0
    hostnormExpected = 2.5
    cpuRef = "KSI2K"

    # Verify that the calculated values and references match the expected values
    assert lsfResourceUsage.cpuLimit == cpuLimitExpected
    assert lsfResourceUsage.wallClockLimit == wallClockLimitExpected
    assert lsfResourceUsage.cpuRef == cpuRef
    assert lsfResourceUsage.normRef == normrefExpected
    assert lsfResourceUsage.hostNorm == hostnormExpected
