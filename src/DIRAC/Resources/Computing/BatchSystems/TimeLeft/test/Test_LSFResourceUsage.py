""" Test class for LSFResourceUsage utility
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import pytest

from DIRAC import S_OK, S_ERROR

from DIRAC.Resources.Computing.BatchSystems.TimeLeft.LSFResourceUsage import LSFResourceUsage

LSF_KEK_BQUEUES = """   CPULIMIT
  720.0 min

  RUNLIMIT
  1440.0 min
"""

LSF_LSHOSTS = """ HOST_NAME                       type       model  cpuf ncpus maxmem maxswp server RESOURCES
b66                   SLC6_64 i6_16   2.5    16 29M 19M    Yes (intel share aishare cvmfs wan exe lcg wigner slot15)
  """

LSF_CERN_BQUEUES = """   CPULIMIT
  10080.0 min of KSI2K

  RUNLIMIT
  30240.0 min of KSI2K
"""

# returns with S_ERROR
LSF_CERN_LSHOSTS_1 = """KSI2K: unknown host name.
"""

# shortened
LSF_CERN_LSINFO = """MODEL_NAME      CPU_FACTOR      ARCHITECTURE
i6_12_62d7h20_266      3.06
ai_intel_8            2.44
"""


@pytest.mark.parametrize("runCommandResult, lsbHosts, cpuLimitExpected, wallClockLimitExpected", [
    ((S_OK(LSF_KEK_BQUEUES), S_OK(LSF_LSHOSTS)), 'b66', (720 * 60 / 2.5), (1440 * 60 / 2.5))
])
def test_init(mocker, runCommandResult, lsbHosts, cpuLimitExpected, wallClockLimitExpected):
  mocker.patch("DIRAC.Resources.Computing.BatchSystems.TimeLeft.LSFResourceUsage.runCommand",
               side_effect=runCommandResult)
  mocker.patch.dict(os.environ, {'LSB_HOSTS': lsbHosts})

  lsfResourceUsage = LSFResourceUsage()
  assert lsfResourceUsage.cpuLimit == cpuLimitExpected
  assert lsfResourceUsage.wallClockLimit == wallClockLimitExpected


@pytest.mark.parametrize("runCommandResult, sourceEnvResult, lsbHosts, lsfEnvdir, cpuLimitExpected, \
                         wallClockLimitExpected, normrefExpected, hostnormExpected, cpuRef", [
    ((S_OK(LSF_CERN_BQUEUES), S_ERROR(LSF_CERN_LSHOSTS_1), S_OK(LSF_CERN_LSINFO), S_OK(LSF_LSHOSTS)),
     S_ERROR("no lsf.sh"),
     'b66', '/dev/null', 241920, 725760, 1.0, 2.5, 'KSI2K')
])
def test_init_cern(mocker, runCommandResult, sourceEnvResult, lsbHosts, lsfEnvdir, cpuLimitExpected,
                   wallClockLimitExpected, normrefExpected, hostnormExpected, cpuRef):
  mocker.patch("DIRAC.Resources.Computing.BatchSystems.TimeLeft.LSFResourceUsage.runCommand",
               side_effect=runCommandResult)
  mocker.patch("os.path.isfile", return_value=True)
  mocker.patch.dict(os.environ, {'LSB_HOSTS': lsbHosts, 'LSF_ENVDIR': lsfEnvdir})
  mocker.patch("DIRAC.Resources.Computing.BatchSystems.TimeLeft.LSFResourceUsage.sourceEnv",
               return_value=sourceEnvResult)

  lsfResourceUsage = LSFResourceUsage()

  assert lsfResourceUsage.cpuLimit == cpuLimitExpected
  assert lsfResourceUsage.wallClockLimit == wallClockLimitExpected
  assert lsfResourceUsage.cpuRef == cpuRef
  assert lsfResourceUsage.normRef == normrefExpected
  assert lsfResourceUsage.hostNorm == hostnormExpected
