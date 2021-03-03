""" Test class for SGEResourceUsage utility
"""

import pytest

from DIRAC import S_OK

# sut
from DIRAC.Resources.Computing.BatchSystems.TimeLeft.SGEResourceUsage import SGEResourceUsage

RESULT_FROM_SGE = """
job_number:                 7448711
exec_file:                  job_scripts/7448711
submission_time:            Fri Feb 26 19:56:58 2021
owner:                      pltlhcb001
uid:                        110476
group:                      pltlhcb
gid:                        110013
sge_o_path:                 /usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin
sge_o_host:                 grendel
account:                    sge
merge:                      y
hard resource_list:         decom=FALSE,s_vmem=3994M,h_vmem=3994M
mail_list:                  pltlhcb001@grendel.private.dns.zone
notify:                     FALSE
job_name:                   arc60158374
priority:                   -512
jobshare:                   0
hard_queue_list:            grid7
restart:                    n
shell_list:                 NONE:/bin/sh
env_list:                   TERM=NONE
script_file:                STDIN
parallel environment:  smp range: 8
project:                    grid
binding:                    NONE
job_type:                   NONE
usage         1:            cpu=00:00:23, mem=0.76117 GB s, io=0.08399 GB, vmem=384.875M, maxvmem=384.875M
binding       1:            NONE
scheduling info:            (Collecting of scheduler job information is turned off)
"""


@pytest.mark.parametrize("runCommandResult, cpuLimitExpected, wallClockLimitExpected", [
    ((S_OK(RESULT_FROM_SGE), S_OK('bof')), (720 * 60 / 2.5), (1440 * 60 / 2.5))
])
def test_init(mocker, runCommandResult, cpuLimitExpected, wallClockLimitExpected):
  mocker.patch("DIRAC.Resources.Computing.BatchSystems.TimeLeft.SGEResourceUsage.runCommand",
               side_effect=runCommandResult)

  sgeResourceUsage = SGEResourceUsage()
  res = sgeResourceUsage.getResourceUsage()
  assert not res['OK'], res['Message']
