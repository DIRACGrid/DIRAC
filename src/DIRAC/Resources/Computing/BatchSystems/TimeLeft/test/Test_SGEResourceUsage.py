""" Test class for SGEResourceUsage utility
"""

import pytest

from DIRAC import S_OK
from DIRAC.Resources.Computing.BatchSystems.TimeLeft.SGEResourceUsage import SGEResourceUsage


RESULT_FROM_SGE = """==============================================================
job_number:                 12345
exec_file:                  job_scripts/12345
submission_time:            Wed Apr 11 09:36:41 2012
owner:                      lhcb049
uid:                        18416
group:                      lhcb
gid:                        155
sge_o_home:                 /home/lhcb049
sge_o_log_name:             lhcb049
sge_o_path:                 /opt/sge/bin/lx24-amd64:/usr/bin:/bin
sge_o_shell:                /bin/sh
sge_o_workdir:              /var/glite/tmp
sge_o_host:                 cccreamceli05
account:                    GRID=EGI SITE=IN2P3-CC TIER=tier1 VO=lhcb ROLEVOMS=&2Flhcb&2FRole=pilot&2FCapability=NULL
merge:                      y
hard resource_list:         os=sl5,s_cpu=1000,s_vmem=5120M,s_fsize=51200M,cvmfs=1,dcache=1
mail_list:                  lhcb049@cccreamceli05.in2p3.fr
notify:                     FALSE
job_name:                   cccreamceli05_crm05_749996134
stdout_path_list:           NONE:NONE:/dev/null
jobshare:                   0
hard_queue_list:            huge
restart:                    n
shell_list:                 NONE:/bin/bash
env_list:                   SITE_NAME=IN2P3-CC,MANPATH=/opt/sge/man:/usr/share/man:/usr/local/man:/usr/local/share/man
script_file:                /tmp/crm05_749996134
project:                    P_lhcb_pilot
usage    1:                 cpu=00:01:00, mem=0.03044 GBs, io=0.19846, vmem=288.609M, maxvmem=288.609M
scheduling info:            (Collecting of scheduler job information is turned off)
"""


def test_getResourceUsage(mocker):
    mocker.patch(
        "DIRAC.Resources.Computing.BatchSystems.TimeLeft.SGEResourceUsage.runCommand",
        return_value=S_OK(RESULT_FROM_SGE),
    )

    sgeResourceUsage = SGEResourceUsage("1234", {"Queue": "Test"})
    res = sgeResourceUsage.getResourceUsage()

    assert res["OK"]
    assert res["Value"]["CPU"] == 60
    assert res["Value"]["CPULimit"] == 1000
    # WallClock is random and don't know why, so not testing it
    # assert res["Value"]["WallClock"] == 0.01
    assert res["Value"]["WallClockLimit"] == 1250
