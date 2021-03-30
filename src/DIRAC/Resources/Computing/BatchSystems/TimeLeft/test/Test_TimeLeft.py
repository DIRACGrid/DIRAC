""" Test TimeLeft utility

    (Partially) tested here are SGE and LSF, PBS is TO-DO
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import pytest
from six.moves import reload_module

from DIRAC import S_OK, gLogger
from DIRAC.Resources.Computing.BatchSystems.TimeLeft.TimeLeft import TimeLeft


gLogger.setLevel('DEBUG')

SGE_OUT = """==============================================================
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
scheduling info:            (Collecting of scheduler job information is turned off)"""

PBS_OUT = "bla"

LSF_OUT = "JOBID     USER    STAT  QUEUE      FROM_HOST   EXEC_HOST   JOB_NAME   SUBMIT_TIME  PROJ_NAME CPU_USED MEM"\
          " SWAP PIDS START_TIME FINISH_TIME\n"\
          "12345 user RUN   q1  host1 p01 job1 12/31-20:51:42 default"\
          "    00:00:60.00 6267 40713 25469,14249 12/31-20:52:00 -"

MJF_OUT = "0"

SLURM_OUT_0 = "12345,86400,24,3600,03:00:00"
SLURM_OUT_1 = "12345,86400,24,3600,4-03:00:00"
SLURM_OUT_2 = "12345,21600,24,900,30:00"
SLURM_OUT_3 = "12345,43200,24,1800,30:00"
SLURM_OUT_4 = ""

HTCONDOR_OUT_0 = "86400 3600"
HTCONDOR_OUT_1 = "undefined 3600"
HTCONDOR_OUT_2 = ""


@pytest.mark.parametrize("batch, requiredVariables, returnValue, expected", [
    ('LSF', {}, LSF_OUT, 0.0),
    ('LSF', {'bin': '/usr/bin', 'hostNorm': 10.0}, LSF_OUT, 0.0),
    ('MJF', {}, MJF_OUT, 0.0),
    ('SGE', {}, SGE_OUT, 300.0),
    ('SLURM', {}, SLURM_OUT_0, 432000.0),
    ('SLURM', {}, SLURM_OUT_1, 432000.0),
    ('SLURM', {}, SLURM_OUT_2, 108000.0),
    ('SLURM', {}, SLURM_OUT_3, 216000.0),
    ('SLURM', {}, SLURM_OUT_4, 0.0),
    ('HTCondor', {}, HTCONDOR_OUT_0, 18000.0),
    ('HTCondor', {}, HTCONDOR_OUT_1, 0.0),
    ('HTCondor', {}, HTCONDOR_OUT_2, 0.0)
])
def test_getScaledCPU(mocker, batch, requiredVariables, returnValue, expected):
  """ Test getScaledCPU()
  """
  mocker.patch("DIRAC.Resources.Computing.BatchSystems.TimeLeft.TimeLeft.runCommand", return_value=S_OK(returnValue))
  tl = TimeLeft()
  res = tl.getScaledCPU()
  assert res == 0

  tl.scaleFactor = 5.0
  tl.normFactor = 5.0

  batchSystemName = '%sResourceUsage' % batch
  batchSystemPath = 'DIRAC.Resources.Computing.BatchSystems.TimeLeft.%s' % batchSystemName
  batchPlugin = __import__(batchSystemPath, globals(), locals(), [batchSystemName])  # pylint: disable=unused-variable
  # Need to be reloaded to update the mock within the module, else, it will reuse the one when loaded the first time
  reload_module(batchPlugin)

  batchStr = 'batchPlugin.%s()' % (batchSystemName)
  tl.batchPlugin = eval(batchStr)

  # Update attributes of the batch systems to get scaled CPU
  tl.batchPlugin.__dict__.update(requiredVariables)

  res = tl.getScaledCPU()
  assert res == expected


@pytest.mark.parametrize("batch, requiredVariables, returnValue, expected_1, expected_2", [
    ('LSF', {'bin': '/usr/bin', 'hostNorm': 10.0, 'cpuLimit': 1000, 'wallClockLimit': 1000}, LSF_OUT, True, 9400.0),
    ('SGE', {}, SGE_OUT, True, 9400.0),
    ('SLURM', {}, SLURM_OUT_0, True, 72000.0),
    ('SLURM', {}, SLURM_OUT_1, True, 3528000.0),
    ('SLURM', {}, SLURM_OUT_2, True, 9000.0),
    ('SLURM', {}, SLURM_OUT_3, True, 0.0),
    ('SLURM', {}, SLURM_OUT_4, False, 0.0),
    ('HTCondor', {}, HTCONDOR_OUT_0, True, 828000),
    ('HTCondor', {}, HTCONDOR_OUT_1, False, 0.0),
    ('HTCondor', {}, HTCONDOR_OUT_2, False, 0.0)
])
def test_getTimeLeft(mocker, batch, requiredVariables, returnValue, expected_1, expected_2):
  """ Test getTimeLeft()
  """
  mocker.patch("DIRAC.Resources.Computing.BatchSystems.TimeLeft.TimeLeft.runCommand", return_value=S_OK(returnValue))
  tl = TimeLeft()

  batchSystemName = '%sResourceUsage' % batch
  batchSystemPath = 'DIRAC.Resources.Computing.BatchSystems.TimeLeft.%s' % batchSystemName
  batchPlugin = __import__(batchSystemPath, globals(), locals(), [batchSystemName])
  # Need to be reloaded to update the mock within the module, else, it will reuse the one when loaded the first time
  reload_module(batchPlugin)

  batchStr = 'batchPlugin.%s()' % (batchSystemName)
  tl.batchPlugin = eval(batchStr)
  tl.scaleFactor = 10.0
  tl.normFactor = 10.0

  # Update attributes of the batch systems to get scaled CPU
  tl.batchPlugin.__dict__.update(requiredVariables)

  res = tl.getTimeLeft()
  assert res['OK'] is expected_1
  if res['OK']:
    assert res['Value'] == expected_2
