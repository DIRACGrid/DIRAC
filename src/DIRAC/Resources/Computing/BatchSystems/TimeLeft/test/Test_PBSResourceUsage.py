""" Test class for LSFResourceUsage utility
"""

import os
import pytest

from DIRAC import S_OK, gLogger

from DIRAC.Resources.Computing.BatchSystems.TimeLeft.PBSResourceUsage import PBSResourceUsage

gLogger.setLevel("DEBUG")

RRCKI_OUT = """
Job Id: 55755440.seer.t1.grid.kiae.ru
    Job_Name = igyLDmzixXznYTGZXoIFMvcqIyasxmGoZvymfcHKDmABFKDmVYviIm
    Job_Owner = lhcbpilot0000@calc1.t1.grid.kiae.ru
    resources_used.cput = 43:02:47
    resources_used.mem = 1611016kb
    resources_used.vmem = 3636260kb
    resources_used.walltime = 43:22:30
    job_state = R
    queue = lhcb
    server = seer.t1.grid.kiae.ru
    Checkpoint = u
    ctime = Wed Aug 11 10:20:59 2021
    Error_Path = calc1.t1.grid.kiae.ru:/shared/sandbox/04/igyLDmzixXznYTGZXoIF
        MvcqIyasxmGoZvymfcHKDmABFKDmVYviIm.comment
    exec_host = n175.t1.grid.kiae.ru/17
    Hold_Types = n
    Join_Path = eo
    Keep_Files = n
    Mail_Points = n
    mtime = Wed Aug 11 17:13:13 2021
    Output_Path = calc1.t1.grid.kiae.ru:/shared/sandbox/04/igyLDmzixXznYTGZXoI
        FMvcqIyasxmGoZvymfcHKDmABFKDmVYviIm/igyLDmzixXznYTGZXoIFMvcqIyasxmGoZv
        ymfcHKDmABFKDmVYviIm.o55755440
    Priority = -1
    qtime = Wed Aug 11 10:20:59 2021
    Rerunable = False
    Resource_List.cput = 100:00:00
    Resource_List.nice = 10
    Resource_List.nodect = 1
    Resource_List.nodes = 1
    Resource_List.walltime = 120:00:00
    session_id = 20422
    Shell_Path_List = /bin/bash
    stagein = HOME@calc1.t1.grid.kiae.ru:/shared/sandbox/04/igyLDmzixXznYTG
        ZXoIFMvcqIyasxmGoZvymfcHKDmABFKDmVYviIm
    stageout = HOME@igyLDmzixXznYTGZXoIFMvcqIyasxmGoZvymfcHKDmABFKDmVYviIm@
        calc1.t1.grid.kiae.ru:/shared/sandbox/04
    Variable_List = PBS_O_QUEUE=lhcb,PBS_O_HOST=calc1.t1.grid.kiae.ru,
        PBS_O_HOME=/,PBS_O_LANG=en_US.UTF-8,
        PBS_O_PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin,
        PBS_SERVER=seer.t1.grid.kiae.ru,
        PBS_O_WORKDIR=/shared/sandbox/04/igyLDmzixXznYTGZXoIFMvcqIyasxmGoZvym
        fcHKDmABFKDmVYviIm
    etime = Wed Aug 11 10:20:59 2021
    submit_args = -r n -S /bin/bash -m n
    start_time = Wed Aug 11 17:13:12 2021
    Walltime.Remaining = 275789
    start_count = 1
    fault_tolerant = False
    submit_host = calc1.t1.grid.kiae.ru
    init_work_dir = /shared/sandbox/04/igyLDmzixXznYTGZXoIFMvcqIyasxmGoZvymfcH
        KDmABFKDmVYviIm
    resource_limits.pvmem = 6gb
    resource_limits.vmem = 8gb
    kill_gracetime = 60000
    min_nice = 19
"""


def test_getResourcUsage(mocker):
    mocker.patch(
        "DIRAC.Resources.Computing.BatchSystems.TimeLeft.PBSResourceUsage.runCommand",
        side_effect=[S_OK(RRCKI_OUT)],
    )
    mocker.patch("os.path.isfile", return_value=True)

    pbsRU = PBSResourceUsage("55755440.seer.t1.grid.kiae.ru", {"Queue": "lhcb", "BinaryPath": "/some/path"})
    res = pbsRU.getResourceUsage()
    assert res["OK"], res["Message"]
    assert len(res["Value"]) == 4
    assert res["Value"]["CPU"] == 154967.0  # pylint: disable=invalid-sequence-index
    assert res["Value"]["WallClock"] == 156150.0  # pylint: disable=invalid-sequence-index
