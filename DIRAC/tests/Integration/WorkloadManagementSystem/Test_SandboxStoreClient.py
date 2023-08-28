""" This is a test of using SandboxStoreClient in the WMS

    In order to run this test we need the following DBs installed:
    - SandboxMetadataDB

    And the following services should also be on:
    - SandboxStore

    And a SandboxSE should be configured, something like:
      SandboxStore
      {
        LocalSE = FedericoSandboxSE
        Port = 9196
        BasePath = /home/toffo/Rumenta/
        Authorization
        {
          Default = authenticated
          FileTransfer
          {
            Default = all
          }
        }
      }

    A user proxy is also needed to submit,
    and the Framework/ProxyManager need to be running with a such user proxy already uploaded.

    Suggestion: for local testing, run this with::
        python -m pytest -c ../pytest.ini  -vv tests/Integration/WorkloadManagementSystem/Test_SandboxStoreClient.py
"""

from diraccfg import CFG

import DIRAC

DIRAC.initialize()  # Initialize configuration

from DIRAC import gConfig, gLogger
from DIRAC.tests.Utilities.utils import find_all
from DIRAC.WorkloadManagementSystem.Client.SandboxStoreClient import SandboxStoreClient
from DIRAC.WorkloadManagementSystem.DB.SandboxMetadataDB import SandboxMetadataDB

# cfg = CFG()
# cfg.loadFromBuffer(
#     """
# Systems
# {
#   WorkloadManagement
#   {
#     dirac-JenkinsSetup
#     {
#       Services
#       {
#         SandboxStore
#         {
#           BasePath = /scratch/
#         }
#       }
#     }
#   }
# }
# """
# )
# gConfig.loadCFG(cfg)

gLogger.setLevel("DEBUG")


def test_SSCChain():
    """full test of functionalities"""
    ssc = SandboxStoreClient()

    jobId = 1

    exeScriptLocation = find_all("exe-script.py", "../..", "/DIRAC/tests/Integration")[0]
    fileList = [exeScriptLocation]

    res = ssc.uploadFilesAsSandbox(fileList)
    assert res["OK"], res["Message"]

    res = ssc.uploadFilesAsSandboxForJob(fileList, jobId, "Input")
    assert res["OK"], res["Message"]

    # TODO : FIXME
    # res = ssc.downloadSandboxForJob(jobId, "Input")  # to run this we need the RSS on
    # print(res)  # for debug...
    # assert res["OK"], res["Message"]

    res = ssc.unassignJobs([jobId])
    assert res["OK"], res["Message"]
