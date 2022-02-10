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

import DIRAC

DIRAC.initialize()  # Initialize configuration

from DIRAC.tests.Utilities.utils import find_all

from DIRAC import gLogger

from DIRAC.WorkloadManagementSystem.Client.SandboxStoreClient import SandboxStoreClient
from DIRAC.WorkloadManagementSystem.DB.SandboxMetadataDB import SandboxMetadataDB


gLogger.setLevel("DEBUG")


def test_SSCChain():
    """full test of functionalities"""
    ssc = SandboxStoreClient()
    smDB = SandboxMetadataDB()

    exeScriptLocation = find_all("exe-script.py", "..", "/DIRAC/tests/Integration")[0]
    fileList = [exeScriptLocation]
    res = ssc.uploadFilesAsSandbox(fileList)
    assert res["OK"] is True, res["Message"]
    #     SEPFN = res['Value'].split( '|' )[1]
    res = ssc.uploadFilesAsSandboxForJob(fileList, 1, "Input")
    assert res["OK"] is True, res["Message"]
    res = ssc.downloadSandboxForJob(1, "Input")  # to run this we need the RSS on
    print(res)  # for debug...
    assert res["OK"] is True, res["Message"]

    # only ones needing the DB
    res = smDB.getUnusedSandboxes()
    print(res)
    assert res["OK"] is True, res["Message"]
    # smDB.getSandboxId(SEName, SEPFN, requesterName, requesterGroup)
    # # cleaning
    # res = smDB.deleteSandboxes(SBIdList)
    # assert res['OK'] is True


def test_SandboxMetadataDB():
    smDB = SandboxMetadataDB()

    seNameToUse = "ProductionSandboxSE"
    sbPath = "/sb/pfn/1.tar.bz2"
    assignTo = {"adminusername": "dirac_admin"}

    res = smDB.registerAndGetSandbox(
        "adminusername", "/C=ch/O=DIRAC/OU=DIRAC CI/CN=ciuser", "dirac_admin", seNameToUse, sbPath, 123
    )
    assert res["OK"], res["Message"]

    sbURL = f"SB:{seNameToUse}|{sbPath}"
    assignTo = dict([(key, [(sbURL, assignTo[key])]) for key in assignTo])
    res = smDB.assignSandboxesToEntities(assignTo, "adminusername", "dirac_admin", "enSetup")
    assert res["OK"], res["Message"]
