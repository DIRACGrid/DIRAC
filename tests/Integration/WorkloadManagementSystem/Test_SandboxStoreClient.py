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

from DIRAC import gLogger
from DIRAC.tests.Utilities.utils import find_all
from DIRAC.WorkloadManagementSystem.Client.SandboxStoreClient import SandboxStoreClient

gLogger.setLevel("DEBUG")


def test_SSCChain():
    """full test of functionalities"""
    ssc = SandboxStoreClient()

    exeScriptLocation = find_all("exe-script.py", "../..", "/DIRAC/tests/Integration")[0]
    fileList = [exeScriptLocation]

    res = ssc.uploadFilesAsSandbox(fileList)
    assert res["OK"], res["Message"]
