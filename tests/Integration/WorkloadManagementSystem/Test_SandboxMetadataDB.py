# pylint: disable=missing-docstring, wrong-import-position

import DIRAC

DIRAC.initialize()  # Initialize configuration

from DIRAC import gLogger
from DIRAC.WorkloadManagementSystem.DB.SandboxMetadataDB import SandboxMetadataDB


gLogger.setLevel("DEBUG")


def test_SandboxMetadataDB():
    smDB = SandboxMetadataDB()

    owner = "adminusername"
    ownerDN = "/C=ch/O=DIRAC/OU=DIRAC CI/CN=ciuser"
    ownerGroup = "dirac_admin"

    sbSE = "ProductionSandboxSE"
    sbPFN = "/sb/pfn/1.tar.bz2"

    res = smDB.registerAndGetSandbox(owner, ownerGroup, sbSE, sbPFN, 123)
    assert res["OK"], res["Message"]
    sbId, newSandbox = res["Value"]
    print(f"sbId:{sbId}")
    print(f"newSandbox:{newSandbox}")

    assignTo = {owner: [(f"SB:{sbSE}|{sbPFN}", ownerGroup)]}
    res = smDB.assignSandboxesToEntities(assignTo, owner, ownerGroup)
    assert res["OK"], res["Message"]
    assert res["Value"] == 1

    res = smDB.getSandboxOwner(sbSE, sbPFN, ownerDN, ownerGroup)
    assert res["OK"], res["Message"]
    assert res["Value"] == (owner, ownerGroup)

    res = smDB.getSandboxId(sbSE, sbPFN, owner, ownerGroup)
    assert res["OK"], res["Message"]
    assert res["Value"] == sbId

    res = smDB.accessedSandboxById(sbId)
    assert res["OK"], res["Message"]

    res = smDB.deleteSandboxes([sbId])
    assert res["OK"], res["Message"]

    res = smDB.getUnusedSandboxes()
    assert res["OK"], res["Message"]
