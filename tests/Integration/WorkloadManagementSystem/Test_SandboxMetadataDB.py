# pylint: disable=missing-docstring, wrong-import-position

import DIRAC

DIRAC.initialize(require_auth=False, host_credentials=True)  # Initialize configuration

from DIRAC import gLogger
from DIRAC.WorkloadManagementSystem.DB.SandboxMetadataDB import SandboxMetadataDB


gLogger.setLevel("DEBUG")


def test_SandboxMetadataDB():
    smDB = SandboxMetadataDB()

    owner = "adminusername"
    ownerGroup = "dirac_admin"
    VO = "vo"

    sbSE = "ProductionSandboxSE"
    sbPFN = "/sb/pfn/1.tar.bz2"

    res = smDB.registerAndGetSandbox(owner, ownerGroup, VO, sbSE, sbPFN, 123)
    assert res["OK"], res["Message"]
    sbId, newSandbox = res["Value"]
    print(f"sbId:{sbId}")
    print(f"newSandbox:{newSandbox}")

    assignTo = {owner: [(f"SB:{sbSE}|{sbPFN}", ownerGroup)]}
    res = smDB.assignSandboxesToEntities(assignTo, owner, ownerGroup)
    assert res["OK"], res["Message"]
    assert res["Value"] == 1

    res = smDB.getSandboxId(sbSE, sbPFN, owner, ownerGroup)
    assert res["OK"], res["Message"]
    assert res["Value"] == sbId

    res = smDB.accessedSandboxById(sbId)
    assert res["OK"], res["Message"]

    res = smDB.deleteSandboxes([sbId])
    assert res["OK"], res["Message"]

    res = smDB.getUnusedSandboxes()
    assert res["OK"], res["Message"]


def test_SandboxMetadataDB_transformationAssignment():
    smDB = SandboxMetadataDB()

    owner = "adminusername"
    ownerGroup = "dirac_admin"
    VO = "vo"

    sbSE = "ProductionSandboxSE"
    sbPFN = "/sb/pfn/trans1.tar.bz2"

    res = smDB.registerAndGetSandbox(owner, ownerGroup, VO, sbSE, sbPFN, 123)
    assert res["OK"], res["Message"]
    sbId, _newSandbox = res["Value"]

    entityId = "Transformation:999"
    assignTo = {entityId: [(f"SB:{sbSE}|{sbPFN}", "Input")]}

    res = smDB.assignSandboxesToEntities(assignTo, owner, ownerGroup)
    assert res["OK"], res["Message"]
    assert res["Value"] == 1

    # While the mapping exists the sandbox is reachable via the transformation entity
    res = smDB.getSandboxesAssignedToEntity(entityId, owner, ownerGroup, VO)
    assert res["OK"], res["Message"]
    assert (sbSE, sbPFN, "Input") in res["Value"]

    # Removing the mapping leaves no rows for that entity
    res = smDB.unassignEntities([entityId])
    assert res["OK"], res["Message"]

    res = smDB.getSandboxesAssignedToEntity(entityId, owner, ownerGroup, VO)
    assert res["OK"], res["Message"]
    assert not res["Value"]

    # cleanup
    res = smDB.deleteSandboxes([sbId])
    assert res["OK"], res["Message"]
