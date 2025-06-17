"""Test for WMS clients."""
# pylint: disable=protected-access, missing-docstring, invalid-name

from pathlib import Path
import shutil
import pytest

from unittest.mock import MagicMock

from DIRAC import S_OK, S_ERROR
from DIRAC.WorkloadManagementSystem.Client.DownloadInputData import DownloadInputData

MODULE_NAME = "DIRAC.WorkloadManagementSystem.Client.DownloadInputData"

# pylint: disable=redefined-outer-name, unused-argument


@pytest.fixture(autouse=True)
def setUpLogger():
    from DIRAC import gLogger

    gLogger.setLevel("DEBUG")


@pytest.fixture
def mockSE(mocker):
    mockObjectSE = MagicMock()
    mockObjectSE.getFileMetadata.return_value = S_OK(
        {"Successful": {"/a/lfn/1.txt": {"Cached": 1, "Accessible": 1}}, "Failed": {}}
    )
    mockObjectSE.getFile.return_value = S_OK({"Successful": {"/a/lfn/1.txt": {}}, "Failed": {}})
    mockObjectSE.getStatus.return_value = S_OK({"Read": True, "DiskSE": True})
    mockObjectSE.status.return_value = {"Read": True, "DiskSE": True}

    theMockSE = MagicMock()
    theMockSE.return_value = mockObjectSE
    mocker.patch(MODULE_NAME + ".StorageElement", new=theMockSE)
    return theMockSE


@pytest.fixture
def dli():
    theDLI = DownloadInputData(
        {
            "InputData": [],
            "Configuration": {"LocalSEList": ["SE_Local"]},
            "InputDataDirectory": "CWD",
            "FileCatalog": S_OK(
                {
                    "Successful": {
                        "/a/lfn/1.txt": {
                            "Size": 10,
                            "GUID": "aGUID",
                            "SE_Local": "",
                            "SE_Remote": "",
                            "SE_Bad": "",
                            "SE_Tape": "",
                        }
                    }
                }
            ),
        }
    )
    theDLI.availableSEs = ["SE_Local", "SE_Remote"]
    return theDLI


@pytest.fixture
def osPathExists(mocker):
    osPathMock = MagicMock(return_value=True)
    mocker.patch(f"{MODULE_NAME}.os.path.exists", new=osPathMock)
    return osPathMock


def test_DLIDownloadFromSE_fail(dli, mockSE, osPathExists):
    osPathExists.return_value = False
    res = dli._downloadFromSE("/a/lfn/1.txt", "mySE", {"mySE": []}, "aGuid")
    assert not res["OK"]


def test_DLIDownloadFromSE_local(dli, mockSE, osPathExists):
    res = dli._downloadFromSE("/a/lfn/1.txt", "mySE", {"mySE": []}, "aGuid")
    assert res["OK"], res
    assert res["Value"]["protocol"] == "LocalData"


def test_DLIDownloadFromSE_realDown(dli, mockSE, osPathExists):
    osPathExists.side_effect = (False, False, True)
    res = dli._downloadFromSE("/a/lfn/1.txt", "mySE", {"mySE": []}, "aGuid")
    assert res["OK"], res.get("Message", "No Error")
    assert res["Value"]["protocol"] == "Downloaded"
    assert "path" in res["Value"]
    assert res["Value"]["path"].endswith("1.txt")


def test_DLIDownloadFromSE_realDown_Fail(dli, mockSE, osPathExists):
    mockObjectSE = mockSE.return_value
    mockObjectSE.getFile.return_value = S_OK({"Successful": {}, "Failed": {"/a/lfn/1.txt": {}}})
    mockObjectSE.getStatus.return_value = S_OK({"Read": True, "DiskSE": True})

    osPathExists.return_value = False
    res = dli._downloadFromSE("/a/lfn/1.txt", "mySE", {"mySE": []}, "aGuid")
    assert not res["OK"], res.get("Message", "No Error")


def test_DLIDownloadFromBestSE_isLocal(dli, mockSE, osPathExists):
    res = dli._downloadFromBestSE("/a/lfn/1.txt", {"mySE": []}, "aGuid")
    assert res["OK"], res.get("Message", "No error")
    assert res["Value"]["protocol"] == "LocalData"


def test_DLIDownloadFromBestSE_Fail(dli, mockSE, osPathExists):
    osPathExists.return_value = False
    res = dli._downloadFromBestSE("/a/lfn/1.txt", {"mySE": []}, "aGuid")
    assert not res["OK"]


def test_DLI_execute(mocker, dli, mockSE):
    mocker.patch("DIRAC.WorkloadManagementSystem.Client.DownloadInputData.gConfig.getValue", return_value=2)
    dli._downloadFromSE = MagicMock(return_value=S_OK({"path": "/local/path/1.txt"}))
    res = dli.execute(dataToResolve=["/a/lfn/1.txt"])
    assert res["OK"]
    assert not res["Value"]["Failed"]
    assert "/a/lfn/1.txt" in res["Value"]["Successful"], res


def test_DLI_execute_getFileMetadata_Fails(mocker, dli, mockSE):
    """When getFileMetadata fails for the first SE, we should fall back to the second."""
    mocker.patch("DIRAC.WorkloadManagementSystem.Client.DownloadInputData.gConfig.getValue", return_value=2)
    mockObjectSE = mockSE.return_value
    mockObjectSE.getFileMetadata.return_value = S_OK(
        {"Successful": {}, "Failed": {"/a/lfn/1.txt": "Error Getting MetaData"}}
    )

    dli._downloadFromSE = MagicMock(return_value=S_OK({"path": "/local/path/1.txt"}))
    dli._isCache = MagicMock(return_value=True)
    res = dli.execute(dataToResolve=["/a/lfn/1.txt"])
    assert res["OK"]
    assert not res["Value"]["Failed"]
    assert "/a/lfn/1.txt" in res["Value"]["Successful"], res
    assert res["Value"]["Successful"]["/a/lfn/1.txt"]["path"] == "/local/path/1.txt", res


def test_DLI_execute_getFileMetadata_Lost(mocker, dli, mockSE):
    """When getFileMetadata fails for the first SE, we should fall back to the second."""
    mocker.patch("DIRAC.WorkloadManagementSystem.Client.DownloadInputData.gConfig.getValue", return_value=2)
    mockObjectSE = mockSE.return_value
    mockObjectSE.getFileMetadata.return_value = S_OK(
        {
            "Successful": {
                "/a/lfn/1.txt": {
                    "Cached": 1,
                    "Accessible": 0,
                    "Lost": True,
                }
            },
            "Failed": {},
        }
    )
    dli._downloadFromSE = MagicMock(return_value=S_OK({"path": "/local/path/1.txt"}))
    dli._isCache = MagicMock(return_value=True)
    res = dli.execute(dataToResolve=["/a/lfn/1.txt"])
    assert res["OK"]
    assert not res["Value"]["Failed"]
    assert "/a/lfn/1.txt" in res["Value"]["Successful"], res
    assert res["Value"]["Successful"]["/a/lfn/1.txt"]["path"] == "/local/path/1.txt", res


def test_DLI_execute_getFileMetadata_Unavailable(mocker, dli, mockSE):
    """When getFileMetadata fails for the first SE, we should fall back to the second."""
    mocker.patch("DIRAC.WorkloadManagementSystem.Client.DownloadInputData.gConfig.getValue", return_value=2)
    mockObjectSE = mockSE.return_value
    mockObjectSE.getFileMetadata.return_value = S_OK(
        {
            "Successful": {
                "/a/lfn/1.txt": {
                    "Cached": 0,
                    "Accessible": 0,
                    "Unavailable": True,
                }
            },
            "Failed": {},
        }
    )
    dli._downloadFromSE = MagicMock(return_value=S_OK({"path": "/local/path/1.txt"}))
    dli._isCache = MagicMock(return_value=True)
    res = dli.execute(dataToResolve=["/a/lfn/1.txt"])
    assert res["OK"]
    assert not res["Value"]["Failed"]
    assert "/a/lfn/1.txt" in res["Value"]["Successful"], res
    assert res["Value"]["Successful"]["/a/lfn/1.txt"]["path"] == "/local/path/1.txt", res


def test_DLI_execute_getFileMetadata_Cached(mocker, dli, mockSE):
    """When getFileMetadata fails for the first SE, we should fall back to the second."""
    mocker.patch("DIRAC.WorkloadManagementSystem.Client.DownloadInputData.gConfig.getValue", return_value=2)
    mockObjectSE = mockSE.return_value
    mockObjectSE.getFileMetadata.return_value = S_OK(
        {
            "Successful": {
                "/a/lfn/1.txt": {
                    "Cached": 0,
                    "Accessible": 0,
                    "Unavailable": False,
                }
            },
            "Failed": {},
        }
    )
    dli._downloadFromSE = MagicMock(return_value=S_OK({"path": "/local/path/1.txt"}))
    dli._isCache = MagicMock(return_value=True)
    res = dli.execute(dataToResolve=["/a/lfn/1.txt"])
    assert res["OK"]
    assert not res["Value"]["Failed"]
    assert "/a/lfn/1.txt" in res["Value"]["Successful"], res
    assert res["Value"]["Successful"]["/a/lfn/1.txt"]["path"] == "/local/path/1.txt", res


def test_DLI_execute_FirstDownFailed(mocker, dli, mockSE):
    """When getFileMetadata fails for the first SE, we should fall back to the second."""
    mocker.patch("DIRAC.WorkloadManagementSystem.Client.DownloadInputData.gConfig.getValue", return_value=2)
    mockObjectSE = mockSE.return_value
    mockObjectSE.getFileMetadata.return_value = S_OK(
        {"Successful": {"/a/lfn/1.txt": {"Cached": 1, "Accessible": 0}}, "Failed": {}}
    )
    dli._downloadFromSE = MagicMock(side_effect=[S_ERROR("Failed to down"), S_OK({"path": "/local/path/1.txt"})])
    dli._isCache = MagicMock(return_value=True)
    res = dli.execute(dataToResolve=["/a/lfn/1.txt"])
    assert res["OK"]
    assert not res["Value"]["Failed"]
    assert "/a/lfn/1.txt" in res["Value"]["Successful"], res
    assert res["Value"]["Successful"]["/a/lfn/1.txt"]["path"] == "/local/path/1.txt", res


def test_DLI_execute_AllDownFailed(mocker, dli, mockSE):
    """When getFileMetadata fails for the first SE, we should fall back to the second."""
    mocker.patch("DIRAC.WorkloadManagementSystem.Client.DownloadInputData.gConfig.getValue", return_value=2)
    mockObjectSE = mockSE.return_value
    mockObjectSE.getFileMetadata.return_value = S_OK(
        {"Successful": {"/a/lfn/1.txt": {"Cached": 1, "Accessible": 0}}, "Failed": {}}
    )
    dli._downloadFromSE = MagicMock(return_value=S_ERROR("Failed to down"))
    dli._isCache = MagicMock(return_value=True)
    res = dli.execute(dataToResolve=["/a/lfn/1.txt"])
    assert res["OK"]
    assert res["Value"]["Failed"]
    assert "/a/lfn/1.txt" in res["Value"]["Failed"], res
    assert res["Value"]["Failed"][0] == "/a/lfn/1.txt", res


def test_DLI_execute_NoLocal(mocker, dli, mockSE):
    """Data only at the remote SE."""
    mocker.patch("DIRAC.WorkloadManagementSystem.Client.DownloadInputData.gConfig.getValue", return_value=2)
    dli = DownloadInputData(
        {
            "InputData": [],
            "Configuration": {"LocalSEList": ["SE_Local", "SE_Tape"]},
            "InputDataDirectory": "CWD",
            "FileCatalog": S_OK(
                {
                    "Successful": {
                        "/a/lfn/1.txt": {
                            "Size": 10,
                            "GUID": "aGUID",
                            "SE_Tape": "",
                        }
                    }
                }
            ),
        }
    )
    dli.availableSEs = ["SE_Local", "SE_Remote", "SE_Tape"]
    mockObjectSE = mockSE.return_value
    statDict = {"Read": True, "TapeSE": True, "DiskSE": False}
    mockObjectSE.getStatus.return_value = S_OK(statDict)
    mockObjectSE.status.return_value = statDict
    dli._downloadFromSE = MagicMock(return_value=S_ERROR("Failed to down"))
    dli._isCache = MagicMock(return_value=True)

    res = dli.execute(dataToResolve=["/a/lfn/1.txt"])
    assert res["OK"]
    assert res["Value"]["Failed"]
    assert "/a/lfn/1.txt" in res["Value"]["Failed"], res
    assert res["Value"]["Failed"][0] == "/a/lfn/1.txt", res


def test_DLI_execute_jobIDPath(mocker, dli, mockSE):
    """Specify a jobIDPath. Output should be in the jobIDPath directory."""
    mocker.patch("DIRAC.WorkloadManagementSystem.Client.DownloadInputData.gConfig.getValue", return_value=2)

    # Create the jobIDPath directory
    jobIDPath = Path().cwd() / "job" / "12345"
    jobIDPath.mkdir(parents=True, exist_ok=True)

    dli.configuration["JobIDPath"] = str(jobIDPath)

    mocker.patch("DIRAC.WorkloadManagementSystem.Client.DownloadInputData.gConfig.getValue", return_value=2)
    mockObjectSE = mockSE.return_value
    mockObjectSE.getFileMetadata.return_value = S_OK(
        {"Successful": {"/a/lfn/1.txt": {"Cached": 1, "Accessible": 0}}, "Failed": {}}
    )
    dli._downloadFromSE = MagicMock(side_effect=[S_ERROR("Failed to down"), S_OK({"path": jobIDPath / "1.txt"})])
    dli._isCache = MagicMock(return_value=True)
    res = dli.execute(dataToResolve=["/a/lfn/1.txt"])

    assert res["OK"]
    assert not res["Value"]["Failed"]
    assert "/a/lfn/1.txt" in res["Value"]["Successful"], res

    # Check that the output path is in the jobIDPath directory
    assert res["Value"]["Successful"]["/a/lfn/1.txt"]["path"] == jobIDPath / "1.txt", res
