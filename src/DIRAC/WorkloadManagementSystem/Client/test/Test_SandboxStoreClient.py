"""Tests for the SandboxStoreClient when using DiracX"""
# pylint: disable=missing-docstring,redefined-outer-name,unsubscriptable-object

import hashlib
import io
import tarfile
from http import HTTPStatus
from unittest.mock import MagicMock

import pytest
import requests
from azure.core.exceptions import HttpResponseError

from DIRAC.WorkloadManagementSystem.Client import SandboxStoreClient as legacyModule
from DIRAC.WorkloadManagementSystem.FutureClient import SandboxStoreClient as futureModule
from DIRAC.WorkloadManagementSystem.FutureClient.SandboxStoreClient import SandboxStoreClient


def makeTarball(content=b"hello"):
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:bz2") as tf:
        tarInfo = tarfile.TarInfo(name="hello.txt")
        tarInfo.size = len(content)
        tf.addfile(tarInfo, io.BytesIO(content))
    return buffer.getvalue()


TARBALL = makeTarball()
CHECKSUM = hashlib.sha256(TARBALL).hexdigest()
PFN = f"/S3/sandboxes/vo/vo_user/someone/sha256:{CHECKSUM}.tar.bz2"


def fakeResponse(status=200, content=b""):
    response = MagicMock()
    response.status_code = status
    response.iter_content.return_value = [content[i : i + 3] for i in range(0, len(content), 3)]
    response.__enter__.return_value = response
    if status >= 400:
        response.raise_for_status.side_effect = requests.HTTPError(response=response)
    return response


@pytest.fixture
def api(monkeypatch):
    api = MagicMock()
    api.jobs.get_sandbox_file.return_value = MagicMock(url="https://s3.invalid/get")

    class FakeDiracXClient:
        def __enter__(self):
            return api

        def __exit__(self, *args):
            pass

    monkeypatch.setattr(futureModule, "DiracXClient", FakeDiracXClient)
    monkeypatch.setattr(futureModule.time, "sleep", MagicMock())
    return api


def test_download(api, monkeypatch, tmp_path):
    get = MagicMock(return_value=fakeResponse(content=TARBALL))
    monkeypatch.setattr(futureModule.requests, "get", get)

    res = SandboxStoreClient().downloadSandbox(PFN, str(tmp_path / "sb.tar.bz2"))
    assert res["OK"], res
    assert (tmp_path / "sb.tar.bz2").read_bytes() == TARBALL
    api.jobs.get_sandbox_file.assert_called_once_with(pfn=PFN)
    get.assert_called_once()


def test_download_retries(api, monkeypatch, tmp_path):
    responses = [fakeResponse(status=503), fakeResponse(content=b"truncated"), fakeResponse(content=TARBALL)]
    monkeypatch.setattr(futureModule.requests, "get", MagicMock(side_effect=responses))

    res = SandboxStoreClient().downloadSandbox(PFN, str(tmp_path / "sb.tar.bz2"))
    assert res["OK"], res
    assert (tmp_path / "sb.tar.bz2").read_bytes() == TARBALL
    # A fresh presigned URL is requested for each attempt
    assert api.jobs.get_sandbox_file.call_count == 3


def test_download_gives_up(api, monkeypatch, tmp_path):
    get = MagicMock(side_effect=lambda *a, **kw: fakeResponse(status=503))
    monkeypatch.setattr(futureModule.requests, "get", get)

    res = SandboxStoreClient().downloadSandbox(PFN, str(tmp_path / "sb.tar.bz2"))
    assert not res["OK"]
    assert get.call_count == futureModule.MAX_ATTEMPTS


def test_download_not_retryable(api, monkeypatch, tmp_path):
    get = MagicMock(return_value=fakeResponse(status=404))
    monkeypatch.setattr(futureModule.requests, "get", get)

    res = SandboxStoreClient().downloadSandbox(PFN, str(tmp_path / "sb.tar.bz2"))
    assert not res["OK"]
    get.assert_called_once()


def test_download_invalid_pfn(api):
    res = SandboxStoreClient().downloadSandbox("/SandBox/s/someone.vo_user/abc/def/abcdef.tar.bz2", "/dev/null")
    assert not res["OK"]
    api.jobs.get_sandbox_file.assert_not_called()


@pytest.fixture
def tarballPath(tmp_path):
    path = tmp_path / "upload.tar.bz2"
    path.write_bytes(TARBALL)
    return str(path)


def test_upload(api, monkeypatch, tarballPath):
    api.jobs.initiate_sandbox_upload.return_value = MagicMock(
        url="https://s3.invalid/post", fields={"key": "value"}, pfn=f"SB:SandboxSE|{PFN}"
    )
    post = MagicMock(return_value=fakeResponse())
    monkeypatch.setattr(futureModule.requests, "post", post)

    res = SandboxStoreClient().uploadSandbox(tarballPath, {"Job:123": "Output"})
    assert res["OK"], res
    assert res["Value"] == f"SB:SandboxSE|{PFN}"

    sandboxInfo = api.jobs.initiate_sandbox_upload.call_args.args[0]
    assert sandboxInfo.checksum == CHECKSUM
    assert sandboxInfo.size == len(TARBALL)
    assert sandboxInfo.format == "tar.bz2"
    assert post.call_args.kwargs["data"] == {"key": "value"}
    api.jobs.assign_sandbox_to_job.assert_called_once_with(123, PFN)


def test_upload_existing(api, monkeypatch, tarballPath):
    api.jobs.initiate_sandbox_upload.return_value = MagicMock(url=None, pfn=f"SB:SandboxSE|{PFN}")
    post = MagicMock()
    monkeypatch.setattr(futureModule.requests, "post", post)

    res = SandboxStoreClient().uploadSandbox(tarballPath, {})
    assert res["OK"], res
    post.assert_not_called()
    api.jobs.assign_sandbox_to_job.assert_not_called()


def test_upload_already_assigned(api, monkeypatch, tarballPath):
    api.jobs.initiate_sandbox_upload.return_value = MagicMock(url=None, pfn=f"SB:SandboxSE|{PFN}")
    error = HttpResponseError(message="Sandbox already assigned")
    error.status_code = HTTPStatus.BAD_REQUEST
    api.jobs.assign_sandbox_to_job.side_effect = error

    res = SandboxStoreClient().uploadSandbox(tarballPath, {"Job:123": "Output"})
    assert res["OK"], res


def test_upload_too_big(api, monkeypatch, tmp_path):
    path = tmp_path / "big.tar.bz2"
    path.write_bytes(b"0" * (11 * 1024 * 1024))

    res = SandboxStoreClient().uploadSandbox(str(path), {})
    assert not res["OK"]
    assert "too big" in res["Message"]
    api.jobs.initiate_sandbox_upload.assert_not_called()


def test_upload_input_assignment(api, tarballPath):
    res = SandboxStoreClient().uploadSandbox(tarballPath, {"Job:123": "Input"})
    assert not res["OK"]
    api.jobs.initiate_sandbox_upload.assert_not_called()


def test_getSandboxesAssignedToJob(api):
    api.jobs.get_job_sandboxes.return_value = {"Input": [PFN], "Output": [None]}

    res = SandboxStoreClient().getSandboxesAssignedToJob(123)
    assert res["OK"], res
    assert res["Value"] == {"Input": [f"SB:SandboxSE|{PFN}"]}


def test_getSandboxesAssignedToJob_legacy(api):
    legacyPFN = "/SandBox/s/someone.vo_user/abc/def/abcdef.tar.bz2"
    api.jobs.get_job_sandboxes.return_value = {"Input": [legacyPFN], "Output": []}

    res = SandboxStoreClient().getSandboxesAssignedToJob(123)
    assert res["OK"], res
    assert res["Value"] == {"Input": [f"SB:ProductionSandboxSE|{legacyPFN}"]}


@pytest.fixture
def legacyClient(monkeypatch):
    monkeypatch.setattr(legacyModule, "useLegacyAdapter", lambda *args: True)
    futureClient = MagicMock()
    monkeypatch.setattr(legacyModule, "futureSandboxStoreClient", MagicMock(return_value=futureClient))
    storageElement = MagicMock()
    monkeypatch.setattr(legacyModule, "StorageElement", storageElement)
    return legacyModule.SandboxStoreClient(), futureClient, storageElement


def test_legacy_download_s3(legacyClient, tmp_path):
    client, futureClient, storageElement = legacyClient

    def download(pfn, destination):
        with open(destination, "wb") as fh:
            fh.write(TARBALL)
        return {"OK": True, "Value": None}

    futureClient.downloadSandbox.side_effect = download

    res = client.downloadSandbox(f"SB:SandboxSE|{PFN}", destinationDir=str(tmp_path))
    assert res["OK"], res
    assert (tmp_path / "hello.txt").read_bytes() == b"hello"
    storageElement.assert_not_called()


def test_legacy_download_non_s3(legacyClient, tmp_path):
    client, futureClient, storageElement = legacyClient
    storageElement.return_value.getFile.return_value = {"OK": True, "Value": {"Successful": {}, "Failed": {"x": "y"}}}

    res = client.downloadSandbox("SB:ProductionSandboxSE|/SandBox/abcdef.tar.bz2", destinationDir=str(tmp_path))
    assert not res["OK"]
    storageElement.assert_called_once()
    futureClient.downloadSandbox.assert_not_called()


def test_legacy_upload(legacyClient, tmp_path):
    client, futureClient, _ = legacyClient
    futureClient.uploadSandbox.return_value = {"OK": True, "Value": f"SB:SandboxSE|{PFN}"}
    (tmp_path / "file.txt").write_text("content")

    res = client.uploadFilesAsSandbox([str(tmp_path / "file.txt")], assignTo={"Job:123": "Output"})
    assert res["OK"], res
    assert res["Value"] == f"SB:SandboxSE|{PFN}"
    assert futureClient.uploadSandbox.call_args.args[1] == {"Job:123": "Output"}
