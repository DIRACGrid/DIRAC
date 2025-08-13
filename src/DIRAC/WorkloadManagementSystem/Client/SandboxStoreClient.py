""" Client for the SandboxStore.
    Will connect to the WorkloadManagement/SandboxStore service.
"""
from __future__ import annotations

import hashlib
import os
import re
import tarfile
import tempfile
from contextlib import contextmanager
from io import BytesIO, StringIO
from typing import Literal

import zstandard

from DIRAC import S_ERROR, S_OK, gLogger
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getVOForGroup
from DIRAC.Core.Base.Client import Client
from DIRAC.Core.Tornado.Client.ClientSelector import TransferClientSelector as TransferClient
from DIRAC.Core.Utilities.File import getGlobbedTotalSize, mkDir
from DIRAC.Core.Utilities.ReturnValues import returnSingleResult
from DIRAC.Resources.Storage.StorageElement import StorageElement


@contextmanager
def ZstdCompatibleTarFile(tarFileName: os.PathLike, *, mode: Literal["r"] = "r"):
    """Context manager to extend tarfile.open to support zstd compressed files.

    This is only needed for Python <=3.13.
    """
    with open(tarFileName, "rb") as f:
        magic = f.read(4)
    # Read magic bytes to determine compression format
    if magic.startswith(b"\x28\xb5\x2f\xfd"):  # zstd magic number
        dctx = zstandard.ZstdDecompressor()
        with open(tarFileName, "rb") as f, dctx.stream_reader(f) as decompressor:
            with tarfile.open(fileobj=decompressor, mode=f"{mode}|") as tf:
                yield tf
    else:
        with tarfile.open(name=tarFileName, mode=mode) as tf:
            yield tf


class SandboxStoreClient:
    __validSandboxTypes = ("Input", "Output")

    def __init__(self, rpcClient=None, transferClient=None, smdb=False, **kwargs):
        """Constructor

        :param object rpcClient: SandboxStore service client (None by default)
        :param object transferClient: client to upload/download sandboxes (None by default)
        :param object smdb: SandboxMetadataDB object, or
                            True if SandboxMetadataDB is to be instantiated for direct access or
                            False if no direct access to the SandboxMetadataDB is done (default)
        """

        self.__serviceName = "WorkloadManagement/SandboxStore"
        self.__rpcClient = rpcClient
        self.__transferClient = transferClient
        self.__kwargs = kwargs
        self.__vo = None
        if "delegatedGroup" in kwargs:
            self.__vo = getVOForGroup(kwargs["delegatedGroup"])

    def __getRPCClient(self):
        """Get an RPC client for SB service"""
        if self.__rpcClient:
            return self.__rpcClient
        return Client(url=self.__serviceName, **self.__kwargs)

    def __getTransferClient(self):
        """Get RPC client for TransferClient"""
        if self.__transferClient:
            return self.__transferClient
        return TransferClient(self.__serviceName, **self.__kwargs)

    # Upload generic sandbox

    def uploadFilesAsSandbox(self, fileList, sizeLimit=0, assignTo=None):
        """Send files in the fileList to a Sandbox service for the given jobID.
        This is the preferable method to upload sandboxes.

        a fileList item can be:
          - a string, which is an lfn name
          - a file name (real), that is supposed to be on disk, in the current directory
          - a fileObject that should be a BytesIO type of object

        Parameters:
          - assignTo : Dict containing { 'Job:<jobid>' : '<sbType>', ... }
        """
        errorFiles = []
        files2Upload = []
        if assignTo is None:
            assignTo = {}

        for key in assignTo:
            if assignTo[key] not in self.__validSandboxTypes:
                return S_ERROR(f"Invalid sandbox type {assignTo[key]}")

        if not isinstance(fileList, (list, tuple)):
            return S_ERROR("fileList must be a list or tuple!")

        for sFile in fileList:
            if isinstance(sFile, str):
                if re.search("^lfn:", sFile, flags=re.IGNORECASE):
                    pass
                else:
                    if os.path.exists(sFile):
                        files2Upload.append(sFile)
                    else:
                        errorFiles.append(sFile)

            elif isinstance(sFile, StringIO):
                files2Upload.append(sFile)
            else:
                return S_ERROR(f"Objects of type {type(sFile)} can't be part of InputSandbox")

        if errorFiles:
            return S_ERROR(f"Failed to locate files: {', '.join(errorFiles)}")

        try:
            fd, tmpFilePath = tempfile.mkstemp(prefix="LDSB.")
            os.close(fd)
        except Exception as e:
            return S_ERROR(f"Cannot create temporary file: {repr(e)}")

        with tarfile.open(name=tmpFilePath, mode="w|bz2") as tf:
            for sFile in files2Upload:
                if isinstance(sFile, str):
                    tf.add(os.path.realpath(sFile), os.path.basename(sFile), recursive=True)
                elif isinstance(sFile, StringIO):
                    tarInfo = tarfile.TarInfo(name="jobDescription.xml")
                    value = sFile.getvalue().encode()
                    tarInfo.size = len(value)
                    tf.addfile(tarinfo=tarInfo, fileobj=BytesIO(value))
                else:
                    return S_ERROR(f"Unknown type to upload: {repr(sFile)}")

        if sizeLimit > 0:
            # Evaluate the compressed size of the sandbox
            if getGlobbedTotalSize(tmpFilePath) > sizeLimit:
                result = S_ERROR("Size over the limit")
                result["SandboxFileName"] = tmpFilePath
                return result

        oMD5 = hashlib.md5()
        with open(tmpFilePath, "rb") as fd:
            bData = fd.read(10240)
            while bData:
                oMD5.update(bData)
                bData = fd.read(10240)

        transferClient = self.__getTransferClient()
        result = transferClient.sendFile(tmpFilePath, [f"{oMD5.hexdigest()}.tar.bz2", assignTo])
        result["SandboxFileName"] = tmpFilePath
        try:
            if result["OK"]:
                os.unlink(tmpFilePath)
        except OSError:
            pass
        return result

    ##############
    # Download sandbox

    def downloadSandbox(self, sbLocation, destinationDir="", inMemory=False, unpack=True):
        """
        Download a sandbox file and keep it in bundled form
        """
        if sbLocation.find("SB:") != 0:
            return S_ERROR("Invalid sandbox URL")
        sbLocation = sbLocation[3:]
        sbSplit = sbLocation.split("|")
        if len(sbSplit) < 2:
            return S_ERROR("Invalid sandbox URL")
        seName = sbSplit[0]
        sePFN = "|".join(sbSplit[1:])

        try:
            tmpSBDir = tempfile.mkdtemp(prefix="TMSB.")
        except OSError as e:
            return S_ERROR(f"Cannot create temporary file: {repr(e)}")

        se = StorageElement(seName, vo=self.__vo)
        result = returnSingleResult(se.getFile(sePFN, localPath=tmpSBDir))

        if not result["OK"]:
            return result
        sbFileName = os.path.basename(sePFN)

        result = S_OK()
        tarFileName = os.path.join(tmpSBDir, sbFileName)

        if inMemory:
            try:
                with open(tarFileName, "rb") as tfile:
                    data = tfile.read()
            except OSError as e:
                return S_ERROR(f"Failed to read the sandbox archive: {repr(e)}")
            finally:
                os.unlink(tarFileName)
                os.rmdir(tmpSBDir)
            return S_OK(data)

        # If destination dir is not specified use current working dir
        # If its defined ensure the dir structure is there
        if not destinationDir:
            destinationDir = os.getcwd()
        else:
            mkDir(destinationDir)

        if not unpack:
            result["Value"] = tarFileName
            return result

        try:
            sandboxSize = 0
            with ZstdCompatibleTarFile(tarFileName, mode="r") as tf:
                for tarinfo in tf:
                    tf.extract(tarinfo, path=destinationDir)
                    sandboxSize += tarinfo.size
            # FIXME: here we return the size, but otherwise we always return the location: inconsistent
            # FIXME: looks like this size is used by the JobWrapper
            result["Value"] = sandboxSize
        except OSError as e:
            result = S_ERROR(f"Could not open bundle: {repr(e)}")

        try:
            os.unlink(tarFileName)
            os.rmdir(tmpSBDir)
        except OSError as e:
            gLogger.warn(f"Could not remove temporary dir {tmpSBDir}: {repr(e)}")

        return result

    ##############
    # Jobs

    def downloadSandboxForJob(self, jobId, sbType, destinationPath="", inMemory=False, unpack=True):
        """Download SB for a job"""
        result = self.__getRPCClient().getSandboxesAssignedToEntity(f"Job:{jobId}")

        if not result["OK"]:
            return result
        sbDict = result["Value"]
        if sbType not in sbDict:
            return S_ERROR(
                f"No {sbType} sandbox found for job {jobId}. "
                + "Possible causes are: the job does not exist, no sandbox was "
                "registered or you do not have permission to access it."
            )

        # If inMemory, ensure we return the newest sandbox only
        if inMemory:
            sbLocation = sbDict[sbType][-1]
            return self.downloadSandbox(sbLocation, destinationPath, inMemory, unpack)

        downloadedSandboxesLoc = []
        for sbLocation in sbDict[sbType]:
            result = self.downloadSandbox(sbLocation, destinationPath, inMemory, unpack)
            if not result["OK"]:
                return result
            downloadedSandboxesLoc.append(result["Value"])
        return S_OK(downloadedSandboxesLoc)
