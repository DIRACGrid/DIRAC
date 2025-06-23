"""Base Storage Class provides the base interface for all storage plug-ins

      exists()

These are the methods for manipulating files:
      isFile()
      getFile()
      putFile()
      removeFile()
      getFileMetadata()
      getFileSize()
      prestageFile()
      getTransportURL()

These are the methods for manipulating directories:
      isDirectory()
      getDirectory()
      putDirectory()
      createDirectory()
      removeDirectory()
      listDirectory()
      getDirectoryMetadata()
      getDirectorySize()

These are the methods for manipulating the client:
      changeDirectory()
      getCurrentDirectory()
      getName()
      getParameters()
      getCurrentURL()

These are the methods for getting information about the Storage:
      getOccupancy()

"""

import errno
import json
import os
import shutil
import tempfile

from pathlib import Path

from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Utilities.Pfn import pfnparse, pfnunparse
from DIRAC.Core.Utilities.ReturnValues import returnSingleResult
from DIRAC.Resources.Storage.Utilities import checkArgumentFormat


class StorageBase:
    """
    .. class:: StorageBase

    """

    PROTOCOL_PARAMETERS = ["Protocol", "Host", "Path", "Port", "SpaceToken", "WSUrl"]
    # Options to be prepended in the URL
    # keys are the name of the parameters in the CS
    # values are the name of the options as they appear in the URL
    DYNAMIC_OPTIONS = {}

    def __init__(self, name, parameterDict):
        self.name = name
        self.pluginName = ""
        # This is set by the storageFactory and is the
        # name of the protocol section in the CS
        self.protocolSectionName = ""
        self.protocolParameters = {}

        self.__updateParameters(parameterDict)

        # Keep the list of all parameters passed for constructions
        # Taken from the CS
        # In a further major release, this could be nerged together
        # with protocolParameters. There is no reason for it to
        # be so strict about the possible content.
        self._allProtocolParameters = parameterDict

        if "InputProtocols" in parameterDict:
            self.protocolParameters["InputProtocols"] = parameterDict["InputProtocols"].replace(" ", "").split(",")
        elif hasattr(self, "_INPUT_PROTOCOLS"):
            self.protocolParameters["InputProtocols"] = getattr(self, "_INPUT_PROTOCOLS")
        else:
            self.protocolParameters["InputProtocols"] = [self.protocolParameters["Protocol"], "file"]

        if "OutputProtocols" in parameterDict:
            self.protocolParameters["OutputProtocols"] = parameterDict["OutputProtocols"].replace(" ", "").split(",")
        elif hasattr(self, "_OUTPUT_PROTOCOLS"):
            self.protocolParameters["OutputProtocols"] = getattr(self, "_OUTPUT_PROTOCOLS")
        else:
            self.protocolParameters["OutputProtocols"] = [self.protocolParameters["Protocol"]]

        self.basePath = parameterDict["Path"]
        self.cwd = self.basePath
        self.se = None

        # use True for backward compatibility
        self.srmSpecificParse = True

    def setStorageElement(self, se):
        self.se = se

    def setParameters(self, parameterDict):
        """Set standard parameters, method can be overriden in subclasses
        to process specific parameters
        """
        self.__updateParameters(parameterDict)

    def __updateParameters(self, parameterDict):
        """setParameters implementation method"""
        for item in self.PROTOCOL_PARAMETERS:
            self.protocolParameters[item] = parameterDict.get(item, "")

    def getParameters(self):
        """Get the parameters with which the storage was instantiated"""
        parameterDict = dict(self._allProtocolParameters)
        parameterDict.update(self.protocolParameters)
        parameterDict["StorageName"] = self.name
        parameterDict["PluginName"] = self.pluginName
        parameterDict["URLBase"] = self.getURLBase().get("Value", "")
        parameterDict["Endpoint"] = self.getEndpoint().get("Value", "")

        return parameterDict

    def exists(self, *parms, **kws):
        """Check if the given path exists"""
        return S_ERROR("Storage.exists: implement me!")

    #############################################################
    #
    # These are the methods for file manipulation
    #

    def isFile(self, *parms, **kws):
        """Check if the given path exists and it is a file"""
        return S_ERROR("Storage.isFile: implement me!")

    def getFile(self, *parms, **kws):
        """Get a local copy of the file specified by its path"""
        return S_ERROR("Storage.getFile: implement me!")

    def putFile(self, *parms, **kws):
        """Put a copy of the local file to the current directory on the
        physical storage
        """
        return S_ERROR("Storage.putFile: implement me!")

    def removeFile(self, *parms, **kws):
        """Remove physically the file specified by its path"""
        return S_ERROR("Storage.removeFile: implement me!")

    def getFileMetadata(self, *parms, **kws):
        """Get metadata associated to the file"""
        return S_ERROR("Storage.getFileMetadata: implement me!")

    def getFileSize(self, *parms, **kws):
        """Get the physical size of the given file"""
        return S_ERROR("Storage.getFileSize: implement me!")

    def prestageFile(self, *parms, **kws):
        """Issue prestage request for file"""
        return S_ERROR("Storage.prestageFile: implement me!")

    def prestageFileStatus(self, *parms, **kws):
        """Obtain the status of the prestage request"""
        return S_ERROR("Storage.prestageFileStatus: implement me!")

    def releaseFile(self, *parms, **kws):
        """Release the file on the destination storage element"""
        return S_ERROR("Storage.releaseFile: implement me!")

    #############################################################
    #
    # These are the methods for directory manipulation
    #

    def isDirectory(self, *parms, **kws):
        """Check if the given path exists and it is a directory"""
        return S_ERROR("Storage.isDirectory: implement me!")

    def getDirectory(self, *parms, **kws):
        """Get locally a directory from the physical storage together with all its
        files and subdirectories.
        """
        return S_ERROR("Storage.getDirectory: implement me!")

    def putDirectory(self, *parms, **kws):
        """Put a local directory to the physical storage together with all its
        files and subdirectories.
        """
        return S_ERROR("Storage.putDirectory: implement me!")

    def createDirectory(self, *parms, **kws):
        """Make a new directory on the physical storage"""
        return S_ERROR("Storage.createDirectory: implement me!")

    def removeDirectory(self, *parms, **kws):
        """Remove a directory on the physical storage together with all its files and
        subdirectories.
        """
        return S_ERROR("Storage.removeDirectory: implement me!")

    def listDirectory(self, *parms, **kws):
        """List the supplied path"""
        return S_ERROR("Storage.listDirectory: implement me!")

    def getDirectoryMetadata(self, *parms, **kws):
        """Get the metadata for the directory"""
        return S_ERROR("Storage.getDirectoryMetadata: implement me!")

    def getDirectorySize(self, *parms, **kws):
        """Get the size of the directory on the storage"""
        return S_ERROR("Storage.getDirectorySize: implement me!")

    #############################################################
    #
    # These are the methods for manipulating the client
    #

    def resetCurrentDirectory(self):
        """Reset the working directory to the base dir"""
        self.cwd = self.basePath

    def changeDirectory(self, directory):
        """Change the directory to the supplied directory"""
        if directory.startswith("/"):
            self.cwd = f"{self.basePath}/{directory}"
        else:
            self.cwd = f"{self.cwd}/{directory}"

    def getCurrentDirectory(self):
        """Get the current directory"""
        return self.cwd

    def getCurrentURL(self, fileName):
        """Obtain the current file URL from the current working directory and the filename

        :param self: self reference
        :param str fileName: path on storage
        """
        urlDict = dict(self.protocolParameters)
        if not fileName.startswith("/"):
            # Relative path is given
            urlDict["Path"] = self.cwd
        result = pfnunparse(urlDict, srmSpecific=self.srmSpecificParse)
        if not result["OK"]:
            return result
        cwdUrl = result["Value"]
        fullUrl = f"{cwdUrl}{fileName}"
        return S_OK(fullUrl)

    def getName(self):
        """The name with which the storage was instantiated"""
        return self.name

    def getURLBase(self, withWSUrl=False):
        """This will get the URL base. This is then appended with the LFN in DIRAC convention.

        :param self: self reference
        :param bool withWSUrl: flag to include Web Service part of the url
        :returns: URL
        """
        urlDict = dict(self.protocolParameters)
        if not withWSUrl:
            urlDict["WSUrl"] = ""
        return pfnunparse(urlDict, srmSpecific=self.srmSpecificParse)

    def getEndpoint(self):
        """This will get endpoint of the storage. It basically is the same as :py:meth:`getURLBase`
            but without the basePath

        :returns: 'proto://hostname<:port>'

        """
        urlDict = dict(self.protocolParameters)
        # We remove the basePath
        urlDict["Path"] = ""
        return pfnunparse(urlDict, srmSpecific=self.srmSpecificParse)

    def isURL(self, path):
        """Guess if the path looks like a URL

        :param self: self reference
        :param string path: input file LFN or URL
        :returns boolean: True if URL, False otherwise
        """
        if self.basePath and path.startswith(self.basePath):
            return S_OK(True)

        result = pfnparse(path, srmSpecific=self.srmSpecificParse)
        if not result["OK"]:
            return result

        if len(result["Value"]["Protocol"]) != 0:
            return S_OK(True)

        if result["Value"]["Path"].startswith(self.basePath):
            return S_OK(True)

        return S_OK(False)

    def getTransportURL(self, pathDict, protocols):
        """Get a transport URL for a given URL. For a simple storage plugin
        it is just returning input URL if the plugin protocol is one of the
        requested protocols

        :param dict pathDict: URL obtained from File Catalog or constructed according
                        to convention
        :param protocols: a list of acceptable transport protocols in priority order
        :type protocols: `python:list`
        """
        res = checkArgumentFormat(pathDict)
        if not res["OK"]:
            return res
        urls = res["Value"]
        successful = {}
        failed = {}

        if protocols and not self.protocolParameters["Protocol"] in protocols:
            return S_ERROR(errno.EPROTONOSUPPORT, "No native protocol requested")

        for url in urls:
            successful[url] = url

        resDict = {"Failed": failed, "Successful": successful}
        return S_OK(resDict)

    def constructURLFromLFN(self, lfn, withWSUrl=False):
        """Construct URL from the given LFN according to the VO convention for the
        primary protocol of the storage plugin

        :param str lfn: file LFN
        :param boolean withWSUrl: flag to include the web service part into the resulting URL
        :return result: result['Value'] - resulting URL
        """

        # Check the LFN convention:
        # 1. LFN must start with the VO name as the top level directory
        # 2. VO name must not appear as any subdirectory or file name
        lfnSplitList = lfn.split("/")
        voLFN = lfnSplitList[1]
        if voLFN != self.se.vo and voLFN != "SandBox" and voLFN != "S3":
            return S_ERROR(f"LFN ({lfn}) path must start with VO name ({self.se.vo})")

        urlDict = dict(self.protocolParameters)
        urlDict["Options"] = "&".join(
            f"{optionName}={urlDict[paramName]}"
            for paramName, optionName in self.DYNAMIC_OPTIONS.items()
            if urlDict.get(paramName)
        )
        if not withWSUrl:
            urlDict["WSUrl"] = ""
        urlDict["FileName"] = lfn.lstrip("/")

        return pfnunparse(urlDict, srmSpecific=self.srmSpecificParse)

    def updateURL(self, url, withWSUrl=False):
        """Update the URL according to the current SE parameters"""
        result = pfnparse(url, srmSpecific=self.srmSpecificParse)
        if not result["OK"]:
            return result
        urlDict = result["Value"]

        urlDict["Protocol"] = self.protocolParameters["Protocol"]
        urlDict["Host"] = self.protocolParameters["Host"]
        urlDict["Port"] = self.protocolParameters["Port"]
        urlDict["WSUrl"] = ""
        if withWSUrl:
            urlDict["WSUrl"] = self.protocolParameters["WSUrl"]

        return pfnunparse(urlDict, srmSpecific=self.srmSpecificParse)

    def isNativeURL(self, url):
        """Check if URL :url: is valid for :self.protocol:

        :param self: self reference
        :param str url: URL
        """
        res = pfnparse(url, srmSpecific=self.srmSpecificParse)
        if not res["OK"]:
            return res
        urlDict = res["Value"]
        return S_OK(urlDict["Protocol"] == self.protocolParameters["Protocol"])

    @staticmethod
    def _addCommonMetadata(metadataDict):
        """To make the output of getFileMetadata uniform throughout the protocols
        this returns a minimum set of metadata with default value,
        that are then complemented with the protocol specific metadata

        :param metadataDict: specific metadata of the protocol

        :returns: dictionary with all the metadata (specific and basic)
        """
        commonMetadata = {
            "Checksum": "",
            "Directory": False,
            "File": False,
            "Mode": 0o000,
            "Size": 0,
            "Accessible": True,
        }

        commonMetadata.update(metadataDict)

        return commonMetadata

    def _isInputURL(self, url):
        """Check if the given url can be taken as input

        :param self: self reference
        :param str url: URL
        """
        res = pfnparse(url)
        if not res["OK"]:
            return res
        urlDict = res["Value"]

        # Special case of 'file' protocol which can be just a URL
        if not urlDict["Protocol"] and "file" in self.protocolParameters["InputProtocols"]:
            return S_OK(True)

        return S_OK(urlDict["Protocol"] == self.protocolParameters["Protocol"])

    #############################################################
    #
    # These are the methods for getting information about the Storage element:
    #

    def getOccupancy(self, **kwargs):
        """Get the StorageElement occupancy info in MB.

        This generic implementation download a json file supposed to contain the necessary info.

        :param occupancyLFN: (mandatory named argument) LFN of the json file.

        :returns: S_OK/S_ERROR dictionary. The S_OK value should contain a dictionary with Total and Free space in MB
        """

        # Build the URL for the occupancyLFN:
        occupancyLFN = kwargs["occupancyLFN"]
        res = self.constructURLFromLFN(occupancyLFN)
        if not res["OK"]:
            return res
        occupancyURL = res["Value"]

        try:
            # download the file locally
            tmpDirName = tempfile.mkdtemp()
            res = returnSingleResult(self.getFile(occupancyURL, localPath=tmpDirName))
            if not res["OK"]:
                return res

            filePath = os.path.join(tmpDirName, os.path.basename(occupancyLFN))

            # Read its json content
            with open(filePath) as occupancyFile:
                return S_OK(json.load(occupancyFile))

        except Exception as e:
            return S_ERROR(repr(e))

        finally:
            # Clean the temporary dir
            shutil.rmtree(tmpDirName)

    def getWLCGTokenPath(self, lfn: str, wlcgTokenBasePath: str) -> str:
        """
        Returns the path expected to be in a WLCG token
        It basically consists of ``basepath - tokenBasePath  + LFN``
        The tokenBasePath is a configuration on the storage side.

        """

        allDict = dict.fromkeys(["Protocol", "Host", "Port", "Path", "FileName", "Options"], "")
        allDict.update({"Path": self.protocolParameters["Path"], "FileName": lfn.lstrip("/")})
        fullPath = pfnunparse(allDict)["Value"]
        return Path(fullPath).relative_to(Path(wlcgTokenBasePath))
