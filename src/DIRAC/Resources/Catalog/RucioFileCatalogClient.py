""" The RucioFileCatalogClient is a class that allows to interface Dirac with Rucio
"""
import os
import uuid
import datetime
from copy import deepcopy

import DIRAC
from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.Security import Locations
from DIRAC.Resources.Catalog.Utilities import checkCatalogArguments
from DIRAC.Core.Utilities.List import breakListIntoChunks
from DIRAC.Core.Security.ProxyInfo import getProxyInfo, getVOfromProxyGroup
from DIRAC.Resources.Catalog.FileCatalogClientBase import FileCatalogClientBase

from rucio.client import Client
from rucio.common.exception import (
    DataIdentifierNotFound,
    FileReplicaAlreadyExists,
    CannotAuthenticate,
    MissingClientParameter,
)
from rucio.common.utils import chunks, extract_scope

sLog = gLogger.getSubLogger(__name__)


def get_scope(lfn, scopes=None, diracAlgorithm="dirac"):
    """
    Helper function that extracts the scope from the LFN.

    :param str lfn: Logical file name
    :param list scopes: list of scopes
    :param str diracAlgorithm: only used by extract_scope if there is no config file with an algorithm listed.
                               Otherwise use the algorithm listed in the config file.
    :return: scope name
    """

    if scopes is None:
        scopes = []
    scope, _ = extract_scope(did=lfn, scopes=scopes, default_extract=diracAlgorithm)
    return scope


class RucioFileCatalogClient(FileCatalogClientBase):
    READ_METHODS = FileCatalogClientBase.READ_METHODS + [
        "isLink",
        "readLink",
        "isFile",
        "getFileMetadata",
        "getReplicas",
        "getReplicaStatus",
        "getFileSize",
        "isDirectory",
        "getDirectoryReplicas",
        "listDirectory",
        "getDirectoryMetadata",
        "getDirectorySize",
        "getDirectoryContents",
        "resolveDataset",
        "getLFNForPFN",
        "getUserDirectory",
        "getFileUserMetadata",
        "findFilesByMetadata",
    ]

    WRITE_METHODS = FileCatalogClientBase.WRITE_METHODS + [
        "createLink",
        "removeLink",
        "addFile",
        "addReplica",
        "removeReplica",
        "removeFile",
        "setReplicaStatus",
        "setReplicaHost",
        "createDirectory",
        "removeDirectory",
        "removeDataset",
        "removeFileFromDataset",
        "createDataset",
        "changePathOwner",
        "changePathMode",
        "setMetadata",
    ]

    NO_LFN_METHODS = FileCatalogClientBase.NO_LFN_METHODS + [
        "getUserDirectory",
        "createUserDirectory",
        "createUserMapping",
        "removeUserDirectory",
        "findFilesByMetadata",
    ]

    ADMIN_METHODS = FileCatalogClientBase.ADMIN_METHODS + [
        "getUserDirectory",
        "createUserDirectory",
        "createUserMapping",
        "removeUserDirectory",
    ]

    def __init__(self, **options):
        """
        Constructor. Takes options defined in Resources and Operations for this client.

        :param options: options dict
        """
        self.diracScopeAlg = options.get("DiracScopeAlg", "dirac")
        self.useDiracCS = False  # use a Rucio config file
        self.convertUnicode = True
        proxyInfo = {"OK": False}
        if os.getenv("RUCIO_AUTH_TYPE") == "x509_proxy" and not os.getenv("X509_USER_PROXY"):
            proxyInfo = getProxyInfo(disableVOMS=True)
            if proxyInfo["OK"]:
                os.environ["X509_USER_PROXY"] = proxyInfo["Value"]["path"]
                sLog.debug(f"X509_USER_PROXY not defined. Using {proxyInfo['Value']['path']}")
        try:
            try:
                self._client = Client()
                self.account = self._client.account
            except (CannotAuthenticate, MissingClientParameter):
                if os.getenv("RUCIO_AUTH_TYPE") == "x509_proxy":
                    if not proxyInfo["OK"]:
                        proxyInfo = getProxyInfo(disableVOMS=True)
                    if proxyInfo["OK"]:
                        dn = proxyInfo["Value"]["identity"]
                        username = proxyInfo["Value"]["username"]
                        self.account = username
                        sLog.debug(f"Switching to account {username} mapped to proxy {dn}")

            try:
                self._client = Client(account=self.account)
                self.scopes = self._client.list_scopes()
            except Exception as err:
                sLog.error("Cannot instantiate RucioFileCatalog interface using a config file", f"error : {repr(err)}")
                sLog.info("will try using Dirac CS")

        except Exception as err:
            # instantiate the client w/o a config file
            sLog.debug("instantiate the client w/o a config file -  take config params from the CS")
            self.useDiracCS = True
            proxyInfo = getProxyInfo(disableVOMS=True)
            if proxyInfo["OK"]:
                proxyDict = proxyInfo["Value"]
                self.proxyPath = proxyDict.get("path", None)
                self.username = proxyDict.get("username", None)
            else:
                sLog.error("Cannot instantiate RucioFileCatalog interface", proxyInfo["Message"])
                return
            self.VO = getVOfromProxyGroup()["Value"]
            self.rucioHost = options.get("RucioHost", None)
            self.authHost = options.get("AuthHost", None)
            self.caCertPath = Locations.getCAsLocation()
            try:
                sLog.debug(f"Logging in with a proxy located at: {self.proxyPath}")
                sLog.debug("account: ", self.username)
                sLog.debug("rucio host: ", self.rucioHost)
                sLog.debug("auth  host: ", self.authHost)
                sLog.debug("CA cert path: ", self.caCertPath)
                sLog.debug("VO: ", self.VO)

                self._client = Client(
                    account=self.username,
                    rucio_host=self.rucioHost,
                    auth_host=self.authHost,
                    ca_cert=self.caCertPath,
                    auth_type="x509_proxy",
                    creds={"client_proxy": self.proxyPath},
                    timeout=600,
                    user_agent="rucio-clients",
                    vo=self.VO,
                )

                sLog.debug(f"Rucio client instantiated successfully for VO {self.VO} and  account {self.username} ")
            except Exception as err:
                sLog.error("Cannot instantiate RucioFileCatalog interface", f"error : {repr(err)}")

    @property
    def client(self):
        """Check if the session to the server is still active and return an instance of RucioClient"""
        try:
            self._client.ping()
            return self._client
        except Exception:
            if not self.useDiracCS:
                self._client = Client(account=self.account)
            else:
                self._client = Client(
                    account=self.username,
                    rucio_host=self.rucioHost,
                    auth_host=self.authHost,
                    ca_cert=self.caCertPath,
                    auth_type="x509_proxy",
                    creds={"client_proxy": self.proxyPath},
                    timeout=600,
                    user_agent="rucio-clients",
                    vo=self.VO,
                )

            self.scopes = self._client.list_scopes()
            return self._client

    def __getDidsFromLfn(self, lfn):
        """Helper function to convert LFNs into DIDs"""
        if lfn.find(":") > -1:
            scope, name = lfn.split(":")
        else:
            scope = get_scope(lfn, scopes=self.scopes, diracAlgorithm=self.diracScopeAlg)
            name = lfn
        return {"scope": scope, "name": name}

    @checkCatalogArguments
    def getCompatibleMetadata(self, queryDict, path, credDict):
        """Get distinct metadata values compatible with the given already defined metadata"""
        if path != "/":
            result = self.exists(path)
            if not result["OK"]:
                return result
            if not result["Value"]:
                return S_ERROR(f"Path not found: {path}")
        result = S_OK({})
        return result

    @checkCatalogArguments
    def getReplicas(self, lfns, allStatus=False):
        """Returns replicas for an LFN or list of LFNs"""
        result = {"OK": True, "Value": {"Successful": {}, "Failed": {}}}
        lfnChunks = breakListIntoChunks(lfns, 1000)

        for lfnList in lfnChunks:
            try:
                didList = [self.__getDidsFromLfn(lfn) for lfn in lfnList if lfn]
                for rep in self.client.list_replicas(didList):
                    if rep:
                        lfn = rep["name"]
                        if self.convertUnicode:
                            lfn = str(lfn)
                        if lfn not in result["Value"]["Successful"]:
                            result["Value"]["Successful"][lfn] = {}
                        for rse in rep["rses"]:
                            if self.convertUnicode:
                                result["Value"]["Successful"][lfn][str(rse)] = str(rep["rses"][rse][0])
                            else:
                                result["Value"]["Successful"][lfn][rse] = rep["rses"][rse][0]
                    else:
                        for did in didList:
                            result["Value"]["Failed"][did["name"]] = "Error"
            except Exception as err:
                return S_ERROR(str(err))
        return result

    @checkCatalogArguments
    def listDirectory(self, lfns, verbose=False):
        """
        Returns the result of __getDirectoryContents for multiple supplied paths.

        :param list lfns: a list of logical filenames
        :param bool verbose: verbose flag.
        :return: S_OK with a Value of successful and failed directory tree
        """
        result = {"OK": True, "Value": {"Successful": {}, "Failed": {}}}

        for lfn in lfns:
            try:
                did = self.__getDidsFromLfn(lfn)
                # First need to check if it's a dataset or container
                meta = self.client.get_metadata(did["scope"], did["name"])
                if meta["did_type"] == "CONTAINER":
                    if lfn not in result["Value"]["Successful"]:
                        result["Value"]["Successful"][lfn] = {"Files": {}, "Links": {}, "SubDirs": {}}
                    for child in self.client.list_content(scope=did["scope"], name=did["name"]):
                        childName = child["name"]
                        if self.convertUnicode:
                            childName = str(childName)
                        if child["type"] in ["DATASET", "CONTAINER"]:
                            result["Value"]["Successful"][lfn]["SubDirs"][childName] = {"Mode": 509}
                            if verbose:
                                result["Value"]["Successful"][lfn]["SubDirs"][childName] = {
                                    "Status": "-",
                                    "CreationDate": datetime.datetime(1970, 1, 1),
                                    "ChecksumType": "",
                                    "OwnerRole": "None",
                                    "Checksum": "",
                                    "NumberOfLinks": -999,
                                    "OwnerDN": "None",
                                    "Mode": 509,
                                    "ModificationDate": datetime.datetime(1970, 1, 1),
                                    "GUID": "",
                                    "LastAccess": 0,
                                    "Size": 0,
                                }
                        else:
                            result["Value"]["Successful"][lfn]["Files"][childName] = {"Mode": 509}
                            if verbose:
                                pass
                elif meta["did_type"] == "DATASET":
                    file_dict = {}
                    for file_did in self.client.list_files(scope=did["scope"], name=did["name"]):
                        guid = file_did["guid"]
                        file_dict[file_did["name"]] = guid
                        if guid:
                            file_dict[file_did["name"]] = str(uuid.UUID(guid))
                    if lfn not in result["Value"]["Successful"]:
                        result["Value"]["Successful"][lfn] = {"Files": {}, "Links": {}, "SubDirs": {}}
                    for rep in self.client.list_replicas(
                        [
                            did,
                        ]
                    ):
                        if rep:
                            name = rep["name"]
                            if self.convertUnicode:
                                name = str(name)
                            result["Value"]["Successful"][lfn]["Files"][name] = {"Metadata": {}, "Replicas": {}}
                            result["Value"]["Successful"][lfn]["Files"][name]["Metadata"] = {
                                "GUID": str(file_dict.get(name, "UNKNOWN")),
                                "Mode": 436,
                                "Size": rep["bytes"],
                            }
                            if verbose:
                                result["Value"]["Successful"][lfn]["Files"][name]["Metadata"] = {
                                    "Status": "-",
                                    "CreationDate": datetime.datetime(1970, 1, 1),
                                    "ChecksumType": "AD",
                                    "OwnerRole": "None",
                                    "Checksum": str(rep["adler32"]),
                                    "NumberOfLinks": 1,
                                    "OwnerDN": "None",
                                    "Mode": 436,
                                    "ModificationDate": datetime.datetime(1970, 1, 1),
                                    "GUID": str(file_dict.get(name, "UNKNOWN")),
                                    "LastAccess": 0,
                                    "Size": rep["bytes"],
                                }
                            for rse in rep["rses"]:
                                pfn = rep["rses"][rse][0]
                                if self.convertUnicode:
                                    pfn = str(pfn)
                                result["Value"]["Successful"][lfn]["Files"][name]["Replicas"][rse] = {
                                    "PFN": pfn,
                                    "Status": "U",
                                }
            except DataIdentifierNotFound as err:
                result["Value"]["Failed"][lfn] = "No such file or directory"
            except Exception as err:
                return S_ERROR(str(err))
        return result

    @checkCatalogArguments
    def getFileMetadata(self, lfns, ownership=False):
        """Returns the file metadata associated to a supplied LFN"""
        successful, failed = {}, {}
        lfnChunks = breakListIntoChunks(lfns, 1000)
        listFiles = deepcopy(list(lfns))
        for chunk in lfnChunks:
            try:
                dids = [self.__getDidsFromLfn(lfn) for lfn in chunk]
                for meta in self.client.get_metadata_bulk(dids):
                    lfn = str(meta["name"])
                    if meta["did_type"] in ["DATASET", "CONTAINER"]:
                        nlinks = len([child for child in self.client.list_content(meta["scope"], meta["name"])])
                        successful[lfn] = {
                            "Checksum": "",
                            "ChecksumType": "",
                            "CreationDate": meta["created_at"],
                            "GUID": "",
                            "Mode": 509,
                            "ModificationDate": meta["updated_at"],
                            "NumberOfLinks": nlinks,
                            "Size": 0,
                            "Status": "-",
                        }
                        try:
                            listFiles.remove(lfn)
                        except ValueError:
                            pass
                    else:
                        guid = meta["guid"]
                        if guid:
                            guid = str(uuid.UUID(guid))
                        successful[lfn] = {
                            "Checksum": str(meta["adler32"]),
                            "ChecksumType": "AD",
                            "CreationDate": meta["created_at"],
                            "GUID": guid,
                            "Mode": 436,
                            "ModificationDate": meta["updated_at"],
                            "NumberOfLinks": 1,
                            "Size": meta["bytes"],
                            "Status": "-",
                        }
                        try:
                            listFiles.remove(lfn)
                        except ValueError:
                            pass
            except DataIdentifierNotFound as err:
                failed[lfn] = str(err)
            except Exception as err:
                return S_ERROR(str(err))
        for lfn in listFiles:
            failed[lfn] = "No such file or directory"
        resDict = {"Failed": failed, "Successful": successful}
        return S_OK(resDict)

    @checkCatalogArguments
    def exists(self, lfns):
        """Check if the path exists"""
        result = {"OK": True, "Value": {"Successful": {}, "Failed": {}}}
        for lfn in lfns:
            try:
                did = self.__getDidsFromLfn(lfn)
                exists = True
                self.client.get_metadata(did["scope"], did["name"])
            except DataIdentifierNotFound:
                exists = False
            except Exception as err:
                return S_ERROR(str(err))
            result["Value"]["Successful"][lfn] = exists
        return result

    @checkCatalogArguments
    def getFileSize(self, lfns):
        """Get the size of a supplied file"""
        result = {"OK": True, "Value": {"Successful": {}, "Failed": {}}}
        for lfn in lfns:
            try:
                did = self.__getDidsFromLfn(lfn)
                meta = self.client.get_metadata(did["scope"], did["name"])
                if meta["did_type"] == "FILE":
                    result["Value"]["Successful"][lfn] = meta["bytes"]
                else:
                    result["Value"]["Successful"][lfn] = 0
            except DataIdentifierNotFound:
                result["Value"]["Failed"][lfn] = "No such file or directory"
            except Exception as err:
                return S_ERROR(str(err))
        return result

    @checkCatalogArguments
    def isDirectory(self, lfns):
        """Determine whether the path is a directory"""
        result = {"Successful": {}, "Failed": {}}
        dids = [self.__getDidsFromLfn(lfn) for lfn in lfns]
        try:
            for meta in self.client.get_metadata_bulk(dids):
                lfn = str(meta["name"])
                result["Successful"][lfn] = meta["did_type"] in ["DATASET", "CONTAINER"]
            for lfn in lfns:
                if lfn not in result["Successful"] and lfn not in result["Failed"]:
                    result["Failed"][lfn] = "No such file or directory"
        except Exception as err:
            return S_ERROR(str(err))
        return S_OK(result)

    @checkCatalogArguments
    def isFile(self, lfns):
        """Determine whether the path is a file"""
        result = {"Successful": {}, "Failed": {}}
        dids = []
        for lfn in lfns:
            dids.append(self.__getDidsFromLfn(lfn))
        try:
            for meta in self.client.get_metadata_bulk(dids):
                lfn = str(meta["name"])
                result["Successful"][lfn] = meta["did_type"] in ["FILE"]
            for lfn in lfns:
                if lfn not in result["Successful"] and lfn not in result["Failed"]:
                    result["Failed"][lfn] = "No such file or directory"
        except Exception as err:
            return S_ERROR(str(err))
        return S_OK(result)

    @checkCatalogArguments
    def addFile(self, lfns):
        """Register supplied files"""
        failed = {}
        successful = {}
        deterministicDictionary = {}
        for lfnList in breakListIntoChunks(lfns, 100):
            listLFNs = []
            for lfn in list(lfnList):
                lfnInfo = lfns[lfn]
                pfn = None
                se = lfnInfo["SE"]
                if se not in deterministicDictionary:
                    isDeterministic = self.client.get_rse(se)["deterministic"]
                    deterministicDictionary[se] = isDeterministic
                if not deterministicDictionary[se]:
                    pfn = lfnInfo["PFN"]
                size = lfnInfo["Size"]
                guid = lfnInfo.get("GUID", None)
                checksum = lfnInfo["Checksum"]
                rep = {"lfn": lfn, "bytes": size, "adler32": checksum, "rse": se}
                if pfn:
                    rep["pfn"] = pfn
                if guid:
                    rep["guid"] = guid
                listLFNs.append(rep)
            try:
                self.client.add_files(lfns=listLFNs, ignore_availability=True)
                for lfn in list(lfnList):
                    successful[lfn] = True
            except Exception as err:
                # Try inserting one by one
                sLog.warn("Cannot bulk insert files", f"error : {repr(err)}")
                for lfn in listLFNs:
                    try:
                        self.client.add_files(lfns=[lfn], ignore_availability=True)
                        successful[lfn["lfn"]] = True
                    except FileReplicaAlreadyExists:
                        successful[lfn["lfn"]] = True
                    except Exception as err:
                        failed[lfn["lfn"]] = str(err)
        resDict = {"Failed": failed, "Successful": successful}
        sLog.debug(resDict)
        return S_OK(resDict)

    @checkCatalogArguments
    def addReplica(self, lfns):
        """This adds a replica to the catalogue."""
        failed = {}
        successful = {}
        deterministicDictionary = {}
        for lfn, info in lfns.items():
            pfn = None
            se = info["SE"]
            if se not in deterministicDictionary:
                isDeterministic = self.client.get_rse(se)["deterministic"]
                deterministicDictionary[se] = isDeterministic
            if not deterministicDictionary[se]:
                pfn = info["PFN"]
            size = info.get("Size", None)
            checksum = info.get("Checksum", None)
            try:
                did = self.__getDidsFromLfn(lfn)
                if not size or not checksum:
                    meta = self.client.get_metadata(did["scope"], did["name"])
                    size = meta["bytes"]
                    checksum = meta["adler32"]
                rep = {"scope": did["scope"], "name": did["name"], "bytes": size, "adler32": checksum}
                if pfn:
                    rep["pfn"] = pfn
                self.client.add_replicas(
                    rse=se,
                    files=[
                        rep,
                    ],
                )
                self.client.add_replication_rule(
                    [{"scope": did["scope"], "name": did["name"]}],
                    copies=1,
                    activity="User Transfers",
                    rse_expression=se,
                    weight=None,
                    lifetime=None,
                    grouping="NONE",
                    account=self.account,
                )
                successful[lfn] = True
            except FileReplicaAlreadyExists:
                successful[lfn] = True
            except Exception as err:
                failed[lfn] = str(err)
        resDict = {"Failed": failed, "Successful": successful}
        return S_OK(resDict)

    @checkCatalogArguments
    def removeReplica(self, lfns):
        """Remove the supplied replicas"""
        failed = {}
        successful = {}
        for lfn, info in lfns.items():
            if "SE" not in info:
                failed[lfn] = "Required parameters not supplied"
                continue
            se = info["SE"]
            try:
                did = self.__getDidsFromLfn(lfn)
                meta = self.client.get_metadata(did["scope"], did["name"])
                if meta["did_type"] == "FILE":
                    # For file cannot use dataset_locks to identify the rule
                    for rule in self.client.list_did_rules(did["scope"], did["name"]):
                        rid = rule["id"]
                        self.client.update_replication_rule(rid, options={"lifetime": -86400})
                    successful[lfn] = True
                elif meta["did_type"] == "DATASET":
                    rules = {}
                    for lock in self.client.get_dataset_locks(did["scope"], did["name"]):
                        rule_id = lock["rule_id"]
                        if rule_id not in rules:
                            rules[rule_id] = []
                        rse = lock["rse"]
                        if str(rse) not in rules[rule_id]:
                            rules[rule_id].append(str(rse))

                    for rid in rules:
                        if len(rules[rid]) == 1 and rules[rid][0] == se:
                            rule_info = self.client.get_replication_rule(rid)
                            if str(rule_info["name"]) == lfn:
                                self.client.update_replication_rule(rid, options={"lifetime": -86400})
                    successful[lfn] = True

            except Exception as err:
                # Always assumes that it succeeded
                failed[lfn] = str(err)
        resDict = {"Failed": failed, "Successful": successful}
        return S_OK(resDict)

    @checkCatalogArguments
    def removeFile(self, lfns):
        """Remove the supplied path"""
        resDict = {"Successful": {}, "Failed": {}}
        for lfn in lfns:
            try:
                did = self.__getDidsFromLfn(lfn)
                meta = self.client.get_metadata(did["scope"], did["name"])
                if meta["did_type"] == "FILE":
                    parentLfn = "/".join(lfn.split("/")[:-1])
                    parentDid = self.__getDidsFromLfn(parentLfn)
                    dsnScope, dsnName = parentDid["scope"], parentDid["name"]
                    try:
                        self.client.detach_dids(dsnScope, dsnName, [{"scope": did["scope"], "name": did["name"]}])
                        resDict["Successful"][lfn] = True
                    except Exception as err:
                        resDict["Failed"][lfn] = str(err)
                else:
                    resDict["Failed"][lfn] = "Not a file"
            except DataIdentifierNotFound:
                resDict["Failed"][lfn] = "No such file or directory"
            except Exception as err:
                return S_ERROR(str(err))
        return S_OK(resDict)

    @checkCatalogArguments
    def removeDirectory(self, lfns):
        """Remove the supplied directory"""
        resDict = {"Successful": {}, "Failed": {}}
        for lfn in lfns:
            try:
                did = self.__getDidsFromLfn(lfn)
                meta = self.client.get_metadata(did["scope"], did["name"])
                if meta["did_type"] == "DATASET":
                    try:
                        self.client.set_metadata(scope=did["scope"], name=did["name"], key="lifetime", value=1)
                        resDict["Successful"][lfn] = True
                    except Exception as err:
                        resDict["Failed"][lfn] = str(err)
                else:
                    resDict["Failed"][lfn] = "Not a directory"
            except DataIdentifierNotFound:
                resDict["Failed"][lfn] = "No such file or directory"
            except Exception as err:
                return S_ERROR(str(err))
        return S_OK(resDict)

    @checkCatalogArguments
    def getDirectorySize(self, lfns, longOutput=False, rawFiles=False):
        """Get the directory size"""
        resDict = {"Successful": {}, "Failed": {}}
        for lfn in lfns:
            try:
                did = self.__getDidsFromLfn(lfn)
                meta = self.client.get_metadata(did["scope"], did["name"])
                if meta["did_type"] == "FILE":
                    resDict["Failed"][lfn] = "Not a directory"

                elif meta["did_type"] == "CONTAINER":
                    resDict["Successful"][lfn] = {
                        "ClosedDirs": [],
                        "Files": 0,
                        "SiteUsage": {},
                        "SubDirs": {},
                        "TotalSize": 0,
                    }
                    for child in self.client.list_content(scope=did["scope"], name=did["name"]):
                        childName = child["name"]
                        if self.convertUnicode:
                            childName = str(childName)
                        resDict["Successful"][lfn]["SubDirs"][childName] = datetime.datetime.now()

                else:
                    resDict["Successful"][lfn] = {
                        "ClosedDirs": [],
                        "Files": 0,
                        "SiteUsage": {},
                        "SubDirs": {},
                        "TotalSize": 0,
                    }
                    for child in self.client.list_files(scope=did["scope"], name=did["name"]):
                        resDict["Successful"][lfn]["Files"] += 1
                        resDict["Successful"][lfn]["TotalSize"] += child["bytes"]
                    for rep in self.client.list_replicas([{"scope": did["scope"], "name": did["name"]}]):
                        fileSize = rep["bytes"]
                        for rse in rep["rses"]:
                            if rse not in resDict["Successful"][lfn]["SiteUsage"]:
                                resDict["Successful"][lfn]["SiteUsage"][rse] = {"Files": 0, "Size": 0}
                            resDict["Successful"][lfn]["SiteUsage"][rse]["Files"] += 1
                            resDict["Successful"][lfn]["SiteUsage"][rse]["Size"] += fileSize

            except DataIdentifierNotFound:
                resDict["Failed"][lfn] = "No such file or directory"
            except Exception as err:
                return S_ERROR(str(err))
        return S_OK(resDict)

    @checkCatalogArguments
    def getFileUserMetadata(self, path):
        """Get the meta data attached to a file, but also to
        all its parents
        """
        path = next(iter(path))
        resDict = {"Successful": {}, "Failed": {}}
        try:
            did = self.__getDidsFromLfn(path)
            meta = next(self.client.get_metadata_bulk(dids=[did], inherit=True, plugin="ALL"))
            if meta["did_type"] == "FILE":  # Should we also return the metadata for the directories ?
                resDict["Successful"][path] = meta
            else:
                resDict["Failed"][path] = "Not a file"
        except DataIdentifierNotFound:
            resDict["Failed"][path] = "No such file or directory"
        except Exception as err:
            return S_ERROR(str(err))
        return S_OK(resDict)

    @checkCatalogArguments
    def getFileUserMetadataBulk(self, lfns):
        """Get the meta data attached to a list of files, but also to
        all their parents
        """
        resDict = {"Successful": {}, "Failed": {}}
        dids = []
        lfnChunks = breakListIntoChunks(lfns, 1000)
        for lfnList in lfnChunks:
            try:
                dids = [self.__getDidsFromLfn(lfn) for lfn in lfnList]
            except Exception as err:
                return S_ERROR(str(err))
            try:
                for met in self.client.get_metadata_bulk(dids=dids, inherit=True):
                    lfn = met["name"]
                    resDict["Successful"][lfn] = met
                for lfn in lfnList:
                    if lfn not in resDict["Successful"]:
                        resDict["Failed"][lfn] = "No such file or directory"
            except Exception as err:
                return S_ERROR(str(err))
        return S_OK(resDict)

    @checkCatalogArguments
    def setMetadataBulk(self, pathMetadataDict):
        """Add metadata for the given paths"""
        resDict = {"Successful": {}, "Failed": {}}
        dids = []
        for path, metadataDict in pathMetadataDict.items():
            try:
                did = self.__getDidsFromLfn(path)
                did["meta"] = metadataDict
                dids.append(did)
            except Exception as err:
                return S_ERROR(str(err))
        try:
            self.client.set_dids_metadata_bulk(dids=dids, recursive=False)
        except Exception as err:
            return S_ERROR(str(err))
        return S_OK(resDict)

    @checkCatalogArguments
    def setMetadata(self, path, metadataDict):
        """Add metadata to the given path"""
        pathMetadataDict = {}
        path = next(iter(path))
        pathMetadataDict[path] = metadataDict
        return self.setMetadataBulk(pathMetadataDict)

    @checkCatalogArguments
    def removeMetadata(self, path, metadata):
        """Remove the specified metadata for the given file"""
        resDict = {"Successful": {}, "Failed": {}}
        try:
            did = self.__getDidsFromLfn(path)
            failedMeta = {}
            # TODO : Implement bulk delete_metadata method in Rucio
            for meta in metadata:
                try:
                    self.client.delete_metadata(scope=did["scope"], name=did["name"], key=meta)
                except DataIdentifierNotFound:
                    return S_ERROR(f"File {path} not found")
                except Exception as err:
                    failedMeta[meta] = str(err)

            if failedMeta:
                metaExample = list(failedMeta)[0]
                result = S_ERROR(f"Failed to remove {len(failedMeta)} metadata, e.g. {failedMeta[metaExample]}")
                result["FailedMetadata"] = failedMeta
        except Exception as err:
            return S_ERROR(str(err))
        return S_OK()

    def findFilesByMetadata(self, metadataFilterDict, path="/", timeout=120):
        """find the dids for the given metadataFilterDict"""
        ruciometadataFilterDict = self.__transform_DIRAC_filter_dict_to_Rucio_filter_dict([metadataFilterDict])
        dids = []
        for scope in self.scopes:
            try:
                dids.extend(self.client.list_dids(scope=scope, filters=ruciometadataFilterDict, did_type="all"))
            except Exception as err:
                return S_ERROR(str(err))
        return S_OK(dids)

    def __transform_DIRAC_operator_to_Rucio(self, DIRAC_dict):
        """
        Transforms a DIRAC's metadata Query dictionary to a Rucio-compatible dictionary.
        This method takes a dictionary with DIRAC operators and converts it to a
        dictionary with Rucio-compatible operators based on predefined mappings.
        for example :
            input_dict={'key1': 'value1', 'key2': {'>': 10}, 'key3': {'=': 10}}
            return = {'key1': 'value1', 'key2.gt': 10, 'key3': 10}
        """
        rucio_dict = {}
        operator_mapping = {">": ".gt", "<": ".lt", ">=": ".gte", "<=": ".lte", "=<": ".lte", "!=": ".ne", "=": ""}

        for key, value in DIRAC_dict.items():
            if isinstance(value, dict):
                for operator, num in value.items():
                    if operator in operator_mapping:
                        mapped_operator = operator_mapping[operator]
                        rucio_dict[f"{key}{mapped_operator}"] = num
            else:
                rucio_dict[key] = value

        return rucio_dict

    def __transform_dict_with_in_operateur(self, DIRAC_dict_with_in_operator_list):
        """
        Transforms a list of DIRAC dictionaries containing 'in' operators into a combined list of dictionaries,
        expanding the 'in' operator into individual dictionaries while preserving other keys.
        example
            input_dict_list = [{'particle': {'in': ['proton','electron']},'site': {'in': [ "LaPalma", 'paranal']},'configuration_id': {'=': 14} }    ]
            return = [{'particle': 'proton', 'site': 'LaPalma', 'configuration_id': {'=': 14} }, {'particle': 'proton', 'site': 'paranal', 'configuration_id': {'=': 14} }, {'particle': 'electron', 'site': 'LaPalma', 'configuration_id': {'=': 14} }, {'particle': 'electron', 'site': 'paranal', 'configuration_id': {'=': 14} }]
        """
        if not isinstance(DIRAC_dict_with_in_operator_list, list):
            raise TypeError("DIRAC_dict_with_in_operator_list must be a list of dictionaries")

        combined_dict_list = []  # Final list of transformed dictionaries
        break_reached = False  # Boolean to track if 'in' was found and processed in any dictionary

        # Process each dictionary in the input list
        for DIRAC_dict_with_in_operator in DIRAC_dict_with_in_operator_list:
            if not isinstance(DIRAC_dict_with_in_operator, dict):
                raise TypeError("Each element in DIRAC_dict_with_in_operator_list must be a dictionary")

            in_key = None
            in_values = []

            # Extract the key with 'in' operator and the list of values
            for key, value in DIRAC_dict_with_in_operator.items():
                if isinstance(value, dict) and "in" in value:
                    in_key = key
                    in_values = value["in"]
                    break_reached = True  # 'in' operator found
                    break

            # If an 'in' key exists, expand the dictionary for each value
            if in_key:
                for val in in_values:
                    # Copy the original dictionary and replace the 'in' key
                    new_dict = DIRAC_dict_with_in_operator.copy()
                    new_dict[in_key] = val  # Replace the 'in' key with the current value
                    combined_dict_list.append(new_dict)
            else:
                # If no 'in' key, simply add the input dictionary as-is
                combined_dict_list.append(DIRAC_dict_with_in_operator)

        return combined_dict_list, break_reached

    def __transform_DIRAC_filter_dict_to_Rucio_filter_dict(self, DIRAC_filter_dict_list):
        """
        Transforms a list of DIRAC filter dictionaries into a list of Rucio filter dictionaries.
        This method takes a list of filter dictionaries used in DIRAC and converts them into a format
        that is compatible with Rucio. It handles the transformation of operators and expands filters
        that use the 'in' operator.
        example:
            input_dict_list = [{'particle': {'in': ['proton','electron']},'site': {'in': [ "LaPalma", 'paranal']},'configuration_id': {'=': 14} }    ]
            return = [{'particle': 'proton', 'site': 'LaPalma', 'configuration_id': 14}, {'particle': 'proton', 'site': 'paranal', 'configuration_id': 14}, {'particle': 'electron', 'site': 'LaPalma', 'configuration_id': 14}, {'particle': 'electron', 'site': 'paranal', 'configuration_id': 14}]
        """
        break_detected = True
        DIRAC_expanded_filters = DIRAC_filter_dict_list
        while break_detected:
            DIRAC_expanded_filters, break_detected = self.__transform_dict_with_in_operateur(DIRAC_expanded_filters)
        Rucio_filters = []
        for filter in DIRAC_expanded_filters:
            Rucio_filters.append(self.__transform_DIRAC_operator_to_Rucio(filter))
        return Rucio_filters
