"""CStoJSONSynchronizer
Module that keeps the pilot parameters file synchronized with the information
in the Operations/Pilot section of the CS. If there are additions in the CS,
these are incorporated to the file.
The module uploads to a web server the latest version of the pilot scripts.
"""

import datetime
import glob
import os
import shutil
import tarfile
from typing import Any
from git import Repo

from DIRAC import S_OK, gConfig, gLogger
from DIRAC.ConfigurationSystem.Client.ConfigurationData import gConfigurationData
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.ConfigurationSystem.Client.Helpers.Path import cfgPath
from DIRAC.Core.Utilities.ReturnValues import DOKReturnType, DReturnType

import socket
from urllib.parse import urlparse


def exclude_master_cs_aliases(urls: list[str], master_cs_url: str) -> list[str]:
    """
    Excludes URLs that are DNS aliases of the given MasterCS server URL.

    This function resolves the IP addresses of the MasterCS server and each URL in the input list.
    It returns a new list containing only those URLs whose hostnames do not resolve to any of the
    MasterCS server's IP addresses, effectively excluding all DNS aliases of the MasterCS server.

    Args:
        urls (list[str]): A list of URLs to filter. Each URL should be a string in a valid URL format.
        master_cs_url (str): The reference URL (e.g., MasterCS server URL) whose DNS aliases are to be excluded.

    Returns:
        list[str]: A new list of URLs with all aliases of the MasterCS server removed.
                  If the MasterCS hostname cannot be resolved, the original list is returned unchanged.

    Example:
        >>> urls = [
        ...     'dips://lbvobox303.cern.ch:9135/Configuration/Server',
        ...     'dips://ccwlcglhcb02.in2p3.fr:9135/Configuration/Server',
        ...     'dips://lbvobox302.cern.ch:9135/Configuration/Server',
        ... ]
        >>> master_cs_url = "dips://mastercs.cern.ch:9135/Configuration/Server"
        >>> exclude_master_cs_aliases(urls, master_cs_url)
        ['dips://ccwlcglhcb02.in2p3.fr:9135/Configuration/Server']

    Notes:
        - If the MasterCS hostname cannot be resolved, the function returns the original list.
        - If a hostname in the input list cannot be resolved, it is included in the result.
        - The comparison is based on IP addresses, not hostnames.
    """
    master_cs_hostname = urlparse(master_cs_url).hostname
    if not master_cs_hostname:
        return urls

    # Resolve IP addresses for the MasterCS hostname
    try:
        master_cs_ips = set(socket.gethostbyname_ex(master_cs_hostname)[2])
    except socket.gaierror:
        return urls

    # Function to get IPs for a hostname
    def get_ips(hostname):
        try:
            return set(socket.gethostbyname_ex(hostname)[2])
        except socket.gaierror:
            return set()

    filtered_urls = []
    for url in urls:
        hostname = urlparse(url).hostname
        ips = get_ips(hostname)
        if not ips & master_cs_ips:
            filtered_urls.append(url)

    return filtered_urls


class PilotCStoJSONSynchronizer:
    """
    2 functions are executed:
    - It updates a JSON file with the values on the CS which can be used by Pilot3 pilots
    - It updates the pilot 3 files
    This synchronizer can be triggered at any time via PilotCStoJSONSynchronizer().sync().
    """

    def __init__(self):
        """c'tor
        Just setting defaults
        """
        self.workDir = ""  # Working directory where the files are going to be stored

        # domain name of the web server(s) used to upload the pilot json file and the pilot scripts
        self.pilotFileServer = ""

        # pilot sync default parameters
        self.pilotRepo = "https://github.com/DIRACGrid/Pilot.git"  # repository of the pilot
        self.pilotVORepo = ""  # repository of the VO that can contain a pilot extension
        self.projectDir = ""
        # where the find the pilot scripts in the VO pilot repository
        self.pilotScriptPath = "Pilot"  # where the find the pilot scripts in the pilot repository
        self.pilotVOScriptPath = ""
        self.pilotRepoBranch = "master"
        self.pilotVORepoBranch = "master"

        self.log = gLogger.getSubLogger(__name__)

        ops = Operations()

        # Overriding parameters from the CS
        self.pilotRepo = ops.getValue("Pilot/pilotRepo", self.pilotRepo)
        self.pilotVORepo = ops.getValue("Pilot/pilotVORepo", self.pilotVORepo)
        self.projectDir = ops.getValue("Pilot/projectDir", self.projectDir)
        self.pilotScriptPath = ops.getValue("Pilot/pilotScriptsPath", self.pilotScriptPath)
        self.pilotVOScriptPath = ops.getValue("Pilot/pilotVOScriptsPath", self.pilotVOScriptPath)
        self.pilotRepoBranch = ops.getValue("Pilot/pilotRepoBranch", self.pilotRepoBranch)
        self.pilotVORepoBranch = ops.getValue("Pilot/pilotVORepoBranch", self.pilotVORepoBranch)

    def getCSDict(self, includeMasterCS: bool = True) -> DReturnType[Any]:
        """
        Gets minimal info for running a pilot, from the CS. The complete Operations section is
        dumped to a dictionary. A decision which VO to use will be delegated to a pilot.

        :returns: pilotDict (containing pilots run info)
        :rtype: S_OK, S_ERROR, value is pilotDict
        """

        pilotDict = {
            "timestamp": datetime.datetime.utcnow().isoformat(),
            "CEs": {},
            "GenericPilotDNs": [],
        }

        self.log.info("-- Getting the content of the CS --")

        # Get the whole Operations section as a dict.
        self.log.verbose("From Operations (whole section)")
        opRes = gConfig.getOptionsDictRecursively("/Operations")
        if not opRes["OK"]:
            self.log.error("Can't get sections from Operations", opRes["Message"])
            return opRes
        pilotDict.update(opRes["Value"])

        # we still need a pilotVOVersion
        self.opsHelper = Operations()
        self.pilotVOVersion = self.opsHelper.getValue("/Pilot/Version")
        # if self.pilotVORepo is defined and self.pilotVOVersion is not, syncScripts is likely to fail.
        if self.pilotVOVersion is None and self.pilotVORepo:
            self.log.error("Pilot VO repo is set in the CS but the pilot VO version is not. Expect problems ahead")

        self.log.verbose("From Resources/Sites")
        sitesSection = gConfig.getSections("/Resources/Sites/")
        if not sitesSection["OK"]:
            self.log.error("Can't get sections from Resources", sitesSection["Message"])
            return sitesSection

        for grid in sitesSection["Value"]:
            gridSection = gConfig.getSections("/Resources/Sites/" + grid)
            if not gridSection["OK"]:
                self.log.error("Can't get sections from Resources", gridSection["Message"])
                return gridSection

            for site in gridSection["Value"]:
                ceList = gConfig.getSections(cfgPath("/Resources", "Sites", grid, site, "CEs"))
                if not ceList["OK"]:
                    # Skip but log it
                    self.log.error("Site has no CEs! - skipping", site)
                    continue

                for ce in ceList["Value"]:
                    # This CEType is like 'HTCondor' or 'AREX' etc.
                    ceType = gConfig.getValue(cfgPath("/Resources", "Sites", grid, site, "CEs", ce, "CEType"))
                    if ceType is None:
                        # Skip but log it
                        self.log.error("CE has no option CEType!", ce + " at " + site)
                        pilotDict["CEs"][ce] = {"Site": site}
                    else:
                        pilotDict["CEs"][ce] = {"Site": site, "GridCEType": ceType}

                    # This LocalCEType is like 'InProcess' or 'Pool' or 'Pool/Singularity' etc.
                    # It can be at the installation, queue and/or CE level
                    defaultLocalCEType = gConfig.getValue("/Resources/Computing/DefaultLocalCEType", "")
                    localCEType = gConfig.getValue(
                        cfgPath("/Resources", "Sites", grid, site, "CEs", ce, "LocalCEType"), defaultLocalCEType
                    )
                    if localCEType is not None:
                        pilotDict["CEs"][ce].setdefault("LocalCEType", localCEType)

                    res = gConfig.getSections(cfgPath("/Resources", "Sites", grid, site, "CEs", ce, "Queues"))
                    if not res["OK"]:
                        # Skip but log it
                        self.log.error("No queues found for CE", ce + ": " + res["Message"])
                        continue
                    queueList = res["Value"]
                    for queue in queueList:
                        localCEType = gConfig.getValue(
                            cfgPath("/Resources", "Sites", grid, site, "CEs", ce, "Queues", queue, "LocalCEType")
                        )
                        if localCEType is not None:
                            pilotDict["CEs"][ce].setdefault(queue, {"LocalCEType": localCEType})

        defaultSetup = gConfig.getValue("/DIRAC/DefaultSetup")
        if defaultSetup:
            pilotDict["DefaultSetup"] = defaultSetup

        self.log.debug("From DIRAC/Configuration")
        configurationServers = gConfig.getServersList()
        if not includeMasterCS:
            masterCS = gConfigurationData.getMasterServer()
            configurationServers = exclude_master_cs_aliases(configurationServers, masterCS)

        pilotDict["ConfigurationServers"] = configurationServers

        preferredURLPatterns = gConfigurationData.extractOptionFromCFG("/DIRAC/PreferredURLPatterns")
        if preferredURLPatterns:
            pilotDict["PreferredURLPatterns"] = preferredURLPatterns

        self.log.debug("Got pilotDict", str(pilotDict))

        return S_OK(pilotDict)

    def syncScripts(self) -> DOKReturnType[Any]:
        """Clone the pilot scripts from the Pilot repositories (handle also extensions)"""
        tarFiles = []

        # Extension, if it exists
        if self.pilotVORepo:
            pilotVOLocalRepo = os.path.join(self.workDir, "pilotVOLocalRepo")
            if os.path.isdir(pilotVOLocalRepo):
                shutil.rmtree(pilotVOLocalRepo)
            os.mkdir(pilotVOLocalRepo)
            repo_VO = Repo.init(pilotVOLocalRepo)
            upstream = repo_VO.create_remote("upstream", self.pilotVORepo)
            upstream.fetch()
            upstream.pull(upstream.refs[0].remote_head)
            if repo_VO.tags:
                repo_VO.git.checkout(repo_VO.tags[self.pilotVOVersion], b="pilotVOScripts")
            else:
                repo_VO.git.checkout(f"upstream/{self.pilotVORepoBranch}", b="pilotVOScripts")
            scriptDir = os.path.join(pilotVOLocalRepo, self.projectDir, self.pilotVOScriptPath, "*.py")
            for fileVO in glob.glob(scriptDir):
                tarFiles.append(fileVO)
        else:
            self.log.info("The /Operations/Pilot/pilotVORepo option is not defined, using Vanilla DIRAC pilot")

        # DIRAC repo
        pilotLocalRepo = os.path.join(self.workDir, "pilotLocalRepo")
        if os.path.isdir(pilotLocalRepo):
            shutil.rmtree(pilotLocalRepo)
        os.mkdir(pilotLocalRepo)
        repo = Repo.init(pilotLocalRepo)
        upstream = repo.create_remote("upstream", self.pilotRepo)
        upstream.fetch()
        upstream.pull(upstream.refs[0].remote_head)
        repo.git.checkout(f"upstream/{self.pilotRepoBranch}", b="pilotScripts")

        scriptDir = os.path.join(pilotLocalRepo, self.pilotScriptPath, "*.py")
        for filename in glob.glob(scriptDir):
            tarFiles.append(filename)

        tarPath = os.path.join(self.workDir, "pilot.tar")
        with tarfile.TarFile(name=tarPath, mode="w") as tf:
            for ptf in tarFiles:
                # This copy makes sure that all the files in the tarball are accessible
                # in the work directory. It should be kept
                shutil.copyfile(ptf, os.path.join(self.workDir, os.path.basename(ptf)))
                tf.add(ptf, arcname=os.path.basename(ptf), recursive=False)

        tarFilesPaths = [os.path.join(self.workDir, os.path.basename(tarredF)) for tarredF in tarFiles]

        return S_OK((tarPath, tarFilesPaths))
