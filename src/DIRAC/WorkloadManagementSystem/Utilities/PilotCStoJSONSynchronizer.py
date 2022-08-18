""" CStoJSONSynchronizer
  Module that keeps the pilot parameters file synchronized with the information
  in the Operations/Pilot section of the CS. If there are additions in the CS,
  these are incorporated to the file.
  The module uploads to a web server the latest version of the pilot scripts.
"""
import os
import glob
import shutil
import tarfile
import datetime

from git import Repo

from DIRAC import gLogger, gConfig, S_OK
from DIRAC.ConfigurationSystem.Client.ConfigurationData import gConfigurationData
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.ConfigurationSystem.Client.Helpers.Path import cfgPath


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
        self.pilotSetup = gConfig.getValue("/DIRAC/Setup", "")
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

    def getCSDict(self, includeMasterCS=True):
        """Gets minimal info for running a pilot, from the CS

        :returns: pilotDict (containing pilots run info)
        :rtype: S_OK, S_ERROR, value is pilotDict
        """

        pilotDict = {
            "timestamp": datetime.datetime.utcnow().isoformat(),
            "Setups": {},
            "CEs": {},
            "GenericPilotDNs": [],
        }

        self.log.info("-- Getting the content of the CS --")

        # These are in fact not only setups: they may be "Defaults" sections, or VOs, in multi-VOs installations
        setupsRes = gConfig.getSections("/Operations/")
        if not setupsRes["OK"]:
            self.log.error("Can't get sections from Operations", setupsRes["Message"])
            return setupsRes
        setupsInOperations = setupsRes["Value"]

        # getting the setup(s) in this CS, and comparing with what we found in Operations
        setupsInDIRACRes = gConfig.getSections("DIRAC/Setups")
        if not setupsInDIRACRes["OK"]:
            self.log.error("Can't get sections from DIRAC/Setups", setupsInDIRACRes["Message"])
            return setupsInDIRACRes
        setupsInDIRAC = setupsInDIRACRes["Value"]

        # Handling the case of multi-VO CS
        if not set(setupsInDIRAC).intersection(set(setupsInOperations)):
            vos = list(setupsInOperations)
            for vo in vos:
                setupsFromVOs = gConfig.getSections("/Operations/%s" % vo)
                if not setupsFromVOs["OK"]:
                    continue
                else:
                    setupsInOperations = setupsFromVOs["Value"]

        self.log.verbose("From Operations/[Setup]/Pilot")

        for setup in setupsInOperations:
            self._getPilotOptionsPerSetup(setup, pilotDict)

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
                    # This CEType is like 'HTCondor' or 'ARC' etc.
                    ceType = gConfig.getValue(cfgPath("/Resources", "Sites", grid, site, "CEs", ce, "CEType"))
                    if ceType is None:
                        # Skip but log it
                        self.log.error("CE has no option CEType!", ce + " at " + site)
                        pilotDict["CEs"][ce] = {"Site": site}
                    else:
                        pilotDict["CEs"][ce] = {"Site": site, "GridCEType": ceType}

                    # This LocalCEType is like 'InProcess' or 'Pool' or 'Pool/Singularity' etc.
                    # It can be in the queue and/or the CE level
                    localCEType = gConfig.getValue(cfgPath("/Resources", "Sites", grid, site, "CEs", ce, "LocalCEType"))
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
            configurationServers = list(set(configurationServers) - {masterCS})
        pilotDict["ConfigurationServers"] = configurationServers

        self.log.debug("Got pilotDict", str(pilotDict))

        return S_OK(pilotDict)

    def _getPilotOptionsPerSetup(self, setup, pilotDict):
        """Given a setup, returns its pilot options in a dictionary"""

        options = gConfig.getOptionsDict("/Operations/%s/Pilot" % setup)
        if not options["OK"]:
            self.log.warn("Section does not exist: skipping", "/Operations/%s/Pilot " % setup)
            return

        # We include everything that's in the Pilot section for this setup
        if setup == self.pilotSetup:
            self.pilotVOVersion = options["Value"]["Version"]
        pilotDict["Setups"][setup] = options["Value"]
        # We update separately 'GenericPilotDNs'
        try:
            pilotDict["GenericPilotDNs"].append(pilotDict["Setups"][setup]["GenericPilotDN"])
        except KeyError:
            pass
        ceTypesCommands = gConfig.getOptionsDict("/Operations/%s/Pilot/Commands" % setup)
        if ceTypesCommands["OK"]:
            # It's ok if the Pilot section doesn't list any Commands too
            pilotDict["Setups"][setup]["Commands"] = {}
            for ceType in ceTypesCommands["Value"]:
                # FIXME: inconsistent that we break Commands down into a proper list but other things are comma-list strings
                pilotDict["Setups"][setup]["Commands"][ceType] = ceTypesCommands["Value"][ceType].split(", ")
                # pilotDict['Setups'][setup]['Commands'][ceType] = ceTypesCommands['Value'][ceType]
        if "CommandExtensions" in pilotDict["Setups"][setup]:
            # FIXME: inconsistent that we break CommandExtensionss down into a proper
            # list but other things are comma-list strings
            pilotDict["Setups"][setup]["CommandExtensions"] = pilotDict["Setups"][setup]["CommandExtensions"].split(
                ", "
            )
            # pilotDict['Setups'][setup]['CommandExtensions'] = pilotDict['Setups'][setup]['CommandExtensions']

        # Getting the details aboout the MQ Services to be used for logging, if any
        if "LoggingMQService" in pilotDict["Setups"][setup]:
            loggingMQService = gConfig.getOptionsDict(
                "/Resources/MQServices/%s" % pilotDict["Setups"][setup]["LoggingMQService"]
            )
            if not loggingMQService["OK"]:
                self.log.error(loggingMQService["Message"])
                return loggingMQService
            pilotDict["Setups"][setup]["Logging"] = {}
            pilotDict["Setups"][setup]["Logging"]["Host"] = loggingMQService["Value"]["Host"]
            pilotDict["Setups"][setup]["Logging"]["Port"] = loggingMQService["Value"]["Port"]

            loggingMQServiceQueuesSections = gConfig.getSections(
                "/Resources/MQServices/%s/Queues" % pilotDict["Setups"][setup]["LoggingMQService"]
            )
            if not loggingMQServiceQueuesSections["OK"]:
                self.log.error(loggingMQServiceQueuesSections["Message"])
                return loggingMQServiceQueuesSections
            pilotDict["Setups"][setup]["Logging"]["Queue"] = {}

            for queue in loggingMQServiceQueuesSections["Value"]:
                loggingMQServiceQueue = gConfig.getOptionsDict(
                    "/Resources/MQServices/{}/Queues/{}".format(pilotDict["Setups"][setup]["LoggingMQService"], queue)
                )
                if not loggingMQServiceQueue["OK"]:
                    self.log.error(loggingMQServiceQueue["Message"])
                    return loggingMQServiceQueue
                pilotDict["Setups"][setup]["Logging"]["Queue"][queue] = loggingMQServiceQueue["Value"]

            queuesRes = gConfig.getSections(
                "/Resources/MQServices/%s/Queues" % pilotDict["Setups"][setup]["LoggingMQService"]
            )
            if not queuesRes["OK"]:
                return queuesRes
            queues = queuesRes["Value"]
            queuesDict = {}
            for queue in queues:
                queueOptionRes = gConfig.getOptionsDict(
                    "/Resources/MQServices/{}/Queues/{}".format(pilotDict["Setups"][setup]["LoggingMQService"], queue)
                )
                if not queueOptionRes["OK"]:
                    return queueOptionRes
                queuesDict[queue] = queueOptionRes["Value"]
            pilotDict["Setups"][setup]["Logging"]["Queues"] = queuesDict

    def syncScripts(self):
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
                repo_VO.git.checkout("upstream/%s" % self.pilotVORepoBranch, b="pilotVOScripts")
            scriptDir = os.path.join(pilotVOLocalRepo, self.projectDir, self.pilotVOScriptPath, "*.py")
            for fileVO in glob.glob(scriptDir):
                tarFiles.append(fileVO)
        else:
            self.log.info("The /Operations/<Setup>/Pilot/pilotVORepo option is not defined, using Vanilla DIRAC pilot")

        # DIRAC repo
        pilotLocalRepo = os.path.join(self.workDir, "pilotLocalRepo")
        if os.path.isdir(pilotLocalRepo):
            shutil.rmtree(pilotLocalRepo)
        os.mkdir(pilotLocalRepo)
        repo = Repo.init(pilotLocalRepo)
        upstream = repo.create_remote("upstream", self.pilotRepo)
        upstream.fetch()
        upstream.pull(upstream.refs[0].remote_head)
        repo.git.checkout("upstream/%s" % self.pilotRepoBranch, b="pilotScripts")

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
