""" This agent syncs CS and pilot files to a web server of your choice

.. literalinclude:: ../ConfigTemplate.cfg
  :start-after: ##BEGIN PilotSyncAgent
  :end-before: ##END
  :dedent: 2
  :caption: PilotsSyncAgent options

"""
import os
import json
import shutil
import hashlib
import requests

from DIRAC import S_OK
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.Core.Security.Locations import getHostCertificateAndKeyLocation, getCAsLocation
from DIRAC.DataManagementSystem.Client.DataManager import DataManager
from DIRAC.WorkloadManagementSystem.Utilities.PilotCStoJSONSynchronizer import PilotCStoJSONSynchronizer


class PilotSyncAgent(AgentModule):
    """Syncs CS and pilot files to a web server of your choice"""

    def __init__(self, *args, **kwargs):
        """c'tor"""
        super().__init__(*args, **kwargs)

        # This location would be enough if we are running this agent on the DIRAC web server
        # '/opt/dirac/webRoot/www/pilot'
        self.saveDir = ""
        self.uploadLocations = []
        self.includeMasterCS = True

    def initialize(self):
        """Initial settings"""
        self.workingDirectory = self.am_getOption("WorkDirectory")
        self.saveDir = self.am_getOption("SaveDirectory", self.saveDir)
        self.uploadLocations = self.am_getOption("UploadLocations", self.uploadLocations)
        includeMasterCS = self.am_getOption("IncludeMasterCS", self.includeMasterCS)
        if isinstance(includeMasterCS, str) and includeMasterCS.lower() in ["n", "no", "false"]:
            self.includeMasterCS = False

        self.certAndKeyLocation = getHostCertificateAndKeyLocation()
        self.casLocation = getCAsLocation()

        return S_OK()

    def execute(self):
        """cycle"""

        ps = PilotCStoJSONSynchronizer()
        ps.workDir = self.workingDirectory

        self.log.verbose("Parameters for this sync:")
        self.log.verbose("repo=" + ps.pilotRepo)
        self.log.verbose("VO repo=" + ps.pilotVORepo)
        self.log.verbose("projectDir=" + ps.projectDir)
        self.log.verbose("pilotScriptsPath=" + ps.pilotScriptPath)
        self.log.verbose("pilotVOScriptsPath=" + ps.pilotVOScriptPath)
        self.log.verbose("pilotRepoBranch=" + ps.pilotRepoBranch)
        self.log.verbose("pilotVORepoBranch=" + ps.pilotVORepoBranch)

        # pilot.json
        res = ps.getCSDict(includeMasterCS=self.includeMasterCS)
        if not res["OK"]:
            return res
        pilotDict = res["Value"]
        print(json.dumps(pilotDict, indent=4, sort_keys=True))  # just print here as formatting is important
        with open(os.path.join(self.workingDirectory, "pilot.json"), "w") as jf:
            json.dump(pilotDict, jf)

        # pilot files
        res = ps.syncScripts()
        if not res["OK"]:
            return res
        tarPath, tarFiles = res["Value"]

        allFiles = [tarPath] + tarFiles + [os.path.join(self.workingDirectory, "pilot.json")]

        # checksums
        checksumDict = {}
        for pFile in allFiles:
            filename = os.path.basename(pFile)
            with open(pFile, "rb") as fp:
                checksumDict[filename] = hashlib.sha512(fp.read()).hexdigest()
            cksPath = os.path.join(self.workingDirectory, "checksums.sha512")
        with open(cksPath, "w") as chksums:
            for filename, chksum in sorted(checksumDict.items()):
                # same as the output from sha512sum commands
                chksums.write(f"{chksum}  {filename}\n")

        allFiles = list(set(allFiles + [cksPath]))

        if self.saveDir:
            # Moving files to the correct location
            self.log.info("Moving pilot files", "to %s" % self.saveDir)
            for tf in allFiles:
                # this overrides the destinations
                shutil.move(tf, os.path.join(self.saveDir, os.path.basename(tf)))

        # Here, attempting upload somewhere, and somehow
        for server in self.uploadLocations:
            self.log.info("Attempting to upload", "to %s" % server)
            if server.startswith("https://"):
                for tf in allFiles:
                    res = requests.put(server, data=tf, verify=self.casLocation, cert=self.certAndKeyLocation)
                    if res.status_code not in (200, 202):
                        self.log.error("Could not upload", f"to {server}: status {res.status_code}")
            else:  # Assumes this is a DIRAC SE
                for tf in allFiles:
                    res = DataManager().put(lfn=tf, fileName=tf, diracSE=server)
                    if not res["OK"]:
                        self.log.error("Could not upload", "to {}: {}".format(server, res["Message"]))

        return S_OK()
