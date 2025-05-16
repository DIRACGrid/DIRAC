""" SingularityCE is a type of "inner" CEs
    (meaning it's used by a jobAgent inside a pilot).
    A computing element class using singularity containers,
    where Singularity is supposed to be found on the WN.

    The goal of this CE is to start the job in the container set by
    the "ContainerRoot" config option.

    DIRAC can be re-installed within the container.

    See the Configuration/Resources/Computing documention for details on
    where to set the option parameters.
"""
import json
import os
import re
import shutil
import sys
import tempfile
from pathlib import Path

import DIRAC
from DIRAC import S_ERROR, S_OK, gConfig, gLogger
from DIRAC.ConfigurationSystem.Client.Helpers import Operations
from DIRAC.Core.Utilities.CGroups2 import CG2Manager
from DIRAC.Core.Utilities.ThreadScheduler import gThreadScheduler
from DIRAC.Resources.Computing.ComputingElement import ComputingElement
from DIRAC.Resources.Storage.StorageElement import StorageElement
from DIRAC.WorkloadManagementSystem.Utilities.Utils import createJobWrapper

# Default container to use if it isn't specified in the CE options
CONTAINER_DEFROOT = "/cvmfs/cernvm-prod.cern.ch/cvm4"
CONTAINER_WORKDIR = "DIRAC_containers"
CONTAINER_INNERDIR = "/tmp"


# What is executed inside the container (2 options given)

CONTAINER_WRAPPER_INSTALL = """#!/bin/bash

echo "Starting inner container wrapper scripts at `date`."
set -ex
cd /tmp
# Install DIRAC
installer_name="DIRACOS-Linux-$(uname -m).sh"
if [[ -d /cvmfs/dirac.egi.eu/installSource/ ]]; then
  bash /cvmfs/dirac.egi.eu/installSource/"${installer_name}"
else
  curl -LO "https://github.com/DIRACGrid/DIRACOS2/releases/latest/download/${installer_name}"
  bash "${installer_name}"
  rm "${installer_name}"
fi
source diracos/diracosrc
pip install %(dirac_project)s==%(version)s
dirac-configure -F %(config_args)s -I
# Add compatibility with pilot3 where config is in pilot.cfg
ln -s diracos/etc/dirac.cfg pilot.cfg
# Run next wrapper (to start actual job)
bash %(next_wrapper)s
# Write the payload errorcode to a file for the outer scripts
echo $? > retcode
chmod 644 retcode
echo "Finishing inner container wrapper scripts at `date`."

"""
CONTAINER_WRAPPER_NO_INSTALL = """#!/bin/bash

echo "Starting inner container wrapper scripts (no install) at `date`."
set -x
cd /tmp
export DIRAC=%(dirac_env_var)s
export DIRACOS=%(diracos_env_var)s
# In any case we need to find a bashrc, and a pilot.cfg, both created by the pilot
source %(rc_script)s
# Run next wrapper (to start actual job)
bash %(next_wrapper)s
# Write the payload errorcode to a file for the outer scripts
echo $? > retcode
chmod 644 retcode
echo "Finishing inner container wrapper scripts at `date`."

"""


ENV_VAR_WHITELIST = [
    r"TERM",
    r"VOMS_.*",
    r"X509_.*",
    r"XRD_.*",
    r"Xrd.*",
    r"DIRAC_.*",
    r"BEARER_TOKEN.*",
]
ENV_VAR_WHITELIST = re.compile(r"^(" + r"|".join(ENV_VAR_WHITELIST) + r")$")


class SingularityComputingElement(ComputingElement):
    """A Computing Element for running a job within a Singularity container."""

    def __init__(self, ceUniqueID):
        """Standard constructor."""
        super().__init__(ceUniqueID)
        self.__submittedJobs = 0
        self.__runningJobs = 0
        self.__root = CONTAINER_DEFROOT
        if "ContainerRoot" in self.ceParameters:
            self.__root = self.ceParameters["ContainerRoot"]
        self.__workdir = CONTAINER_WORKDIR
        self.__innerdir = CONTAINER_INNERDIR
        self.__installDIRACInContainer = self.ceParameters.get("InstallDIRACInContainer", False)
        if isinstance(self.__installDIRACInContainer, str) and self.__installDIRACInContainer.lower() in (
            "false",
            "no",
        ):
            self.__installDIRACInContainer = False

        self.processors = int(self.ceParameters.get("NumberOfProcessors", 1))

    @staticmethod
    def __findInstallBaseDir():
        """Find the path to root of the current DIRAC installation"""
        return os.path.realpath(sys.base_prefix)

    def __getInstallFlags(self, infoDict=None):
        """Get the flags for installing inside the container."""

        if not infoDict:
            infoDict = {}

        setup = infoDict.get("DefaultSetup")
        if not setup:
            setup = list(infoDict.get("Setups"))[0]
        setup = str(setup)

        diracProject = "DIRAC"

        project = str(infoDict.get("Project"))
        if not project or project == "None":
            diracProject = Operations.Operations(setup=setup).getValue("Pilot/Project", "") + diracProject

        diracVersions = str(infoDict["Setups"][setup].get("Version")).split(",")
        if not diracVersions:
            diracVersions = str(infoDict["Setups"]["Defaults"].get("Version")).split(",")
        if not diracVersions:
            diracVersions = Operations.Operations(setup=setup).getValue("Pilot/Version", [])
        version = diracVersions[0].strip()

        return diracProject, version

    @staticmethod
    def __getConfigFlags(infoDict=None):
        """Get the flags for dirac-configure inside the container.
        Returns a string containing the command line flags.
        """
        if not infoDict:
            infoDict = {}

        cfgOpts = []

        setup = infoDict.get("DefaultSetup")
        cfgOpts.append(f"-S '{setup}'")

        csServers = infoDict.get("ConfigurationServers")
        if not csServers:
            csServers = gConfig.getValue("/DIRAC/Configuration/Servers", [])
        cfgOpts.append(f"-C '{','.join([str(ce) for ce in csServers])}'")
        cfgOpts.append(f"-n '{DIRAC.siteName()}'")
        return " ".join(cfgOpts)

    def __createWorkArea(self, jobDesc=None, log=None, logLevel="INFO", proxy=None):
        """Creates a directory for the container and populates it with the
        template directories, scripts & proxy.
        """
        if not jobDesc:
            jobDesc = {}
        if not log:
            log = gLogger

        # Create the directory for our container area
        try:
            os.mkdir(self.__workdir)
        except OSError:
            if not os.path.isdir(self.__workdir):
                return S_ERROR(f"Failed to create container base directory '{self.__workdir}'")
            # Otherwise, directory probably just already exists...
        baseDir = None
        try:
            baseDir = tempfile.mkdtemp(prefix=f"job{jobDesc.get('jobID', 0)}_", dir=self.__workdir)
        except OSError:
            return S_ERROR(f"Failed to create container work directory in '{self.__workdir}'")

        self.log.debug(f"Use singularity workarea: {baseDir}")
        for subdir in ["home", "tmp", "var_tmp"]:
            os.mkdir(os.path.join(baseDir, subdir))
        tmpDir = os.path.join(baseDir, "tmp")

        # Now we have a directory, we can stage in the proxy and scripts
        # Proxy
        if proxy:
            proxyLoc = os.path.join(tmpDir, "proxy")
            rawfd = os.open(proxyLoc, os.O_WRONLY | os.O_CREAT, 0o600)
            fd = os.fdopen(rawfd, "w")
            fd.write(proxy)
            fd.close()
        else:
            self.log.warn("No user proxy")

        # Job Wrapper (Standard-ish DIRAC wrapper)
        result = createJobWrapper(
            wrapperPath=tmpDir,
            rootLocation=self.__innerdir,
            jobID=jobDesc.get("jobID", 0),
            jobParams=jobDesc.get("jobParams", {}),
            resourceParams=jobDesc.get("resourceParams", {}),
            optimizerParams=jobDesc.get("optimizerParams", {}),
            pythonPath="python",
            log=log,
            logLevel=logLevel,
            extraOptions="" if self.__installDIRACInContainer else "/tmp/pilot.cfg",
        )
        if not result["OK"]:
            return result
        wrapperPath = result["Value"].get("JobExecutableRelocatedPath")

        if self.__installDIRACInContainer:
            infoDict = None
            if os.path.isfile("pilot.json"):  # if this is a pilot 3 this file should be found
                with open("pilot.json") as pj:
                    infoDict = json.load(pj)

            # Extra Wrapper (Container DIRAC installer)
            installFlags = self.__getInstallFlags(infoDict)
            wrapSubs = {
                "next_wrapper": wrapperPath,
                "dirac_project": installFlags[0],
                "version": installFlags[1],
                "config_args": self.__getConfigFlags(infoDict),
            }
            CONTAINER_WRAPPER = CONTAINER_WRAPPER_INSTALL

        else:  # In case we don't (re)install DIRAC
            wrapSubs = {
                "next_wrapper": wrapperPath,
                "dirac_env_var": os.environ.get("DIRAC", ""),
                "diracos_env_var": os.environ.get("DIRACOS", ""),
            }
            wrapSubs["rc_script"] = os.path.join(self.__findInstallBaseDir(), "diracosrc")
            shutil.copyfile("pilot.cfg", os.path.join(tmpDir, "pilot.cfg"))
            CONTAINER_WRAPPER = CONTAINER_WRAPPER_NO_INSTALL

        wrapLoc = os.path.join(tmpDir, "dirac_container.sh")
        rawfd = os.open(wrapLoc, os.O_WRONLY | os.O_CREAT, 0o700)
        fd = os.fdopen(rawfd, "w")
        fd.write(CONTAINER_WRAPPER % wrapSubs)
        fd.close()

        ret = S_OK()
        ret["baseDir"] = baseDir
        ret["tmpDir"] = tmpDir
        if proxy:
            ret["proxyLocation"] = proxyLoc
        return ret

    def __deleteWorkArea(self, baseDir):
        """Deletes the container work area (baseDir path) unless 'KeepWorkArea'
        option is set. Returns None.
        """
        if self.ceParameters.get("KeepWorkArea", False):
            return
        # We can't really do anything about errors: The tree should be fully owned
        # by the pilot user, so we don't expect any permissions problems.
        shutil.rmtree(baseDir, ignore_errors=True)

    def __getEnv(self):
        """Gets the environment for use within the container.
        We blank almost everything to prevent contamination from the host system.
        """

        if self.__installDIRACInContainer:
            payloadEnv = {}
        else:
            payloadEnv = {k: v for k, v in os.environ.items() if ENV_VAR_WHITELIST.match(k)}

        payloadEnv["PATH"] = str(Path(sys.executable).parent)
        payloadEnv["TMP"] = "/tmp"
        payloadEnv["TMPDIR"] = "/tmp"
        payloadEnv["X509_USER_PROXY"] = os.path.join(self.__innerdir, "proxy")
        payloadEnv["DIRACSYSCONFIG"] = os.path.join(self.__innerdir, "pilot.cfg")

        return payloadEnv

    @staticmethod
    def __checkResult(tmpDir):
        """Gets the result of the payload command and returns it."""
        # The wrapper writes the inner job return code to "retcode"
        # in the working directory.
        try:
            with open(os.path.join(tmpDir, "retcode")) as fp:
                retCode = int(fp.read())
        except (OSError, ValueError):
            # Something failed while trying to get the return code
            return S_ERROR("Failed to get return code from inner wrapper")

        return S_OK(retCode)

    def submitJob(self, executableFile, proxy=None, **kwargs):
        """Start a container for a job.
        executableFile is ignored. A new wrapper suitable for running in a
        container is created from jobDesc.

        :return: S_OK(payload exit code) / S_ERROR() if submission issue
        """
        rootImage = self.__root
        renewTask = None

        self.log.info("Creating singularity container")

        # Start by making the directory for the container
        ret = self.__createWorkArea(kwargs.get("jobDesc"), kwargs.get("log"), kwargs.get("logLevel", "INFO"), proxy)
        if not ret["OK"]:
            return ret
        baseDir = ret["baseDir"]
        tmpDir = ret["tmpDir"]

        if proxy:
            payloadProxyLoc = ret["proxyLocation"]

            # Now we have to set-up payload proxy renewal for the container
            # This is fairly easy as it remains visible on the host filesystem
            result = gThreadScheduler.addPeriodicTask(
                self.proxyCheckPeriod, self._monitorProxy, taskArgs=(payloadProxyLoc,), executions=0, elapsedTime=0
            )
            if result["OK"]:
                renewTask = result["Value"]
            else:
                self.log.warn("Failed to start proxy renewal task")

        # Very simple accounting
        self.__submittedJobs += 1
        self.__runningJobs += 1

        # Now prepare start singularity
        # Mount /cvmfs in if it exists on the host
        withCVMFS = os.path.isdir("/cvmfs")
        innerCmd = os.path.join(self.__innerdir, "dirac_container.sh")
        outerCmd = ["apptainer", "exec"]
        outerCmd.extend(["--contain"])  # use minimal /dev and empty other directories (e.g. /tmp and $HOME)
        outerCmd.extend(["--ipc"])  # run container in a new IPC namespace
        outerCmd.extend(["--workdir", baseDir])  # working directory to be used for /tmp, /var/tmp and $HOME
        outerCmd.extend(["--home", "/tmp"])  # Avoid using small tmpfs for default $HOME and use scratch /tmp instead
        outerCmd.append("--userns")
        if withCVMFS:
            outerCmd.extend(["--bind", "/cvmfs"])
        if not self.__installDIRACInContainer:
            outerCmd.extend(["--bind", "{0}:{0}:ro".format(self.__findInstallBaseDir())])

        rawBindPaths = self.ceParameters.get("ContainerBind", "")
        bindPaths = rawBindPaths.split(",") if rawBindPaths else []
        siteName = gConfig.getValue("/LocalSite/Site", "")
        ceName = gConfig.getValue("/LocalSite/GridCE", "")
        if siteName and ceName:
            gridName = siteName.split(".")[0]
            bindPaths.extend(
                gConfig.getValue(
                    f"/Resources/Sites/{gridName}/{siteName}/ContainerBind",
                    [],
                )
            )
            bindPaths.extend(
                gConfig.getValue(
                    "/Resources/Sites/{gridName}/{siteName}/CEs/{ceName}/ContainerBind".format(
                        gridName=gridName, siteName=siteName, ceName=ceName
                    ),
                    [],
                )
            )

        # Check if there is a locally mounted filesystem
        # such that we bind mount it in the container too
        localSEs = gConfig.getValue("/LocalSite/LocalSE", [])
        for seName in localSEs:
            try:
                # Find the base path if a File protocol is defined
                se = StorageElement(seName)
                if not se.valid:
                    self.log.warn(f"Storage Element {seName} not valid")
                    continue

                mountedPath = se.getStorageParameters(protocol="file")["Value"]["Path"]
                mountOptions = se.getStorageParameters(protocol="file")["Value"].get("MountOptions", "ro")
                if mountOptions not in ("rw", "ro"):
                    self.log.warn(f"Unknown mount mode: {mountOptions}")
                    continue

                bindPaths.append(f"{mountedPath}:{mountedPath}:{mountOptions}")
            except KeyError:
                pass

        for bindPath in bindPaths:
            if len(bindPath.split(":::")) == 1:
                outerCmd.extend(["--bind", bindPath.strip()])
            elif len(bindPath.split(":::")) in [2, 3]:
                outerCmd.extend(["--bind", ":".join([bp.strip() for bp in bindPath.split(":::")])])

        if "ContainerOptions" in self.ceParameters:
            containerOpts = self.ceParameters["ContainerOptions"].split(",")
            for opt in containerOpts:
                outerCmd.extend([opt.strip()])
        if not (os.path.isdir(rootImage) or os.path.isfile(rootImage)):
            # if we are here is because there's no image, or it is not accessible (e.g. not on CVMFS)
            self.log.error("Singularity image to exec not found: ", rootImage)
            return S_ERROR("Failed to find singularity image to exec")
        outerCmd.append(rootImage)
        cmd = outerCmd + [innerCmd]

        self.log.debug(f"Execute singularity command: {cmd}")
        self.log.debug(f"Execute singularity env: {self.__getEnv()}")
        result = CG2Manager().systemCall(
            0, cmd, callbackFunction=self.sendOutput, env=self.__getEnv(), ceParameters=self.ceParameters
        )

        self.__runningJobs -= 1

        if not result["OK"]:
            self.log.error("Fail to run Singularity", result["Message"])
            # If we fail to run the container try to run it again with verbose output
            # to help with debugging.
            self.log.error("Singularity command was: ", cmd)
            self.log.error(f"Singularity env was: {self.__getEnv()}")
            debugCmd = [outerCmd[0], "--debug"] + outerCmd[1:] + ["echo", "All okay"]
            self.log.error("Running with debug output to facilitate debugging", debugCmd)
            result = CG2Manager().systemCall(
                0, debugCmd, callbackFunction=self.sendOutput, env=self.__getEnv(), ceParameters=self.ceParameters
            )
            if proxy and renewTask:
                gThreadScheduler.removeTask(renewTask)
            self.__deleteWorkArea(baseDir)
            return S_ERROR("Error running singularity command")

        result = self.__checkResult(tmpDir)
        if proxy and renewTask:
            gThreadScheduler.removeTask(renewTask)
        self.__deleteWorkArea(baseDir)
        return result

    def getCEStatus(self):
        """Method to return information on running and pending jobs."""
        result = S_OK()
        result["SubmittedJobs"] = self.__submittedJobs
        result["RunningJobs"] = self.__runningJobs
        result["WaitingJobs"] = 0
        # processors
        result["AvailableProcessors"] = self.processors
        return result
