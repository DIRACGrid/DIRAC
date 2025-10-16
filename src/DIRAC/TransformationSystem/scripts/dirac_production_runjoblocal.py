#!/usr/bin/env python
"""
Module created to run failed jobs locally on a CVMFS-configured machine.
It creates the necessary environment, downloads the necessary files, modifies the necessary
files and runs the job
"""
from importlib import import_module
import os
import shutil
import ssl

from xml.etree import ElementTree as et

from urllib.request import urlopen

from DIRAC.Interfaces.API.Dirac import Dirac
from DIRAC.Core.Utilities.File import mkDir
from DIRAC.Core.Base.Script import Script
from DIRAC.ConfigurationSystem.Client.Helpers.CSGlobals import getVO
from DIRAC.ConfigurationSystem.Client.ConfigurationData import gConfigurationData


def __runSystemDefaults(jobID, vo):
    """
    Creates the environment for running the job and returns
    the path for the other functions.
    """
    tempdir = str(vo) + "job" + str(jobID) + "temp"
    mkDir(tempdir)
    basepath = os.getcwd()
    return basepath + os.path.sep + tempdir + os.path.sep


def __downloadJobDescriptionXML(jobID, basepath):
    """
    Downloads the jobDescription.xml file into the temporary directory
    created.
    """
    jdXML = Dirac()
    jdXML.getInputSandbox(jobID, basepath)


def __modifyJobDescription(jobID, basepath, downloadinputdata):
    """
    Modifies the jobDescription.xml to, instead of DownloadInputData, it
    uses InputDataByProtocol
    """
    if not downloadinputdata:
        archive = et.parse(basepath + "InputSandbox" + str(jobID) + os.path.sep + "jobDescription.xml")
        for element in archive.iter():
            if element.text == "DIRAC.WorkloadManagementSystem.Client.DownloadInputData":
                element.text = "DIRAC.WorkloadManagementSystem.Client.InputDataByProtocol"
                archive.write(basepath + "InputSandbox" + str(jobID) + os.path.sep + "jobDescription.xml")


def __downloadPilotScripts():
    """
    Downloads the scripts necessary to configure the pilot
    """

    context = ssl._create_unverified_context()
    for fileName in [
        "dirac-pilot.py",
        "pilotCommands.py",
        "pilotTools",
    ]:
        remoteFile = urlopen(
            os.path.join("https://raw.githubusercontent.com/DIRACGrid/Pilot/master/Pilot/", fileName),
            timeout=10,
            context=context,
        )
        with open(fileName, "wb") as localFile:
            localFile.write(remoteFile.read())


def __configurePilot(basepath, vo):
    """
    Configures the pilot.
    This method was created specifically for LHCb pilots, more info
    about othe VOs is needed to make it more general.
    """
    masterCS = gConfigurationData.getMasterServer()

    os.system(
        "python "
        + basepath
        + f"dirac-pilot.py -l {vo} -C {masterCS} -N ce.debug.ch -Q default -n DIRAC.JobDebugger.ch -dd"
    )

    diracdir = os.path.expanduser("~") + os.path.sep
    try:
        os.rename(diracdir + ".dirac.cfg", diracdir + ".dirac.cfg.old")
    except (OSError, FileNotFoundError):
        pass
    shutil.copyfile(diracdir + "pilot.cfg", diracdir + ".dirac.cfg")


def __runJobLocally(jobID, basepath, vo):
    """
    Runs the job!
    """
    ipr = import_module(str(vo) + "DIRAC.Interfaces.API." + str(vo) + "Job")
    voJob = getattr(ipr, str(vo) + "Job")
    localJob = voJob(basepath + "InputSandbox" + str(jobID) + os.path.sep + "jobDescription.xml")
    localJob.setInputSandbox(os.getcwd() + os.path.sep + "pilot.cfg")
    localJob.setConfigArgs(os.getcwd() + os.path.sep + "pilot.cfg")
    os.chdir(basepath)
    localJob.runLocal()


@Script()
def main():
    Script.registerSwitch("D:", "Download=", "Set jobID here. Defines data acquisition as DownloadInputData")
    Script.registerSwitch("I:", "Protocol=", "Set jobID here. Defines data acquisition as InputDataByProtocol")
    switches, args = Script.parseCommandLine(ignoreErrors=False)

    _downloadinputdata = False
    _jobID = None

    for switch in switches:
        if switch[0] in ("D", "Download"):
            _downloadinputdata = True
            _jobID = switch[1]
        if switch[0] in ("I", "Protocol"):
            _downloadinputdata = False
            _jobID = switch[1]

    _vo = getVO()
    _dir = os.path.expanduser("~") + os.path.sep
    try:
        _path = __runSystemDefaults(_jobID, _vo)

        __downloadJobDescriptionXML(_jobID, _path)

        __modifyJobDescription(_jobID, _path, _downloadinputdata)

        __downloadPilotScripts()

        __configurePilot(_path, _vo)

        __runJobLocally(_jobID, _path, _vo)
    finally:
        os.chdir(_dir)
        try:
            os.rename(_dir + ".dirac.cfg.old", _dir + ".dirac.cfg")
        except FileNotFoundError:
            pass


if __name__ == "__main__":
    main()
