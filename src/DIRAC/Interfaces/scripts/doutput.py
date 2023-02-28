#!/usr/bin/env python
"""
  Retrieve output sandbox for a DIRAC job
"""
import DIRAC
from DIRAC import S_OK, gLogger
from DIRAC.Interfaces.Utilities.DConfigCache import ConfigCache
from DIRAC.Core.Base.Script import Script

import os
import datetime


class Params:
    def __init__(self):
        self.outputDir = None
        self.outputData = False
        self.outputSandbox = False
        self.verbose = False
        self.noJobDir = False
        self.jobGroup = []
        self.inputFile = None

    def setOutputDir(self, arg=None):
        self.outputDir = arg
        return S_OK()

    def getOutputDir(self):
        return self.outputDir

    def setOutputData(self, arg=None):
        self.outputData = True
        return S_OK()

    def getOutputData(self):
        return self.outputData

    def setOutputSandbox(self, arg=None):
        self.outputSandbox = True
        return S_OK()

    def getOutputSandbox(self):
        return self.outputSandbox

    def setVerbose(self, arg=None):
        self.verbose = True
        return S_OK()

    def getVerbose(self):
        return self.verbose

    def setNoJobDir(self, arg=None):
        self.noJobDir = True
        return S_OK()

    def getNoJobDir(self):
        return self.noJobDir

    def setJobGroup(self, arg=None):
        if arg:
            self.jobGroup.append(arg)
        return S_OK()

    def getJobGroup(self):
        return self.jobGroup

    def setInputFile(self, arg=None):
        self.inputFile = arg
        return S_OK()

    def getInputFile(self):
        return self.inputFile


@Script()
def main():
    params = Params()

    Script.registerArgument(["JobID: DIRAC Job ID"], mandatory=False)
    Script.registerSwitch("D:", "OutputDir=", "destination directory", params.setOutputDir)
    Script.registerSwitch(
        "",
        "Data",
        "download output data instead of output sandbox",
        params.setOutputData,
    )
    Script.registerSwitch(
        "",
        "Sandbox",
        "download output sandbox, even if data was required",
        params.setOutputSandbox,
    )
    Script.registerSwitch("v", "verbose", "verbose output", params.setVerbose)
    Script.registerSwitch("n", "NoJobDir", "do not create job directory", params.setNoJobDir)
    Script.registerSwitch("g:", "JobGroup=", "Get output for jobs in the given group", params.setJobGroup)
    Script.registerSwitch("i:", "input-file=", "read JobIDs from file", params.setInputFile)

    configCache = ConfigCache()
    Script.parseCommandLine(ignoreErrors=True)
    configCache.cacheConfig()
    args = Script.getPositionalArgs()

    from DIRAC.Interfaces.API.Dirac import Dirac
    from DIRAC.Core.Utilities.TimeUtilities import toString, day

    dirac = Dirac()
    exitCode = 0

    if args:
        # handle comma separated list of JobIDs
        newargs = []
        for arg in args:
            newargs += arg.split(",")
        args = newargs

    if params.getInputFile() != None:
        with open(params.getInputFile()) as f:
            for l in f.readlines():
                args += l.split(",")

    for jobGroup in params.getJobGroup():
        jobDate = toString(datetime.datetime.utcnow().date() - 30 * day)

        # Choose jobs in final state, no more than 30 days old
        for s in ["Done", "Failed"]:
            result = dirac.selectJobs(jobGroup=jobGroup, date=jobDate, status=s)
            if not result["OK"]:
                if not "No jobs selected" in result["Message"]:
                    gLogger.error(result["Message"])
                    exitCode = 2
            else:
                args += result["Value"]

    jobs = []

    outputDir = params.getOutputDir() or os.path.curdir

    for arg in args:
        if os.path.isdir(os.path.join(outputDir, arg)) and not params.getNoJobDir():
            gLogger.notice(f"Output for job {arg} already retrieved, remove the output directory to redownload")
        else:
            jobs.append(arg)

    if jobs:
        if not os.path.isdir(outputDir):
            os.makedirs(outputDir)

        errors = []
        inputs = {}
        for job in jobs:
            if not params.getNoJobDir():
                destinationDir = os.path.join(outputDir, job)
            else:
                destinationDir = outputDir
            inputs[job] = {"destinationDir": destinationDir}

            if params.getOutputSandbox() or not params.getOutputData():
                try:
                    result = dirac.getOutputSandbox(job, outputDir=outputDir, noJobDir=params.getNoJobDir())
                except TypeError:
                    errors.append(
                        'dirac.getOutputSandbox doesn\'t accept "noJobDir" argument. Will create per-job directories.'
                    )
                    result = dirac.getOutputSandbox(job, outputDir=outputDir)
                if result["OK"]:
                    inputs[job]["osb"] = destinationDir
                else:
                    errors.append(result["Message"])
                    exitCode = 2

            if params.getOutputData():
                if not os.path.isdir(destinationDir):
                    os.makedirs(destinationDir)
                result = dirac.getJobOutputData(job, destinationDir=destinationDir)
                if result["OK"]:
                    inputs[job]["data"] = result["Value"]
                else:
                    errors.append(result["Message"])
                    exitCode = 2

        for error in errors:
            gLogger.error(error)

        if params.getVerbose():
            for j, d in inputs.items():
                if "osb" in d:
                    gLogger.notice(f"{j}: OutputSandbox", d["osb"])
                if "data" in d:
                    gLogger.notice(f"{j}: OutputData", d["data"])

    DIRAC.exit(exitCode)


if __name__ == "__main__":
    main()
