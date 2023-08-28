#!/usr/bin/env python
"""
Submit jobs to DIRAC WMS

Default JDL can be configured from session in the "JDL" option
"""
import os.path
import sys
import re
import tempfile

from DIRAC import S_OK, gLogger
from DIRAC import exit as DIRACexit
from DIRAC.Interfaces.Utilities.DCommands import pathFromArgument
from DIRAC.Interfaces.Utilities.DConfigCache import ConfigCache
from DIRAC.Interfaces.Utilities.DCommands import DSession
from DIRAC.Interfaces.API.Dirac import Dirac
from DIRAC.Core.Base.Script import Script
from DIRAC.Core.Utilities.ClassAd.ClassAdLight import ClassAd

classAdJob = None


def parseScriptLinesJDLDirectives(lines):
    result = {}

    for l in lines:
        if l.startswith("#JDL "):
            c = l[5:]
            d, v = c.split("=", 1)
            result[d.strip()] = v.strip()
    return result


def parseScriptJDLDirectives(fn):
    with open(fn) as f:
        try:
            lines = f.readlines()
        except UnicodeDecodeError:
            # This is a binary executable, no JDL instructions there
            return {}

    return parseScriptLinesJDLDirectives(lines)


def classAdAppendToInputSandbox(classAd, f):
    classAdAppendToSandbox(classAd, f, "InputSandbox")


def classAdAppendToOutputSandbox(classAd, f):
    classAdAppendToSandbox(classAd, f, "OutputSandbox")


def classAdAppendToSandbox(classAd, f, sbName):
    global classAdJob

    sb = []
    if classAd.isAttributeList(sbName):
        sb = classAd.getListFromExpression(sbName)
    sb.append(f)
    classAdJob.insertAttributeVectorString(sbName, sb)


class Params:
    def __init__(self):
        self.__session = None
        self.attribs = {}
        self.jdl = None
        self.parametric = None
        self.forceExecUpload = False
        self.verbose = False
        self.inputData = ""

    def setSession(self, session):
        self.__session = session
        if self.inputData:
            self.attribs["InputData"] = self.pathListArg(self.inputData)
        return S_OK()

    def listArg(self, arg):
        if arg and not arg.startswith("{"):
            arg = "{" + arg + "}"
        return arg

    def pathListArg(self, arg):
        if not arg:
            return arg

        arg = arg.strip("{}")
        args = arg.split(",")

        pathlist = []
        for path in args:
            pathlist.append(pathFromArgument(self.__session, path))
        return "{" + ",".join(pathlist) + "}"

    def getDefaultJDL(self):
        if not self.__session:
            JDL = ""
        else:
            JDL = self.__session.getJDL()

        if JDL == "":
            # overall default JDL
            JDL = '[OutputSandbox = {"std.out","std.err"};]'

        return JDL

    def setJDL(self, arg=None):
        if os.path.isfile(arg):
            f = open(arg)
            arg = f.read()
            f.close()

        self.jdl = arg
        return S_OK()

    def getJDL(self):
        if self.jdl:
            return self.jdl
        else:
            return self.getDefaultJDL()

    def setVerbose(self, arg=None):
        self.verbose = True
        return S_OK()

    def getVerbose(self):
        return self.verbose

    def setForceExecUpload(self, arg=None):
        self.forceExecUpload = True
        return S_OK()

    def getForceExecUpload(self, arg=None):
        return self.forceExecUpload

    def setName(self, arg=None):
        self.attribs["JobName"] = arg
        return S_OK()

    def getName(self):
        return self.attribs["JobName"]

    def setStdError(self, arg=None):
        self.attribs["StdError"] = arg
        return S_OK()

    def getStdError(self):
        return self.attribs["StdError"]

    def setStdOutput(self, arg=None):
        self.attribs["StdOutput"] = arg
        return S_OK()

    def getStdOutput(self):
        return self.attribs["StdOutput"]

    def setOutputSandbox(self, arg=None):
        self.attribs["OutputSandbox"] = self.listArg(arg)
        return S_OK()

    def getOutputSandbox(self):
        return self.attribs["OutputSandbox"]

    def setInputSandbox(self, arg=None):
        self.attribs["InputSandbox"] = self.listArg(arg)
        return S_OK()

    def getInputSandbox(self):
        return self.attribs["InputSandbox"]

    def setInputData(self, arg=None):
        self.inputData = arg
        return S_OK()

    def getInputData(self):
        return self.attribs.get("InputData", [])

    def setOutputData(self, arg=None):
        self.attribs["OutputData"] = self.listArg(arg)
        return S_OK()

    def getOutputData(self):
        return self.attribs["OutputData"]

    def setOutputPath(self, arg=None):
        self.attribs["OutputPath"] = arg
        return S_OK()

    def getOutputPath(self):
        return self.attribs["OutputPath"]

    def setOutputSE(self, arg=None):
        self.attribs["OutputSE"] = arg
        return S_OK()

    def getOutputSE(self):
        return self.attribs["OutputSE"]

    def setCPUTime(self, arg=None):
        self.attribs["CPUTime"] = arg
        return S_OK()

    def getCPUTime(self):
        return self.attribs["CPUTime"]

    def setSite(self, arg=None):
        self.attribs["Site"] = self.listArg(arg)
        return S_OK()

    def getSite(self):
        return self.attribs["Site"]

    def setBannedSite(self, arg=None):
        self.attribs["BannedSite"] = self.listArg(arg)
        return S_OK()

    def getBannedSite(self):
        return self.attribs["BannedSite"]

    def setPlatform(self, arg=None):
        self.attribs["Platform"] = self.listArg(arg)
        return S_OK()

    def getPlatform(self):
        return self.attribs["Platform"]

    def setPriority(self, arg=None):
        self.attribs["Priority"] = arg
        return S_OK()

    def getPriority(self):
        return self.attribs["Priority"]

    def setJobGroup(self, arg=None):
        self.attribs["JobGroup"] = arg
        return S_OK()

    def getJobGroup(self):
        return self.attribs["JobGroup"]

    def setParametric(self, arg=None):
        self.parametric = arg.split(",")
        return S_OK()

    def getParametric(self):
        return self.parametric

    def modifyClassAd(self, classAd):
        classAd.contents.update(self.attribs)

    def parameterizeClassAd(self, classAd):
        def classAdClone(classAd):
            return ClassAd(classAd.asJDL())

        if not (self.parametric or classAd.lookupAttribute("Parametric")):
            return [classAd]

        parametric = self.parametric
        if not parametric:
            parametric = classAd.getAttributeString("Parametric").split(",")

        float_pat = r"[-+]?(((\d*\.)?\d+)|(\d+\.))([eE][-+]\d+)?"
        loop_re = re.compile(
            "^(?P<start>{fp}):(?P<stop>{fp})(:(?P<step>{fp})(:(?P<factor>{fp}))?)?$".format(fp=float_pat)
        )
        parameters = []
        loops = []
        for param in parametric:
            m = loop_re.match(param)
            if m:
                loop = m.groupdict()
                try:
                    start = int(loop["start"])
                    stop = int(loop["stop"])
                except ValueError:
                    start = float(loop["start"])
                    stop = float(loop["stop"])
                step = 1
                factor = 1
                if "step" in loop and loop["step"]:
                    try:
                        step = int(loop["step"])
                    except ValueError:
                        step = float(loop["step"])
                    if "factor" in loop and loop["factor"]:
                        try:
                            factor = int(loop["factor"])
                        except ValueError:
                            factor = float(loop["factor"])
                loops.append((start, stop, step, factor))
            else:
                parameters.append(param)

        ret = []
        if parameters:
            new = classAdClone(classAd)
            new.insertAttributeVectorString("Parameters", parameters)
            ret.append(new)

        def pnumber(start, stop, step, factor):
            sign = 1.0 if start < stop else -1.0
            i = start
            n = 0
            while (i - stop) * sign < 0:
                n += 1
                ip1 = i * factor + step

                # check that parameter evolves in the good direction
                if (ip1 - i) * sign < 0:
                    raise Exception(f"Error in parametric specification: {start}:{stop}:{step}:{factor}")

                i = ip1

            return n

        for start, stop, step, factor in loops:
            new = classAdClone(classAd)
            number = pnumber(start, stop, step, factor)
            new.insertAttributeString("ParameterStart", str(start))
            new.insertAttributeInt("Parameters", number)
            new.insertAttributeString("ParameterStep", str(step))
            new.insertAttributeString("ParameterFactor", str(factor))
            ret.append(new)

        return ret


@Script()
def main():
    global classAdJob

    params = Params()

    Script.registerArgument(
        "executable: command to be run inside the job.\n"
        "            If a relative path, local file will be included in InputSandbox\n"
        "            If no executable is given and JDL (provided or default) doesn't contain one,\n"
        "            standard input will be read for executable contents",
        mandatory=False,
    )
    Script.registerArgument(
        [
            "arguments: arguments to pass to executable\n"
            "           if some arguments are to begin with a dash '-', prepend '--' before them"
        ],
        mandatory=False,
    )
    Script.registerSwitch("J:", "JDL=", "JDL file or inline", params.setJDL)
    Script.registerSwitch("N:", "JobName=", "job name", params.setName)
    Script.registerSwitch("E:", "StdError=", "job standard error file", params.setStdError)
    Script.registerSwitch("O:", "StdOutput=", "job standard output file", params.setStdOutput)
    Script.registerSwitch("", "OutputSandbox=", "job output sandbox", params.setOutputSandbox)
    Script.registerSwitch("", "InputSandbox=", "job input sandbox", params.setInputSandbox)
    Script.registerSwitch("", "OutputData=", "job output data", params.setOutputData)
    Script.registerSwitch("", "InputData=", "job input data", params.setInputData)
    Script.registerSwitch("", "OutputPath=", "job output data path prefix", params.setOutputPath)
    Script.registerSwitch("", "OutputSE=", "job output data SE", params.setOutputSE)
    Script.registerSwitch("", "CPUTime=", "job CPU time limit (in seconds)", params.setCPUTime)
    Script.registerSwitch("", "Site=", "job Site list", params.setSite)
    Script.registerSwitch("", "BannedSite=", "job Site exclusion list", params.setBannedSite)
    Script.registerSwitch("", "Platform=", "job Platform list", params.setPlatform)
    Script.registerSwitch("", "Priority=", "job priority", params.setPriority)
    Script.registerSwitch("", "JobGroup=", "job JobGroup", params.setJobGroup)
    Script.registerSwitch(
        "",
        "Parametric=",
        "comma separated list or named parameters or number loops (in the form<start>:<stop>[:<step>[:<factor>]])",
        params.setParametric,
    )
    Script.registerSwitch(
        "",
        "ForceExecUpload",
        "Force upload of executable with InputSandbox",
        params.setForceExecUpload,
    )

    Script.registerSwitch("v", "verbose", "verbose output", params.setVerbose)

    configCache = ConfigCache()
    Script.parseCommandLine(ignoreErrors=True)
    configCache.cacheConfig()

    args = Script.getPositionalArgs()

    cmd = None
    cmdArgs = []
    if len(args) >= 1:
        cmd = args[0]
        cmdArgs = args[1:]

    session = DSession()
    params.setSession(session)

    dirac = Dirac()
    exitCode = 0
    errorList = []

    classAdJob = ClassAd(params.getJDL())

    params.modifyClassAd(classAdJob)

    # retrieve JDL provided Executable if present and user did not provide one
    jdlExecutable = classAdJob.getAttributeString("Executable")
    if jdlExecutable and not cmd:
        cmd = jdlExecutable

    tempFiles = []
    if cmd is None:
        # get executable script from stdin
        if sys.stdin.isatty():
            gLogger.notice("\nThe executable is not given")
            gLogger.notice("Type in the executable script lines, finish with ^D")
            gLogger.notice("or exit job submission with ^C\n")

        lines = sys.stdin.readlines()

        # Manage JDL directives inserted in cmd
        jdlDirectives = parseScriptLinesJDLDirectives(lines)
        classAdJob.contents.update(jdlDirectives)
        # re-apply parameters options to take priority over script JDL directives
        params.modifyClassAd(classAdJob)

        f = tempfile.NamedTemporaryFile(delete=False)
        fn = f.name
        for l in lines:
            f.write(l)
        f.close()
        tempFiles.append(fn)

        classAdJob.insertAttributeString("Executable", os.path.basename(fn))

        classAdAppendToInputSandbox(classAdJob, fn)

        if not classAdJob.lookupAttribute("JobName"):
            classAdJob.insertAttributeString("JobName", "STDIN")

    else:
        # Manage JDL directives inserted in cmd
        jdlDirectives = parseScriptJDLDirectives(cmd)
        classAdJob.contents.update(jdlDirectives)
        # re-apply parameters options to take priority over script JDL directives
        params.modifyClassAd(classAdJob)

        # Executable name provided
        if params.getForceExecUpload() and cmd.startswith("/"):
            # job will use uploaded executable (relative path)
            classAdJob.insertAttributeString("Executable", os.path.basename(cmd))
        else:
            classAdJob.insertAttributeString("Executable", cmd)

        uploadExec = params.getForceExecUpload() or not cmd.startswith("/")
        if uploadExec:
            if not os.path.isfile(cmd):
                gLogger.error(f'Executable file "{cmd}" not found')
                DIRACexit(2)

            classAdAppendToInputSandbox(classAdJob, cmd)

            # set job name based on script file name
            if not classAdJob.lookupAttribute("JobName"):
                classAdJob.insertAttributeString("JobName", cmd)

        if cmdArgs:
            classAdJob.insertAttributeString("Arguments", " ".join(cmdArgs))

    classAdJobs = params.parameterizeClassAd(classAdJob)

    if params.getVerbose():
        gLogger.notice("JDL:")
        for p in params.parameterizeClassAd(classAdJob):
            gLogger.notice(p.asJDL())

    jobIDs = []

    for classAdJob in classAdJobs:
        jdlString = classAdJob.asJDL()
        result = dirac.submitJob(jdlString)
        if result["OK"]:
            if isinstance(result["Value"], int):
                jobIDs.append(result["Value"])
            else:
                jobIDs += result["Value"]
        else:
            errorList.append((jdlString, result["Message"]))
            exitCode = 2

    if jobIDs:
        if params.getVerbose():
            gLogger.notice(
                "JobID:",
            )
        gLogger.notice(",".join(map(str, jobIDs)))

    # remove temporary generated files, if any
    for f in tempFiles:
        try:
            os.unlink(f)
        except Exception as err:
            errorList.append(err)

    for error in errorList:
        gLogger.error(f"{error}")

    DIRACexit(exitCode)


if __name__ == "__main__":
    main()
