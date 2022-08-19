########################################################################
# File :   InProcessComputingElement.py
# Author : Stuart Paterson
########################################################################

""" The simplest of the "inner" CEs (meaning it's used by a jobAgent inside a pilot)

    A "InProcess" CE instance submits jobs in the current process.
    This is the standard "inner CE" invoked from the JobAgent, main alternative being the PoolCE
"""
import os
import stat

from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Utilities.Subprocess import systemCall
from DIRAC.Core.Utilities.ThreadScheduler import gThreadScheduler

from DIRAC.Resources.Computing.ComputingElement import ComputingElement


class InProcessComputingElement(ComputingElement):

    #############################################################################
    def __init__(self, ceUniqueID):
        """Standard constructor."""
        super().__init__(ceUniqueID)

        self.submittedJobs = 0
        self.runningJobs = 0

        self.processors = int(self.ceParameters.get("NumberOfProcessors", 1))
        self.ceParameters["MaxTotalJobs"] = 1

    #############################################################################
    def submitJob(self, executableFile, proxy=None, inputs=None, **kwargs):
        """Method to submit job (overriding base method).

        :param str executableFile: file to execute via systemCall.
                                   Normally the JobWrapperTemplate when invoked by the JobAgent.
        :param str proxy: the proxy used for running the job (the payload). It will be dumped to a file.
        :param list inputs: dependencies of executableFile
        """
        payloadEnv = dict(os.environ)
        payloadProxy = ""
        if proxy:
            self.log.verbose("Setting up proxy for payload")
            result = self.writeProxyToFile(proxy)
            if not result["OK"]:
                return result

            payloadProxy = result["Value"]  # payload proxy file location
            payloadEnv["X509_USER_PROXY"] = payloadProxy

            self.log.verbose("Starting process for monitoring payload proxy")
            result = gThreadScheduler.addPeriodicTask(
                self.proxyCheckPeriod, self._monitorProxy, taskArgs=(payloadProxy,), executions=0, elapsedTime=0
            )
            if result["OK"]:
                renewTask = result["Value"]
            else:
                self.log.warn("Failed to start proxy renewal task")
                renewTask = None

        self.submittedJobs += 1
        self.runningJobs += 1

        if not os.access(executableFile, 5):
            os.chmod(executableFile, stat.S_IRWXU | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH)
        cmd = os.path.abspath(executableFile)
        self.log.verbose("CE submission command:", cmd)
        result = systemCall(0, cmd, callbackFunction=self.sendOutput, env=payloadEnv)
        if payloadProxy:
            os.unlink(payloadProxy)

        if proxy and renewTask:
            gThreadScheduler.removeTask(renewTask)

        self.runningJobs -= 1

        # Delete executable file and inputs in case space is limited
        os.unlink(executableFile)
        if inputs:
            if not isinstance(inputs, list):
                inputs = [inputs]
            for inputFile in inputs:
                os.unlink(inputFile)

        ret = S_OK()

        if not result["OK"]:
            self.log.error("Fail to run InProcess", result["Message"])
        elif result["Value"][0] > 128:
            res = S_ERROR()
            # negative exit values are returned as 256 - exit
            res["Value"] = result["Value"][0] - 256  # yes, it's "correct"
            self.log.warn("InProcess Job Execution Failed")
            self.log.info("Exit status:", result["Value"])
            if res["Value"] == -2:
                error = "JobWrapper initialization error"
            elif res["Value"] == -1:
                error = "JobWrapper execution error"
            else:
                error = "InProcess Job Execution Failed"
            res["Message"] = error
            return res
        elif result["Value"][0] > 0:
            self.log.warn("Fail in payload execution")
            self.log.info("Exit status:", result["Value"][0])
            ret["PayloadFailed"] = result["Value"][0]
        else:
            self.log.debug("InProcess CE result OK")

        return ret

    #############################################################################
    def getCEStatus(self):
        """Method to return information on running and waiting jobs,
        as well as number of available processors
        """
        result = S_OK()

        result["SubmittedJobs"] = self.submittedJobs
        result["RunningJobs"] = self.runningJobs
        result["WaitingJobs"] = 0
        # processors
        result["AvailableProcessors"] = self.processors
        return result
