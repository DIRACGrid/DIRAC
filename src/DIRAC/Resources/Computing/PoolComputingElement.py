"""The Pool Computing Element is an "inner" CE (meaning it's used by a jobAgent inside a pilot)

It's used running several jobs simultaneously in separate processes, managed by a ProcessPool.

**Configuration Parameters**

LocalCEType:
   Configuration for the PoolCE submission can be done via the CE configuration such as::

     LocalCEType = Pool

   The Pool Computing Element is specific: it embeds an additional "inner" CE
   (`InProcess` by default, or `Singularity`). The "inner" CE can be specified such as::

     LocalCEType = Pool/Singularity

NumberOfProcessors:
   Maximum number of processors that can be used to compute jobs.

**Code Documentation**
"""

import concurrent.futures
import functools

from DIRAC import S_ERROR, S_OK
from DIRAC.ConfigurationSystem.private.ConfigurationData import ConfigurationData
from DIRAC.Resources.Computing.ComputingElement import ComputingElement
from DIRAC.Resources.Computing.InProcessComputingElement import InProcessComputingElement
from DIRAC.Resources.Computing.SingularityComputingElement import SingularityComputingElement


def executeJob(executableFile, proxy, taskID, inputs, innerCEParameters, **kwargs):
    """wrapper around ce.submitJob: decides which inner CE to use (InProcess or Singularity)

    :param str executableFile: location of the executable file
    :param str proxy: proxy file location to be used for job submission
    :param int taskID: local task ID of the PoolCE

    :return: the result of the job submission (S_OK/S_ERROR)
    """

    innerCESubmissionType = kwargs.pop("InnerCESubmissionType")

    if innerCESubmissionType == "Singularity":
        ce = SingularityComputingElement("Task-" + str(taskID))
    else:
        ce = InProcessComputingElement("Task-" + str(taskID))

    # adding the number of processors to use and the RAM
    ce.ceParameters["NumberOfProcessors"] = innerCEParameters["NumberOfProcessors"]
    ce.ceParameters["MaxRAM"] = innerCEParameters["MaxRAM"]
    return ce.submitJob(executableFile, proxy, inputs=inputs, **kwargs)


class PoolComputingElement(ComputingElement):
    #############################################################################
    def __init__(self, ceUniqueID):
        """Standard constructor."""
        super().__init__(ceUniqueID)

        self.submittedJobs = 0
        self.processors = 1
        self.pPool = None
        self.taskID = 0
        self.processorsPerTask = {}
        self.ramPerTask = {}
        self.ram = 0  # effectively this means "no limits"

        # This CE will effectively submit to another "Inner"CE
        # (by default to the InProcess CE)
        self.innerCESubmissionType = "InProcess"

    def _reset(self):
        """Update internal variables after some extra parameters are added

        :return: None
        """

        self.processors = int(self.ceParameters.get("NumberOfProcessors", self.processors))
        self.ceParameters["MaxTotalJobs"] = self.processors
        if self.ceParameters.get("MaxRAM", 0):  # if there's a limit, we set it
            self.ram = int(self.ceParameters["MaxRAM"])
        # Indicates that the submission is done asynchronously
        # The result is not immediately available
        self.ceParameters["AsyncSubmission"] = True
        self.innerCESubmissionType = self.ceParameters.get("InnerCESubmissionType", self.innerCESubmissionType)
        return S_OK()

    #############################################################################
    def submitJob(self, executableFile, proxy=None, inputs=None, **kwargs):
        """Method to submit job.
        This method will submit to a ProcessPoolExecutor, which returns Future objects.

        :param str executableFile: location of the executable file
        :param str proxy: payload proxy
        :param list inputs: dependencies of executableFile

        :return: S_OK always. The result of the submission should be included in taskResults
        """

        if self.pPool is None:
            self.pPool = concurrent.futures.ProcessPoolExecutor(max_workers=self.processors)

        processorsForJob = self._getProcessorsForJobs(kwargs)
        if not processorsForJob:
            self.taskResults[self.taskID] = S_ERROR("Not enough processors for the job")
            taskID = self.taskID
            self.taskID += 1
            return S_OK(taskID)

        memoryForJob = self._getMemoryForJobs(kwargs)

        if memoryForJob is None:
            self.taskResults[self.taskID] = S_ERROR("Not enough memory for the job")
            taskID = self.taskID
            self.taskID += 1
            return S_OK(taskID)

        # Now persisting the job limits for later use in pilot.cfg file
        cd = ConfigurationData(loadDefaultCFG=False)
        res = cd.loadFile("pilot.cfg")
        if not res["OK"]:
            self.log.error("Could not load pilot.cfg", res["Message"])
        else:
            jobID = int(kwargs.get("jobDesc", {}).get("jobID", 0))
            cd.setOptionInCFG("/Resources/Computing/JobLimits/%d/NumberOfProcessors" % jobID, processorsForJob)
            cd.setOptionInCFG("/Resources/Computing/JobLimits/%d/MaxRAM" % jobID, memoryForJob)
            res = cd.dumpLocalCFGToFile("pilot.cfg")
            if not res["OK"]:
                self.log.error("Could not dump cfg to pilot.cfg", res["Message"])

        # Define the innerCEParameters
        innerCEParameters = {}
        innerCEParameters["NumberOfProcessors"] = processorsForJob
        innerCEParameters["MaxRAM"] = memoryForJob

        # Here we define task kwargs: adding complex objects like thread.Lock can trigger errors in the task
        taskKwargs = {"InnerCESubmissionType": self.innerCESubmissionType}
        taskKwargs["jobDesc"] = kwargs.get("jobDesc", {})

        # Submission
        future = self.pPool.submit(
            executeJob, executableFile, proxy, self.taskID, inputs, innerCEParameters, **taskKwargs
        )
        self.processorsPerTask[future] = processorsForJob
        self.ramPerTask[future] = memoryForJob
        future.add_done_callback(functools.partial(self.finalizeJob, self.taskID))

        taskID = self.taskID
        self.taskID += 1

        return S_OK(taskID)  # returning S_OK as callback will do the rest

    def _getProcessorsForJobs(self, kwargs):
        """helper function"""
        processorsInUse = sum(self.processorsPerTask.values())
        availableProcessors = self.processors - processorsInUse

        self.log.verbose(
            "Processors (total, in use, available)",
            "(%d, %d, %d)" % (self.processors, processorsInUse, availableProcessors),
        )

        # Does this ask for MP?
        if not kwargs.get("mpTag", False):
            if availableProcessors:
                return 1
            else:
                return 0

        # From here we assume the job is asking for MP
        if kwargs.get("wholeNode", False):
            if processorsInUse > 0:
                return 0
            else:
                return self.processors

        if "numberOfProcessors" in kwargs:
            requestedProcessors = int(kwargs["numberOfProcessors"])
        else:
            requestedProcessors = 1

        if availableProcessors < requestedProcessors:
            return 0

        # If there's a maximum number of processors allowed for the job, use that as maximum,
        # otherwise it will use all the remaining processors
        if "maxNumberOfProcessors" in kwargs and kwargs["maxNumberOfProcessors"]:
            requestedProcessors = min(int(kwargs["maxNumberOfProcessors"]), availableProcessors)

        return requestedProcessors

    def _getMemoryForJobs(self, kwargs):
        """helper function to get the memory that will be allocated for the job

        :param kwargs: job parameters
        :return: memory in MB or None if not enough memory
        """

        # # job requirements
        requestedMemory = kwargs.get("MinRAM", kwargs.get("MaxRAM", 0))
        # if there's no limit, we just let it match the maximum
        if not self.ram:
            return max(requestedMemory, kwargs.get("MaxRAM", 0))

        # # now check what the slot can provide
        # Do we have enough memory?
        availableMemory = self.ram - sum(self.ramPerTask.values())
        if availableMemory < requestedMemory:
            return None

        # if there's a MaxRAM requested, we allocate it all (if it fits),
        # and if it doesn't, we stop here instead of using MinRAM
        if kwargs.get("MaxRAM", 0):
            if availableMemory >= kwargs.get("MaxRAM"):
                requestedMemory = kwargs.get("MaxRAM")
            else:
                return None

        return requestedMemory

    def finalizeJob(self, taskID, future):
        """Finalize the job by updating the process utilisation counters

        :param future: evaluating the future result
        """
        nProc = self.processorsPerTask.pop(future)
        ram = self.ramPerTask.pop(future, None)

        result = future.result()  # This would be the result of the e.g. InProcess.submitJob()
        if result["OK"]:
            self.log.info("Task finished successfully:", f"{taskID}; {nProc} processor(s) and {ram}MB freed")
        else:
            self.log.error("Task failed submission:", f"{taskID}; message: {result['Message']}")
        self.taskResults[taskID] = result

    def getCEStatus(self):
        """Method to return information on running and waiting jobs,
        as well as the number of processors (used, and available),
        and the RAM (used, and available)

        :return: dictionary of numbers of jobs per status and processors (used, and available)
        """

        result = S_OK()
        nJobs = 0

        for _j, value in self.processorsPerTask.items():
            if value > 0:
                nJobs += 1
        result["SubmittedJobs"] = nJobs
        result["RunningJobs"] = nJobs
        result["WaitingJobs"] = 0

        # dealing with processors
        processorsInUse = sum(self.processorsPerTask.values())
        result["UsedProcessors"] = processorsInUse
        result["AvailableProcessors"] = self.processors - processorsInUse
        # dealing with RAM
        result["UsedRAM"] = sum(self.ramPerTask.values())
        if self.ram:
            result["AvailableRAM"] = self.ram - sum(self.ramPerTask.values())
        else:
            result["AvailableRAM"] = 0

        return result

    def getDescription(self):
        """Get a list of CEs descriptions (each is a dict)

        This is called by the JobAgent.
        """
        result = super().getDescription()
        if not result["OK"]:
            return result
        ceDict = result["Value"]

        ceDictList = []
        if self.ceParameters.get("MultiProcessorStrategy"):
            strategyRequiredTags = []
            if not ceDict.get("ProcessorsInUse", 0):
                # We are starting from a clean page, try to get the most demanding
                # jobs first
                strategyRequiredTags.append(["WholeNode"])
            processors = ceDict.get("NumberOfProcessors", 0)
            if processors > 1:
                # We have several processors at hand, try to use most of them
                strategyRequiredTags.append(["%dProcessors" % processors])
                # Well, at least jobs with some processors requirement
                strategyRequiredTags.append(["MultiProcessor"])

            for strat in strategyRequiredTags:
                newCEDict = dict(ceDict)
                newCEDict.setdefault("RequiredTag", []).extend(strat)
                ceDictList.append(newCEDict)

        # Do not require anything special if nothing else was lucky
        ceDictList.append(dict(ceDict))

        return S_OK(ceDictList)

    def shutdown(self):
        """Wait for all futures (jobs) to complete"""
        if self.pPool:
            self.pPool.shutdown()  # blocking
        return S_OK(self.taskResults)
