from abc import ABC, abstractmethod

from DIRAC.WorkloadManagementSystem.Client.MatcherClient import MatcherClient


class SubmissionPolicy(ABC):
    """Abstract class to define a submission strategy."""

    @abstractmethod
    def apply(self, availableSlots: int, queueName: str, queueInfo: dict[str, str], vo: str) -> int:
        """Method to redefine in the concrete subclasses

        :param availableSlots: slots available for new pilots
        :param queueName: the name of the targeted queue
        :param queueInfo: a dictionary of attributes related to the queue
        :param vo: VO
        """
        pass


class AgressiveFillingPolicy(SubmissionPolicy):
    def apply(self, availableSlots: int, queueName: str, queueInfo: dict[str, str], vo: str) -> int:
        """All the available slots should be filled up.
        Should be employed for sites that are always processing jobs.

        * Pros: would quickly fill up a queue
        * Cons: would consume a lot of CPU hours for nothing if pilots do not match jobs
        """
        return availableSlots


class WaitingSupportedJobsPolicy(SubmissionPolicy):
    def __init__(self) -> None:
        super().__init__()
        self.matcherClient = MatcherClient()

    def apply(self, availableSlots: int, queueName: str, queueInfo: dict[str, str], vo: str) -> int:
        """Fill up available slots only if waiting supported jobs exist.
        Should be employed for sites that are used from time to time (targeting specific Task Queues).

        * Pros: submit pilots only if necessary, and quickly fill up the queue if needed
        * Cons: would create some unused pilots in all the sites supervised by this policy and targeting a same task queue
        """
        # Prepare CE dictionary from the queue info
        ce = queueInfo["CE"]
        ceDict = ce.ceParameters
        ceDict["GridCE"] = queueInfo["CEName"]
        if vo:
            ceDict["Community"] = vo

        # Get Task Queues related to the CE
        result = self.matcherClient.getMatchingTaskQueues(ceDict)
        if not result["OK"]:
            return 0
        taskQueueDict = result["Value"]

        # Get the number of jobs that would match the capability of the CE
        waitingSupportedJobs = 0
        for tq in taskQueueDict.values():
            waitingSupportedJobs += tq["Jobs"]

        # Return the minimum value between the number of slots available and supported jobs
        return min(availableSlots, waitingSupportedJobs)
