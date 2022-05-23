"""
This class is used as an interface to check the jobs by the
CheckerAdministrator class. All checkers should inherit this class.
"""

from abc import ABC, abstractmethod

from DIRAC.WorkloadManagementSystem.Client.JobState.JobState import JobState


class Checker(ABC):
    """Abstract checker class"""

    @abstractmethod
    def __init__(self, jobState: JobState) -> None:
        self.jobState = jobState

    @abstractmethod
    def check(self):
        """
        Method that needs to be overriden to check the job

        :returns: S_OK() / S_ERROR("Message")
        """
