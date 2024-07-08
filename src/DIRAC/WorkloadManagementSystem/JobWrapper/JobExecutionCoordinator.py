from DIRAC import S_OK


class JobExecutionCoordinator:
    """
    Abstract class for job execution coordinators.

    This class is responsible for pre-processing and post-processing jobs before and after execution.
    It should be implemented by the community job execution coordinator.
    It is used by the JobWrapper to handle the execution of jobs.
    """

    def __init__(self, jobArgs: dict, ceArgs: dict) -> None:
        """
        Initialize the job execution coordinator.

        :param jobArgs: The job arguments
        :param ceArgs: The environment arguments
        """
        self.jobArgs = jobArgs
        self.ceArgs = ceArgs

    def preProcess(self, command: str, exeEnv: dict):
        """
        Pre-process a job before executing it.
        This should handle tasks like downloading inputs, preparing commands, etc.

        :param job: The job to be pre-processed
        """
        return S_OK({"command": command, "env": exeEnv})

    def postProcess(self):
        """
        Post-process a job after executing it.
        This should handle tasks like uploading outputs, checking results, etc.

        :param job: The job to be post-processed
        """
        return S_OK()
