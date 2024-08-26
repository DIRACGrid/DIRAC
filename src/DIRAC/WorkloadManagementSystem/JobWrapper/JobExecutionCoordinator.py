from DIRAC import S_OK


class JobExecutionCoordinator:
    """
    Job Execution Coordinator Class.

    This class serves as the base class for job execution coordinators, providing
    the necessary methods for pre-processing and post-processing jobs before and after
    their execution.

    Communities who need to implement specific workflows for job pre-processing
    and post-processing in their Dirac extension should inherit from this class and
    override the relevant methods with their custom implementations.

    The `JobExecutionCoordinator` class is primarily used by the `JobWrapper` to manage
    the execution of jobs, ensuring that all necessary preparations are made before the
    job starts, and that any required cleanup or data handling is performed after the
    job completes.

    **Example Usage in your Extension:**

    .. code-block:: python

        from DIRAC.WorkloadManagementSystem.JobWrapper.JobExecutionCoordinator import (
            JobExecutionCoordinator as DIRACJobExecutionCoordinator
        )

        class JobExecutionCoordinator(DIRACJobExecutionCoordinator):
            def preProcess(self, job):
                # Custom pre-processing code here
                pass

            def postProcess(self, job):
                # Custom post-processing code here
                pass

    In this example, `JobExecutionCoordinator` inherits from `DiracJobExecutionCoordinator`
    and provides custom implementations for the `preProcess` and `postProcess` methods.

    **Methods to Override:**

    - `preProcess(job)`
    - `postProcess(job)`
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
