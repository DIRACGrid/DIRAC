""" Callback when a staging operation is finished """
from DIRAC import S_OK
from DIRAC.RequestManagementSystem.private.OperationHandlerBase import OperationHandlerBase
from DIRAC.WorkloadManagementSystem.Client.JobStateUpdateClient import JobStateUpdateClient


class StagingCallback(OperationHandlerBase):
    """
    .. class:: StagingCallback

       This performs the 'Done' callback to a job waiting for the staging to finish

       Currently, we cannot store the JobID in the field reserved in the Request, because
       then our crapy finalization system will try updating the job (minor) status
       So we store the job ID in the Argument field of operation
    """

    def __init__(self, operation=None, csPath=None):
        """c'tor

        :param Operation operation: an Operation instance
        :param str csPath: CS path for this handler
        """

        super().__init__(operation, csPath)

    def __call__(self):
        """update the job status"""
        # # decode arguments
        jobID = self.operation.Arguments
        self.log.info(f"Performing callback to job {jobID}")

        res = JobStateUpdateClient().updateJobFromStager(jobID, "Done")

        if not res["OK"]:
            self.log.error("Error performing the callback to the job", res)
            return res

        self.operation.Status = "Done"
        self.log.info("Callback from staging done")
        return S_OK()
