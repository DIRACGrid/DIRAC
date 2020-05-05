""" Create and send a combined request for any pending operations at
    the end of a job:

      * fileReport (for the transformation)
      * jobReport (for jobs)
      * accounting
      * request (for failover)
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from DIRAC import gLogger
from DIRAC.Workflow.Modules.ModuleBase import ModuleBase, GracefulTermination


class FailoverRequest(ModuleBase):

  #############################################################################

  def __init__(self):
    """Module initialization.
    """

    self.log = gLogger.getSubLogger("FailoverRequest")
    super(FailoverRequest, self).__init__(self.log)

  #############################################################################

  def _resolveInputVariables(self):
    """ By convention the module input parameters are resolved here.
    """
    super(FailoverRequest, self)._resolveInputVariables()
    super(FailoverRequest, self)._resolveInputStep()

  def _initialize(self):
    """ checks if is to do, then prepare few things
    """
    if not self._enableModule():
      raise GracefulTermination("Skipping FailoverRequest module")

    self.request.RequestName = 'job_%d_request.xml' % self.jobID
    self.request.JobID = self.jobID
    self.request.SourceComponent = "Job_%d" % self.jobID

  def _execute(self):

    # report on the status of the input data, by default they are 'Processed', unless the job failed
    # failures happening before are not touched
    filesInFileReport = self.fileReport.getFiles()
    if not self._checkWFAndStepStatus(noPrint=True):
      for lfn in self.inputDataList:
        if lfn not in filesInFileReport:
          self.log.info("Forcing status to 'Unused' due to workflow failure for: %s" % (lfn))
          # Set the force flag in case the file was in a terminal status
          self.fileReport.force = True
          self.fileReport.setFileStatus(int(self.production_id), lfn, 'Unused')
    else:
      filesInFileReport = self.fileReport.getFiles()
      for lfn in self.inputDataList:
        if lfn not in filesInFileReport:
          self.log.verbose("No status populated for input data %s, setting to 'Processed'" % lfn)
          self.fileReport.setFileStatus(int(self.production_id), lfn, 'Processed')

    result = self.fileReport.commit()
    if not result['OK']:
      self.log.error("Failed to report file status to TransformationDB")
      self.log.error("Trying again before populating request with file report information")
      result = self.fileReport.generateForwardDISET()
      if not result['OK']:
        self.log.warn("Could not generate Operation for file report with result:\n%s" % (result['Value']))
      else:
        if result['Value'] is None:
          self.log.info("Files correctly reported to TransformationDB")
        else:
          result = self.request.addOperation(result['Value'])
    else:
      self.log.info("Status of files have been properly updated in the TransformationDB")

    # Must ensure that the local job report instance is used to report the final status
    # in case of failure and a subsequent failover operation
    if self.workflowStatus['OK'] and self.stepStatus['OK']:
      self.setApplicationStatus("Job Finished Successfully")

      self.generateFailoverFile()

  def _finalize(self):
    """ Finalize and report correct status for the workflow based on the workflow
        or step status.
    """
    if not self._checkWFAndStepStatus(True):
      raise RuntimeError("Workflow failed, FailoverRequest module completed")

    super(FailoverRequest, self)._finalize("Workflow successful, end of FailoverRequest module execution.")
