# ##WARNING###WARNING###WARNING###WARNING###WARNING###WARNING###WARNING###WARNING###WARNING###WARNING###WARNING #
#                                           Under development                                                   #
# ##WARNING###WARNING###WARNING###WARNING###WARNING###WARNING###WARNING###WARNING###WARNING###WARNING###WARNING #

""" Module to upload specified job output files according to the parameters
    defined in the production workflow.
"""
from DIRAC import gLogger
from DIRAC.Workflow.Modules.ModuleBase import ModuleBase, GracefulTermination


class UploadOutputs(ModuleBase):

    #############################################################################

    def __init__(self):
        """c'tor"""
        self.log = gLogger.getSubLogger(self.__class__.__name__)
        super().__init__(self.log)

        self.outputDataStep = ""
        self.outputData = None
        self.outputList = []
        self.defaultOutputSE = []
        self.outputSE = []
        self.outputPath = ""

    #############################################################################

    def _resolveInputVariables(self):
        """The module parameters are resolved here."""
        super()._resolveInputVariables()

        # this comes from Job().setOutputData(). Typical for user jobs
        if "OutputData" in self.workflow_commons:
            self.outputData = self.workflow_commons["OutputData"]
            if isinstance(self.outputData, str):
                self.outputData = [i.strip() for i in self.outputData.split(";")]
        # if not present, we use the outputList, which is instead incrementally created based on the single step outputs
        # This is more typical for production jobs, that can have many steps linked one after the other
        elif "outputList" in self.workflow_commons:
            self.outputList = self.workflow_commons["outputList"]
        else:
            raise GracefulTermination("Nothing to upload")

        # in case you want to put a mask on the steps
        # TODO: add it to the DIRAC API
        if "outputDataStep" in self.workflow_commons:
            self.outputDataStep = self.workflow_commons["outputDataStep"]

        # this comes from Job().setOutputData(). Typical for user jobs
        if "OutputSE" in self.workflow_commons:
            specifiedSE = self.workflow_commons["OutputSE"]
            if not isinstance(specifiedSE, list):
                self.outputSE = [i.strip() for i in specifiedSE.split(";")]
        else:
            self.log.verbose("No OutputSE specified, using default value: %s" % (", ".join(self.defaultOutputSE)))

        # this comes from Job().setOutputData(). Typical for user jobs
        if "OutputPath" in self.workflow_commons:
            self.outputPath = self.workflow_commons["OutputPath"]

    def _initialize(self):
        """gets the files to upload, check if to upload"""
        # lfnsList = self.__getOutputLFNs( self.outputData ) or outputList?

        if not self._checkWFAndStepStatus():
            raise GracefulTermination("No output data upload attempted")

    def __getOuputLFNs(self, outputList, *args):
        """This is really VO-specific.
        It should be replaced by each VO. Setting an LFN here just as an idea, and for testing purposes.
        """
        lfnList = []
        for outputFile in outputList:
            lfnList.append("/".join([str(x) for x in args]) + outputFile)

        return lfnList

    def _execute(self):
        """uploads the files"""
        pass
