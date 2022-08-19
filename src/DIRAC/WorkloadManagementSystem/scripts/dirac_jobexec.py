#!/usr/bin/env python
########################################################################
# File :    dirac-jobexec
# Author :  Stuart Paterson
########################################################################
""" The dirac-jobexec script is equipped to execute workflows that
    are specified via their XML description.  The main client of
    this script is the Job Wrapper.
"""
import os
import os.path
import sys

import DIRAC
from DIRAC.Core.Base.Script import Script


@Script()
def main():
    # Register workflow parameter switch
    Script.registerSwitch("p:", "parameter=", "Parameters that are passed directly to the workflow")
    # Registering arguments will automatically add their description to the help menu
    Script.registerArgument("jobXMLfile: specify path to the Job XML file description")
    Script.parseCommandLine()

    # from DIRAC.Core.Workflow.Parameter import *
    from DIRAC import gLogger
    from DIRAC.Core.Workflow.Workflow import fromXMLFile
    from DIRAC.Core.Utilities.Proxy import executeWithoutServerCertificate
    from DIRAC.WorkloadManagementSystem.Client.JobReport import JobReport
    from DIRAC.AccountingSystem.Client.DataStoreClient import DataStoreClient
    from DIRAC.RequestManagementSystem.Client.Request import Request

    # Forcing the current directory to be the first in the PYTHONPATH
    sys.path.insert(0, os.path.realpath("."))
    gLogger.showHeaders(True)

    @executeWithoutServerCertificate
    def jobexec(jobxml, wfParameters):
        jobfile = os.path.abspath(jobxml)
        if not os.path.exists(jobfile):
            gLogger.warn("Path to specified workflow %s does not exist" % (jobfile))
            sys.exit(1)
        workflow = fromXMLFile(jobfile)
        gLogger.debug(workflow)
        code = workflow.createCode()
        gLogger.debug(code)
        jobID = 0
        if "JOBID" in os.environ:
            jobID = os.environ["JOBID"]
            gLogger.info(f"DIRAC JobID {jobID} is running at site {DIRAC.siteName()}")

        workflow.addTool("JobReport", JobReport(jobID))
        workflow.addTool("AccountingReport", DataStoreClient())
        workflow.addTool("Request", Request())

        # Propagate the command line parameters to the workflow if any
        for pName, pValue in wfParameters.items():
            workflow.setValue(pName, pValue)

        # Propagate the command line parameters to the workflow module instances of each step
        for stepdefinition in workflow.step_definitions.values():
            for moduleInstance in stepdefinition.module_instances:
                for pName, pValue in wfParameters.items():
                    if moduleInstance.parameters.find(pName):
                        moduleInstance.parameters.setValue(pName, pValue)

        return workflow.execute()

    positionalArgs = Script.getPositionalArgs()
    if len(positionalArgs) != 1:
        gLogger.debug("Positional arguments were %s" % (positionalArgs))
        DIRAC.abort(1, "Must specify the Job XML file description")

    if "JOBID" in os.environ:
        gLogger.info("JobID: %s" % (os.environ["JOBID"]))

    jobXMLfile = positionalArgs[0]
    parList = Script.getUnprocessedSwitches()
    parDict = {}
    for switch, parameter in parList:
        if switch == "p":
            name, value = parameter.split("=")
            value = value.strip()

            # The comma separated list in curly brackets is interpreted as a list
            if value.startswith("{"):
                value = value[1:-1].replace('"', "").replace(" ", "").split(",")
                value = ";".join(value)

            parDict[name] = value

    gLogger.debug("PYTHONPATH:\n%s" % ("\n".join(sys.path)))
    jobExec = jobexec(jobXMLfile, parDict)
    if not jobExec["OK"]:
        gLogger.debug("Workflow execution finished with errors, exiting")
        if jobExec["Errno"]:
            os._exit(jobExec["Errno"])
        else:
            os._exit(1)
    else:
        gLogger.debug("Workflow execution successful, exiting")
        # dirac_jobexec might interact with ARC library which cannot be closed using a simple sys.exit(0)
        # See https://bugzilla.nordugrid.org/show_bug.cgi?id=4022 for further details
        os._exit(0)


if __name__ == "__main__":
    main()
