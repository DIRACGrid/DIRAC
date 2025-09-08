#!/usr/bin/env python
########################################################################
# File :    dirac-wms-job-parameters
# Author :  Stuart Paterson
########################################################################
"""
Retrieve parameters associated to the given DIRAC job

Example:
  $ dirac-wms-job-parameters 1
  {'CPUNormalizationFactor': '6.8',
   'GridCEQueue': 'ce.labmc.inf.utfsm.cl:2119/jobmanager-lcgpbs-prod',
   'HostName': 'wn05.labmc',
   'JobPath': 'JobPath,JobSanity,JobScheduling,TaskQueue',
   'JobSanityCheck': 'Job: 1 JDL: OK,InputData: No input LFNs,  Input Sandboxes: 0, OK.',
   'JobWrapperPID': '599',
   'LocalAccount': 'prod006',
   'LocalBatchID': '',
   'LocalJobID': '277821.ce.labmc.inf.utfsm.cl',
   'Memory(kB)': '858540kB',
   'ModelName': 'Intel(R)Xeon(R)CPU5110@1.60GHz',
   'NormCPUTime(s)': '1.02',
   'OK': 'True',
   'OutputSandboxMissingFiles': 'std.err',
   'PayloadPID': '604',
   'PilotAgent': 'EELADIRAC v1r1; DIRAC v5r12',
   'Pilot_Reference': 'https://lb2.eela.ufrj.br:9000/ktM6WWR1GdkOTm98_hwM9Q',
   'ScaledCPUTime': '115.6',
   'TotalCPUTime(s)': '0.15'}
"""
import DIRAC

from DIRAC.Core.Base.Script import Script


@Script()
def main():
    # Registering arguments will automatically add their description to the help menu
    Script.registerArgument(["JobID:    DIRAC Job ID"])
    _, args = Script.parseCommandLine(ignoreErrors=True)

    from DIRAC.Interfaces.API.Dirac import Dirac, parseArguments

    dirac = Dirac()
    exitCode = 0
    errorList = []

    for job in parseArguments(args):
        result = dirac.getJobParameters(job, printOutput=True)
        if not result["OK"]:
            errorList.append((job, result["Message"]))
            exitCode = 2

    for error in errorList:
        print("ERROR %s: %s" % error)

    DIRAC.exit(exitCode)


if __name__ == "__main__":
    main()
