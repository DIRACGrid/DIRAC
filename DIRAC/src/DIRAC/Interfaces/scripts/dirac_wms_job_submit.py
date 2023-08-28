#!/usr/bin/env python
########################################################################
# File :    dirac-wms-job-submit
# Author :  Stuart Paterson
########################################################################
"""
Submit jobs to DIRAC WMS

Example:
  $ dirac-wms-job-submit Simple.jdl
  JobID = 11
"""
import os

import DIRAC
from DIRAC.Core.Base.Script import Script


@Script()
def main():
    Script.registerSwitch("f:", "File=", "Writes job ids to file <value>")
    Script.registerSwitch("r:", "UseJobRepo=", "Use the job repository")
    # Registering arguments will automatically add their description to the help menu
    Script.registerArgument(["JDL:    Path to JDL file"])
    sws, args = Script.parseCommandLine(ignoreErrors=True)

    from DIRAC.Interfaces.API.Dirac import Dirac

    unprocessed_switches = sws
    use_repo = False
    repo_name = ""
    for sw, value in unprocessed_switches:
        if sw.lower() in ["r", "usejobrepo"]:
            use_repo = True
            repo_name = value
            repo_name = repo_name.replace(".cfg", ".repo")
    dirac = Dirac(use_repo, repo_name)
    exitCode = 0
    errorList = []

    jFile = None
    for sw, value in unprocessed_switches:
        if sw.lower() in ("f", "file"):
            if os.path.isfile(value):
                print(f"Appending job ids to existing logfile: {value}")
                if not os.access(value, os.W_OK):
                    print(f"Existing logfile {value} must be writable by user.")
            jFile = open(value, "a")

    for jdl in args:
        result = dirac.submitJob(jdl)
        if result["OK"]:
            print(f"JobID = {result['Value']}")
            if jFile is not None:
                # parametric jobs
                if isinstance(result["Value"], list):
                    jFile.write("\n".join(str(p) for p in result["Value"]))
                    jFile.write("\n")
                else:
                    jFile.write(str(result["Value"]) + "\n")
        else:
            errorList.append((jdl, result["Message"]))
            exitCode = 2

    if jFile is not None:
        jFile.close()

    for error in errorList:
        print("ERROR %s: %s" % error)

    DIRAC.exit(exitCode)


if __name__ == "__main__":
    main()
