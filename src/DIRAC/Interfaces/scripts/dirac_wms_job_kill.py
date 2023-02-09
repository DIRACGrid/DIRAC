#!/usr/bin/env python
########################################################################
# File :    dirac-wms-job-kill
# Author :  Stuart Paterson
########################################################################
"""
Issue a kill signal to a running DIRAC job

Example:
  $ dirac-wms-job-kill 1918
  Killed job 1918

.. Note::

  - jobs will not disappear from JobDB until JobCleaningAgent has deleted them
  - jobs will be deleted "immediately" if they are in the status 'Deleted'
  - USER jobs will be deleted after a grace period if they are in status Killed, Failed, Done

  What happens when you hit the "kill job" button

  - if the job is in status 'Running', 'Matched', 'Stalled' it will be properly killed, and then its
    status will be marked as 'Killed'
  - otherwise, it will be marked directly as 'Killed'.
"""
import DIRAC
from DIRAC.Core.Base.Script import Script


@Script()
def main():
    # Registering arguments will automatically add their description to the help menu
    Script.registerArgument(["JobID:    DIRAC Job ID"])
    _, args = Script.parseCommandLine(ignoreErrors=True)

    from DIRAC.Interfaces.API.Dirac import Dirac, parseArguments

    result = Dirac().killJob(parseArguments(args))
    if result["OK"]:
        print(f"Killed jobs {','.join([str(j) for j in result['Value']])}")
        exitCode = 0
    else:
        print("ERROR", result["Message"])
        exitCode = 2

    DIRAC.exit(exitCode)


if __name__ == "__main__":
    main()
