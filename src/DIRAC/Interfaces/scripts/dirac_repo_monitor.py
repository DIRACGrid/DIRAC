#!/usr/bin/env python
"""
Monitor the jobs present in the repository
"""
import DIRAC
from DIRAC.Core.Base.Script import Script


@Script()
def main():
    # Registering arguments will automatically add their description to the help menu
    Script.registerArgument("RepoDir:  Location of Job Repository")
    _, args = Script.parseCommandLine(ignoreErrors=False)

    repoLocation = args[0]
    from DIRAC.Interfaces.API.Dirac import Dirac

    dirac = Dirac(withRepo=True, repoLocation=repoLocation)

    result = dirac.monitorRepository(printOutput=True)
    if not result["OK"]:
        print("ERROR: ", result["Message"])
        DIRAC.exit(2)


if __name__ == "__main__":
    main()
