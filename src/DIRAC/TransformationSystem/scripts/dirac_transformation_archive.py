#!/usr/bin/env python
"""
Archive a transformation
"""

__RCSID__ = "$Id$"

import sys

from DIRAC.Core.Utilities.DIRACScript import DIRACScript as Script


@Script()
def main():
    # Registering arguments will automatically add their description to the help menu
    Script.registerArgument(["transID: transformation ID"])
    _, args = Script.parseCommandLine()

    transIDs = [int(arg) for arg in args]

    from DIRAC.TransformationSystem.Agent.TransformationCleaningAgent import TransformationCleaningAgent
    from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient

    agent = TransformationCleaningAgent(
        "Transformation/TransformationCleaningAgent",
        "Transformation/TransformationCleaningAgent",
        "dirac-transformation-archive",
    )
    agent.initialize()

    client = TransformationClient()
    for transID in transIDs:
        agent.archiveTransformation(transID)


if __name__ == "__main__":
    main()
