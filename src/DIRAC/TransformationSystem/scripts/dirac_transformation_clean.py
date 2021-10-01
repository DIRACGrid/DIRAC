#!/usr/bin/env python
"""
Clean a tranformation
"""
from __future__ import print_function
from __future__ import absolute_import
from __future__ import division

__RCSID__ = "$Id$"

import sys

from DIRAC.Core.Utilities.DIRACScript import DIRACScript as Script


@Script()
def main():
    # Registering arguments will automatically add their description to the help menu
    Script.registerArgument(["transID: transformation ID"])
    _, args = Script.parseCommandLine()

    from DIRAC.TransformationSystem.Agent.TransformationCleaningAgent import TransformationCleaningAgent
    from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient

    transIDs = [int(arg) for arg in args]

    agent = TransformationCleaningAgent(
        "Transformation/TransformationCleaningAgent",
        "Transformation/TransformationCleaningAgent",
        "dirac-transformation-clean",
    )
    agent.initialize()

    client = TransformationClient()
    for transID in transIDs:
        agent.cleanTransformation(transID)


if __name__ == "__main__":
    main()
