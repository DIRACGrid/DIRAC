#!/usr/bin/env python
"""
Archive a transformation
"""
from DIRAC.Core.Base.Script import Script


@Script()
def main():
    # Registering arguments will automatically add their description to the help menu
    Script.registerArgument(["transID: transformation ID"])
    _, args = Script.parseCommandLine()

    transIDs = [int(arg) for arg in args]

    from DIRAC.TransformationSystem.Agent.TransformationCleaningAgent import TransformationCleaningAgent

    agent = TransformationCleaningAgent(
        "Transformation/TransformationCleaningAgent",
        "Transformation/TransformationCleaningAgent",
        "dirac-transformation-archive",
    )
    agent.initialize()

    for transID in transIDs:
        agent.archiveTransformation(transID)


if __name__ == "__main__":
    main()
