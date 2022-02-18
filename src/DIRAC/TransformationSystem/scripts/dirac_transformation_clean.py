#!/usr/bin/env python
"""
Clean a tranformation
"""
from DIRAC.Core.Base.Script import Script


@Script()
def main():
    # Registering arguments will automatically add their description to the help menu
    Script.registerArgument(["transID: transformation ID"])
    _, args = Script.parseCommandLine()

    from DIRAC.TransformationSystem.Agent.TransformationCleaningAgent import TransformationCleaningAgent

    transIDs = [int(arg) for arg in args]

    agent = TransformationCleaningAgent(
        "Transformation/TransformationCleaningAgent",
        "Transformation/TransformationCleaningAgent",
        "dirac-transformation-clean",
    )
    agent.initialize()

    for transID in transIDs:
        agent.cleanTransformation(transID)


if __name__ == "__main__":
    main()
