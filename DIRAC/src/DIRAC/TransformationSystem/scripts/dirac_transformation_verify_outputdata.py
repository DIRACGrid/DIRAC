#!/usr/bin/env python
"""
Runs checkTransformationIntegrity from ValidateOutputDataAgent on selected Tranformation
"""
from DIRAC.Core.Base.Script import Script


@Script()
def main():
    # Registering arguments will automatically add their description to the help menu
    Script.registerArgument(["transID: transformation ID"])
    _, args = Script.parseCommandLine()

    transIDs = [int(arg) for arg in args]

    from DIRAC.TransformationSystem.Agent.ValidateOutputDataAgent import ValidateOutputDataAgent

    agent = ValidateOutputDataAgent(
        "Transformation/ValidateOutputDataAgent",
        "Transformation/ValidateOutputDataAgent",
        "dirac-transformation-verify-outputdata",
    )
    agent.initialize()

    for transID in transIDs:
        agent.checkTransformationIntegrity(transID)


if __name__ == "__main__":
    main()
