#!/bin/env python
"""
Create a production to replicate files from some storage elements to others

:since:  May 31, 2018
:author: A. Sailer
"""
from DIRAC.Core.Base.Script import Script


@Script()
def main():
    """reads command line parameters, makes check and creates replication transformation"""
    from DIRAC import gLogger, exit as dexit
    from DIRAC.TransformationSystem.Utilities.ReplicationCLIParameters import Params

    clip = Params()
    clip.registerSwitches(Script)
    Script.parseCommandLine()

    from DIRAC.TransformationSystem.Utilities.ReplicationTransformation import createDataTransformation

    if not clip.checkSettings(Script)["OK"]:
        gLogger.error("ERROR: Missing settings")
        dexit(1)
    for metaValue in clip.metaValues:
        resCreate = createDataTransformation(
            flavour=clip.flavour,
            targetSE=clip.targetSE,
            sourceSE=clip.sourceSE,
            metaKey=clip.metaKey,
            metaValue=metaValue,
            extraData=clip.extraData,
            extraname=clip.extraname,
            groupSize=clip.groupSize,
            tGroup=clip.groupName,
            plugin=clip.plugin,
            enable=clip.enable,
        )
        if not resCreate["OK"]:
            gLogger.error("Failed to create Transformation", resCreate["Message"])
            dexit(1)

    dexit(0)


if __name__ == "__main__":
    main()
