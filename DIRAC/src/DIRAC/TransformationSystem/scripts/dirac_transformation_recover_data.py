#!/bin/env python
"""
Script to call the DataRecoveryAgent functionality by hand.
"""
from DIRAC import S_OK, gLogger
from DIRAC.Core.Base.Script import Script


class Params:
    """Collection of Parameters set via CLI switches."""

    def __init__(self):
        self.enabled = False
        self.transID = 0

    def setEnabled(self, _):
        self.enabled = True
        return S_OK()

    def setTransID(self, transID):
        self.transID = int(transID)
        return S_OK()

    def registerSwitches(self):
        Script.registerSwitch("T:", "TransID=", "TransID to Check/Fix", self.setTransID)
        Script.registerSwitch("X", "Enabled", "Enable the changes", self.setEnabled)


@Script()
def main():
    PARAMS = Params()
    PARAMS.registerSwitches()
    Script.parseCommandLine(ignoreErrors=False)

    # Create Data Recovery Agent and run over single transformation.
    from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient
    from DIRAC.TransformationSystem.Agent.DataRecoveryAgent import DataRecoveryAgent

    DRA = DataRecoveryAgent("Transformation/DataRecoveryAgent", "Transformation/DataRecoveryAgent")
    DRA.jobStatus = ["Done", "Failed"]
    DRA.enabled = PARAMS.enabled
    TRANSFORMATION = TransformationClient().getTransformations(condDict={"TransformationID": PARAMS.transID})
    if not TRANSFORMATION["OK"]:
        gLogger.error(f"Failed to find transformation: {TRANSFORMATION['Message']}")
        exit(1)
    if not TRANSFORMATION["Value"]:
        gLogger.error("Did not find any transformations")
        exit(1)
    TRANS_INFO_DICT = TRANSFORMATION["Value"][0]
    TRANS_INFO_DICT.pop("Body", None)
    gLogger.notice(f"Found transformation: {TRANS_INFO_DICT}")
    DRA.treatTransformation(PARAMS.transID, TRANS_INFO_DICT)
    exit(0)


if __name__ == "__main__":
    main()
