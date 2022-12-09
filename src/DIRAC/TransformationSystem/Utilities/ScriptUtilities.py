"""Utilities used by TS scripts."""

from DIRAC import gLogger
from DIRAC.TransformationSystem.Client import TransformationStatus
from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient


def _getTransformationID(transName):
    """Check that a transformation exists and return its ID or None if it doesn't exist.
    The logic looks also for ancestors of the transformation in input.

    :param transName: name or ID of a transformation
    :type transName: int or string

    :return: transformation ID or None if it doesn't exist
    """
    testName = transName
    trClient = TransformationClient()
    # Try out a long range of indices, to find any related datamanagement transformation, see ....
    for ind in range(1, 100):
        result = trClient.getTransformation(testName)
        if not result["OK"]:
            # Transformation doesn't exist
            break
        status = result["Value"]["Status"]
        # If the status is still compatible, accept
        if status in TransformationStatus.TRANSFORMATION_ACTIVE_STATES:
            return result["Value"]["TransformationID"]
        # If transformationID was given, return error
        if isinstance(transName, int) or transName.isdigit():
            gLogger.error("Transformation in incorrect status", f"{testName}, status {status}")
            return None
        # Transformation name given, try out adding an index
        testName = f"{transName}-{ind}"
    return None


def getTransformations(args):
    """Parse the arguments of the script and generates a list of transformations.

    :param str args: a comma-separated list of transformation IDs, ID ranges in the form id1:id2, or names
    :return: a list of Transformation IDs to look at
    """
    if not args:
        return []

    transList = []
    ids = args[0].split(",")
    try:
        for transID in ids:
            rr = transID.split(":")
            if len(rr) > 1:
                for i in range(int(rr[0]), int(rr[1]) + 1):
                    tid = _getTransformationID(i)
                    if tid is not None:
                        transList.append(tid)
            else:
                tid = _getTransformationID(rr[0])
                if tid:
                    transList.append(tid)
                elif tid is None:
                    gLogger.error("Transformation not found", rr[0])
    except Exception as e:
        gLogger.exception("Invalid transformation", lException=e)
        transList = []
    return transList
