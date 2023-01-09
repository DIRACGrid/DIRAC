"""
Utilities to create replication transformations
"""
from DIRAC import S_ERROR, S_OK, gLogger
from DIRAC.TransformationSystem.Client import TransformationStatus
from DIRAC.TransformationSystem.Client.Transformation import Transformation
from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient


def getTransformationName(transName, unique=True):
    """
    Get a transformation name from a base name.

    If unique is requested, return None if it already exists and is not in a final state.
    If unique is requested and the transformation is final increments index
    until an unfinished transformation is found (results in None),
    or a final one is found and the name with one higher index is returned.
    """
    tName = transName
    trial = 0
    transClient = TransformationClient()
    while True:
        # Check if there is already a transformation with that name
        res = transClient.getTransformation(tName)
        if not res["OK"]:
            # Transformation doesn't exist, OK
            break

        # Transformation already exists
        if unique:
            if res["Value"]["Status"] not in TransformationStatus.TRANSFORMATION_FINAL_STATES:
                tName = None
                gLogger.notice(
                    f"Transformation {transName} already exists with ID {res['Value']['TransformationID']}, status {res['Value']['Status']}"
                )
                break
        trial += 1
        # Check again with new name
        tName = transName + "-" + str(trial)
    return tName


def createDataTransformation(
    flavour,
    targetSE,
    sourceSE,
    metaKey,
    metaValue,
    extraData=None,
    extraname="",
    groupSize=1,
    plugin="Broadcast",
    tGroup=None,
    tBody=None,
    enable=False,
):
    """Creates the replication transformation based on the given parameters.

    :param str flavour: Flavour of replication to create: Replication or Moving
    :param targetSE: Destination for files
    :type targetSE: python:list or str
    :param sourceSE: Origin of files
    :type sourceSE: python:list or str
    :param int metaKey: Meta key to identify input files
    :param int metaValue: Meta value to identify input files
    :param dict metaData: Additional meta data to use to identify input files
    :param str extraname: addition to the transformation name, only needed if the same transformation was already created
    :param int groupSize: number of files per transformation taks
    :param str plugin: plugin to use
    :param str tGroup: transformation group to set
    :param tBody: transformation body to set
    :param bool enable: if true submit the transformation, otherwise dry run
    :returns: S_OK (with the transformation object, if successfully added), S_ERROR
    """
    metadata = {metaKey: metaValue}
    if isinstance(extraData, dict):
        metadata.update(extraData)

    gLogger.debug("Using %r for metadata search" % metadata)

    if isinstance(targetSE, str):
        targetSE = [targetSE]

    if isinstance(sourceSE, (list, tuple)):
        sourceSE = "%s" % (",".join(sourceSE))

    gLogger.debug("Using plugin: %r" % plugin)

    if flavour not in ("Replication", "Moving"):
        return S_ERROR("Unsupported flavour %s" % flavour)

    transVerb = {"Replication": "Replicate", "Moving": "Move"}[flavour]
    transGroup = {"Replication": "Replication", "Moving": "Moving"}[flavour] if not tGroup else tGroup

    trans = Transformation()
    transName = "{}_{}_{}".format(transVerb, str(metaValue), ",".join(targetSE))
    if extraname:
        transName += "_%s" % extraname

    trans.setTransformationName(getTransformationName(transName))
    description = "{} files for {} {} to {}".format(transVerb, metaKey, str(metaValue), ",".join(targetSE))
    trans.setDescription(description[:255])
    trans.setLongDescription(description)
    trans.setType("Replication")
    trans.setTransformationGroup(transGroup)
    trans.setGroupSize(groupSize)
    trans.setPlugin(plugin)

    transBody = (
        {
            "Moving": [
                ("ReplicateAndRegister", {"SourceSE": sourceSE, "TargetSE": targetSE}),
                ("RemoveReplica", {"TargetSE": sourceSE}),
            ],
            "Replication": "",  # empty body
        }[flavour]
        if tBody is None
        else tBody
    )

    trans.setBody(transBody)
    trans.setInputMetaQuery(metadata)

    if sourceSE:
        res = trans.setSourceSE(sourceSE)
        if not res["OK"]:
            return S_ERROR("SourceSE not valid: %s" % res["Message"])
    res = trans.setTargetSE(targetSE)
    if not res["OK"]:
        return S_ERROR("TargetSE not valid: %s" % res["Message"])

    if not enable:
        gLogger.always("Dry run, not creating transformation")
        return S_OK()

    res = trans.addTransformation()
    if not res["OK"]:
        return res
    gLogger.verbose(res)
    trans.setStatus(TransformationStatus.ACTIVE)
    trans.setAgentType("Automatic")

    gLogger.always("Successfully created replication transformation")
    return S_OK(trans)
