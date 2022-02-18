.. _dev-ts-body-plugins:

Create a Body Plugin
====================

This page briefly explains the steps necessary to add a new BodyPlugin  or an extension.
It assumes the reader has some form of a development and testing setup available to them.

The module :py:mod:`DIRAC.TransformationSystem.Client.BodyPlugin.BaseBody` contains documentation on how to code such a plugin


The ReplicateOrMove Plugin Example
----------------------------------

Imagine a world where you would have storages that are reliable, and some that are less. And imagine that you are creating a transformation to shuffle data around. Now, if the destination of your file is a trustworthy one, you could imagine deleting the original copy. But if the destination is likely to lose your file, you are better off keeping a second copy. That's what our example plugin will do.

.. code-block:: python
   :caption: ReplicateOrMoveBody.py
   :linenos:


    from DIRAC import gLogger
    from DIRAC.RequestManagementSystem.Client.Request import Request
    from DIRAC.RequestManagementSystem.Client.Operation import Operation
    from DIRAC.RequestManagementSystem.Client.File import File

    from DIRAC.TransformationSystem.Client.BodyPlugin.BaseBody import BaseBody


    sLog = gLogger.getLocalSubLogger(__name__)


    class ReplicateOrMoveBody(BaseBody):
        """
        If the destination storage is trustworthy, move the file there,
        otherwise, just do a copy of the file
        """

        # This is needed to know how to serialize the object,
        # all attributes that need to be persisted should be in this list.
        # See :py:class:`~DIRAC.Core.Utilities.JEncode.JSerializable` for more details.
        _attrToSerialize = ["trustworthy", "undependable"]

        def __init__(self, trustworthy=None, undependable=None):
            """C'tor

            :param list trustworthy: trustworthy storage to which we will move the file
            :param list undependable: doubtful storage to which we will rather just copy
            """
            self.trustworthy = trustworthy
            self.undependable = undependable

        def taskToRequest(self, taskID, task, transID):
            """
            Convert a task into a Request with either a ReplicateAndRegister
            or a Move Operation
            """
            req = Request()
            op = Operation()

            # Decide on an operation type
            targetSE = task["TargetSE"]
            if targetSE in self.trustworthy:
                op.Type = "MoveReplica"
            elif targetSE in self.undependable:
                op.Type = "ReplicateAndRegister"
            else:
                sLog.warn("Hum, SE is not in a known list... let's try to assess the quality of the storage...")
                # VERY doubtful storage
                if "RAL" in targetSE:
                    op.Type = "ReplicateAndRegister"
                else:
                    op.Type = "MoveReplica"

            for lfn in task["InputData"]:
                trFile = File()
                trFile.LFN = lfn

                op.addFile(trFile)

            req.addOperation(op)

            return req



Using the Body Plugin
---------------------

When creating a transformation, just create the BodyPlugin object you want with the appropriate parameters, and set it using ``setBody``

.. code-block:: python
   :caption: createReplicateOrMove.py
   :linenos:

    from DIRAC import gLogger, initialize

    initialize()

    from DIRAC.TransformationSystem.Client.Transformation import Transformation
    from DIRAC.TransformationSystem.Client.BodyPlugin.ReplicateOrMoveBody import (
        ReplicateOrMoveBody,
    )

    myTrans = Transformation()
    uniqueIdentifier = "sensitiveData"
    myTrans.setTransformationName("ReplicateOrMove_%s" % uniqueIdentifier)
    myTrans.setDescription("Move only to trustworthy storages")
    myTrans.setType("Replication")
    myTrans.setTransformationGroup("MyGroup")
    myTrans.setGroupSize(2)

    # Set the Broadcast plugin
    myTrans.setPlugin("Broadcast")
    myTrans.Destinations(1)


    myBody = ReplicateOrMoveBody(
        trustworthy=["CERN-Storage", "CNAF-Storage"], undependable=["NIPNE-Storage"]
    )

    myTrans.setBody(myBody)

    metadata = {"TransformationID": 2}
    myTrans.setInputMetaQuery(metadata)

    res = myTrans.addTransformation()
    if not res["OK"]:
        gLogger.error("Failed to add the transformation: %s" % res["Message"])
        exit(1)

    # now activate the transformation
    myTrans.setStatus("Active")
    myTrans.setAgentType("Automatic")
    transID = myTrans.getTransformationID()["Value"]

    gLogger.notice("Created ReplicateOrMove transformation: %r" % transID)
    exit(0)
