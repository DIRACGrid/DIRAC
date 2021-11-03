import json

from DIRAC.RequestManagementSystem.Client.Request import Request
from DIRAC.RequestManagementSystem.Client.Operation import Operation

from DIRAC.TransformationSystem.Client.BodyPlugin.BaseBody import BaseBody


class DummyBody(BaseBody):
    """Dummy BodyPlugin class that just creates
    a ForwardDISET operation, whose argument
    is the number of LFNs multiplied by a factor
    It is used mostly as an example and for unit tests.
    """

    # This is needed to know how to serialize the object
    _attrToSerialize = ["factor"]

    def __init__(self, factor=0):
        """C'tor

        :param factor: multiplying factor
        """
        self.factor = factor

    def taskToRequest(self, taskID, task, transID):
        """Convert a task into an RMS with a single ForwardDISET
        Operation whose attribute is the number of files in the task
        """
        req = Request()
        op = Operation()
        op.Type = "ForwardDISET"
        op.Arguments = json.dumps(len(task["InputData"]) * self.factor)
        req.addOperation(op)
        return req
