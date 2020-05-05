from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
import threading
from DIRAC import S_OK
from DIRAC.Core.Utilities import DEncode
from DIRAC.Core.Base.ExecutorModule import ExecutorModule


class PingPongExecutor(ExecutorModule):

  @classmethod
  def initialize(cls):
    """
    Executors need to know to which mind they have to connect.
    """
    cls.ex_setMind("Test/PingPongMind")
    return S_OK()

  def processTask(self, taskid, taskData):
    """
    This is the function that actually does the work. It receives the task,
     does the processing and sends the modified task data back.
    """
    taskData['bouncesLeft'] -= 1
    return S_OK(taskData)

  def deserializeTask(self, taskStub):
    """
    Tasks are received as a stream of bytes. They have to be converted from that into a usable object.
    """
    return S_OK(DEncode.decode(taskStub)[0])

  def serializeTask(self, taskData):
    """
    Before sending the task back to the mind it has to be serialized again.
    """
    return S_OK(DEncode.encode(taskData))
