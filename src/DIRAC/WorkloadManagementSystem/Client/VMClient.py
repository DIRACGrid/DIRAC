from __future__ import print_function
from __future__ import division
from __future__ import absolute_import

from DIRAC import gLogger
from DIRAC.Core.Base.Client import Client


class VMClient(Client):

  def __init__(self):
    """c'tor

    :param self: self reference
    """
    Client.__init__(self)
    self.log = gLogger.getSubLogger("WorkloadManagement/VMClient")
    self.setServer("WorkloadManagement/VirtualMachineManager")
