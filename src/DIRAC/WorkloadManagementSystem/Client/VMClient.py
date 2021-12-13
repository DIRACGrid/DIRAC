from DIRAC import gLogger
from DIRAC.Core.Base.Client import Client, createClient


@createClient("WorkloadManagement/VirtualMachineManager")
class VMClient(Client):
    def __init__(self, *args, **kwargs):
        super(VMClient, self).__init__(*args, **kwargs)
        self.log = gLogger.getSubLogger("WorkloadManagement/VMClient")
        self.setServer("WorkloadManagement/VirtualMachineManager")
