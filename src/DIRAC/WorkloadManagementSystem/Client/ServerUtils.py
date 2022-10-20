"""
This module is used to create an appropriate object which can be used to insert records to the WMS.
It always try to insert the records directly. In case of failure a WMS client is used...
"""

from DIRAC.Core.Utilities.ServerUtils import getDBOrClient


def getPilotAgentsDB():
    serverName = "WorkloadManagement/PilotManager"
    PilotAgentsDB = None
    try:
        from DIRAC.WorkloadManagementSystem.DB.PilotAgentsDB import PilotAgentsDB
    except Exception:
        pass
    return getDBOrClient(PilotAgentsDB, serverName)


pilotAgentsDB = getPilotAgentsDB()


def getVirtualMachineDB():
    serverName = "WorkloadManagement/VirtualMachineManager"
    VirtualMachineDB = None
    try:
        from DIRAC.WorkloadManagementSystem.DB.VirtualMachineDB import VirtualMachineDB
    except Exception:
        pass
    return getDBOrClient(VirtualMachineDB, serverName)


virtualMachineDB = getVirtualMachineDB()
