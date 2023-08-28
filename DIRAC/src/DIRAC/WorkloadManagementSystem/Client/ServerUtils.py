"""
This module is used to create an appropriate object which can be used to insert records to the WMS.
It always try to insert the records directly. In case of failure a WMS client is used...
"""

from DIRAC.Core.Base.Client import Client
from DIRAC.WorkloadManagementSystem.DB.PilotAgentsDB import gPilotAgentsDB


def getPilotAgentsDB():
    try:
        if gPilotAgentsDB and gPilotAgentsDB._connected:
            return gPilotAgentsDB
    except Exception:
        pass
    return Client(url="WorkloadManagement/PilotManager")
