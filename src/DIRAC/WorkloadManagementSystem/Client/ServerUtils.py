"""
  Provide uniform interface to backend for local and remote clients.return

  There's a pretty big assumption here: that DB and Handler expose the same calls, with identical signatures.
  This is not exactly the case for WMS DBs and services.
"""


def getDBOrClient(DB, serverName):
    """Tries to instantiate the DB object
    and returns it if we manage to connect to the DB,
    otherwise returns a Client of the server
    """
    from DIRAC import gLogger
    from DIRAC.Core.Base.Client import Client

    try:
        myDB = DB()
        if myDB._connected:
            return myDB
    except Exception:
        pass

    gLogger.info("Can not connect to DB will use %s" % serverName)
    return Client(url=serverName)


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
