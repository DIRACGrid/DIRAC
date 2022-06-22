"""
This module is used to create an appropriate object which can be used to insert records to the Monitoring system.
It always try to insert the records directly. In case of failure the monitoring client is used...
"""


def getDBOrClient(DB, serverName):
    """Tries to instantiate the DB object and returns it if we manage to connect to the DB,
    otherwise returns a Client of the server
    """
    from DIRAC import gLogger
    from DIRAC.Core.Base.Client import Client

    try:
        database = DB()
        if database._connected:
            return database
    except Exception:
        pass

    gLogger.info(f"Can not connect to DB will use {serverName}")
    return Client(url=serverName)


def getMonitoringDB():
    serverName = "Monitoring/Monitoring"
    MonitoringDB = None
    try:
        from DIRAC.MonitoringSystem.DB.MonitoringDB import MonitoringDB
    except Exception:
        pass
    return getDBOrClient(MonitoringDB, serverName)
