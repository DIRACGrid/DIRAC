"""
This module is used to create an appropriate object which can be used to insert records to the Monitoring system.
It always try to insert the records directly. In case of failure the monitoring client is used...
"""

from DIRAC.Core.Utilities.ServerUtils import getDBOrClient


def getMonitoringDB():
    serverName = "Monitoring/Monitoring"
    MonitoringDB = None
    try:
        from DIRAC.MonitoringSystem.DB.MonitoringDB import MonitoringDB
    except Exception:
        pass
    return getDBOrClient(MonitoringDB, serverName)
