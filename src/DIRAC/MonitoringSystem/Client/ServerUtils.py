"""
This module is used to create an appropriate object which can be used to insert records to the Monitoring system.
It always try to insert the records directly. In case of failure the monitoring client is used...
"""


def getMonitoringDB():
    try:
        from DIRAC.MonitoringSystem.DB.MonitoringDB import gMonitoringDB

        if gMonitoringDB and gMonitoringDB._connected:
            return gMonitoringDB
    except Exception:
        pass

    from DIRAC.Core.Base.Client import Client

    return Client(url="Monitoring/Monitoring")
