"""
This module is used to create an appropriate object which can be used to insert records to the Monitoring system.
It always try to insert the records directly. In case of failure the monitoring client is used...
"""

from DIRAC.Core.Base.Client import Client
from DIRAC.MonitoringSystem.DB.MonitoringDB import gMonitoringDB


def getMonitoringDB():
    try:
        if gMonitoringDB and gMonitoringDB._connected:
            return gMonitoringDB
    except Exception:
        pass
    return Client(url="Monitoring/Monitoring")
