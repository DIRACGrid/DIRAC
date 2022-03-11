"""
ServiceMonitoring type used to monitor DIRAC services.
"""
from DIRAC.MonitoringSystem.Client.Types.BaseType import BaseType


class ServiceMonitoring(BaseType):
    """
    .. class:: ServiceMonitoring
    """

    def __init__(self):

        super().__init__()

        self.keyFields = [
            "Host",
            "Service",
            "Pid",
            "Status",
            "Location",
        ]

        self.monitoringFields = [
            "RunningTime",
            "MemoryUsage",
            "CpuPercentage",
            "Connections",
            "Queries" "PendingQueries",
            "ActiveQueries",
            "RunningThreads",
            "MaxFD",
            "ServiceResponseTime",
        ]

        self.index = "service_monitoring-index"

        self.addMapping(
            {
                "Host": {"type": "keyword"},
                "Service": {"type": "keyword"},
                "Status": {"type": "keyword"},
                "Location": {"type": "keyword"},
                "RunningTime": {"type": "long"},
                "MemoryUsage": {"type": "long"},
                "CpuPercentage": {"type": "long"},
                "Connections": {"type": "long"},
                "Queries": {"type": "long"},
                "PendingQueries": {"type": "long"},
                "ActiveQueries": {"type": "long"},
                "RunningThreads": {"type": "long"},
                "MaxFD": {"type": "long"},
                "ServiceResponseTime": {"type": "long"},
            }
        )

        self.dataToKeep = 86400 * 30  # we need to define...

        self.period = "month"
        self.checkType()
