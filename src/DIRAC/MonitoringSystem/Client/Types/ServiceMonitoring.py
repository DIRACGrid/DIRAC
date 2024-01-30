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
            "ServiceName",
            "Status",
            "Location",
        ]

        self.monitoringFields = [
            "MemoryUsage",
            "CpuPercentage",
            "Connections",
            "Queries",
            "PendingQueries",
            "ActiveQueries",
            "RunningThreads",
            "MaxFD",
            "ResponseTime",
        ]

        self.index = "service_monitoring-index"

        self.addMapping(
            {
                "Host": {"type": "keyword"},
                "ServiceName": {"type": "keyword"},
                "Status": {"type": "keyword"},
                "Location": {"type": "keyword"},
                "MemoryUsage": {"type": "long"},
                "CpuPercentage": {"type": "long"},
                "Connections": {"type": "long"},
                "Queries": {"type": "long"},
                "PendingQueries": {"type": "long"},
                "ActiveQueries": {"type": "long"},
                "RunningThreads": {"type": "long"},
                "MaxFD": {"type": "long"},
                "ResponseTime": {"type": "long"},
            }
        )

        self.period = "week"

        self.checkType()
