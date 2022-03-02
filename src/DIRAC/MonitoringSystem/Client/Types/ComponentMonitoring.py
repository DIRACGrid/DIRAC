"""
ComponentMonitoring type used to monitor DIRAC components.
"""
from DIRAC.MonitoringSystem.Client.Types.BaseType import BaseType


class ComponentMonitoring(BaseType):
    """
    .. class:: ComponentMonitoring
    """

    def __init__(self):

        super().__init__()

        self.keyFields = [
            "host",
            "component",
            "pid",
            "status",
            "componentType",
            "componentLocation",
        ]

        self.monitoringFields = [
            "runningTime",
            "memoryUsage",
            "threads",
            "cpuPercentage",
            "Connections",
            "PendingQueries",
            "ActiveQueries",
            "RunningThreads",
            "MaxFD",
            "ServiceResponseTime",
            "cycleDuration",
            "cycles",
        ]

        self.index = "component_monitoring-index"

        self.addMapping(
            {
                "host": {"type": "keyword"},
                "component": {"type": "keyword"},
                "status": {"type": "keyword"},
                "componentType": {"type": "keyword"},
                "componentLocation": {"type": "keyword"},
                "runningTime": {"type": "long"},
                "memoryUsage": {"type": "long"},
                "threads": {"type": "long"},
                "cpuPercentage": {"type": "long"},
                "Connections": {"type": "long"},
                "PendingQueries": {"type": "long"},
                "ActiveQueries": {"type": "long"},
                "RunningThreads": {"type": "long"},
                "MaxFD": {"type": "long"},
                "ServiceResponseTime": {"type": "long"},
                "cycleDuration": {"type": "long"},
                "cycles": {"type": "long"},
            }
        )

        self.dataToKeep = 86400 * 30  # we need to define...

        self.period = "month"
        self.checkType()
