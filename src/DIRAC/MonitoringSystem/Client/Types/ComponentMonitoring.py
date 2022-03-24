"""
ComponentMonitoring type used to monitor DIRAC components.
"""
from DIRAC.MonitoringSystem.Client.Types.BaseType import BaseType


class ComponentMonitoring(BaseType):
    """
    .. class:: ComponentMonitoring
    """

    def __init__(self):
        """c'tor

        :param self: self reference
        """

        super(ComponentMonitoring, self).__init__()

        self.keyFields = ["host", "component", "pid", "status", "componentType", "componentLocation"]

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

        self.addMapping(
            {
                "host": {"type": "keyword"},
                "component": {"type": "keyword"},
                "status": {"type": "keyword"},
                "componentType": {"type": "keyword"},
                "componentLocation": {"type": "keyword"},
            }
        )

        self.period = "month"
        self.checkType()
