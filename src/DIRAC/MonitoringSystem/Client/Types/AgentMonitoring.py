"""
AgentMonitoring type used to monitor DIRAC agents.
"""
from DIRAC.MonitoringSystem.Client.Types.BaseType import BaseType


class AgentMonitoring(BaseType):
    """
    .. class:: AgentMonitoring
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
            "cpuPercentage",
            "cycleDuration",
            "cycles",
        ]

        self.index = "agent_monitoring-index"

        self.addMapping(
            {
                "host": {"type": "keyword"},
                "component": {"type": "keyword"},
                "status": {"type": "keyword"},
                "componentType": {"type": "keyword"},
                "componentLocation": {"type": "keyword"},
                "runningTime": {"type": "long"},
                "memoryUsage": {"type": "long"},
                "cpuPercentage": {"type": "long"},
                "cycleDuration": {"type": "long"},
                "cycles": {"type": "long"},
            }
        )

        self.dataToKeep = 86400 * 30  # we need to define...

        self.period = "month"
        self.checkType()
