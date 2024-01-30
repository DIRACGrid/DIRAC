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
            "Host",
            "AgentName",
            "Status",
            "Location",
        ]

        self.monitoringFields = [
            "MemoryUsage",
            "CpuPercentage",
            "CycleDuration",
        ]

        self.index = "agent_monitoring-index"

        self.addMapping(
            {
                "Host": {"type": "keyword"},
                "AgentName": {"type": "keyword"},
                "Status": {"type": "keyword"},
                "Location": {"type": "keyword"},
                "MemoryUsage": {"type": "long"},
                "CpuPercentage": {"type": "long"},
                "CycleDuration": {"type": "long"},
            }
        )

        self.period = "week"

        self.checkType()
