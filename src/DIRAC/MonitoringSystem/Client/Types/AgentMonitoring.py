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
            "Pid",
            "Status",
            "Location",
        ]

        self.monitoringFields = [
            "RunningTime",
            "MemoryUsage",
            "CpuPercentage",
            "CycleDuration",
            "Cycles",
        ]

        self.index = "agent_monitoring-index"

        self.addMapping(
            {
                "Host": {"type": "keyword"},
                "Status": {"type": "keyword"},
                "Location": {"type": "keyword"},
                "RunningTime": {"type": "long"},
                "MemoryUsage": {"type": "long"},
                "CpuPercentage": {"type": "long"},
                "CycleDuration": {"type": "long"},
                "Cycles": {"type": "long"},
            }
        )

        self.dataToKeep = 86400 * 30  # we need to define...

        self.period = "month"
        self.checkType()
