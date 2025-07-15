""" Definition for PilotsHistory Monitoring type.
    Filled by the agent "WorkloadManagement/StatesAccountingAgent"
"""

from DIRAC.MonitoringSystem.Client.Types.BaseType import BaseType


class PilotsHistory(BaseType):
    """
    .. class:: PilotsHistoryMonitorType
    """

    def __init__(self):
        """
        :param self: self reference
        """

        super().__init__()

        self.keyFields = ["GridSite", "ComputingElement", "GridType", "Status", "VO"]

        self.monitoringFields = ["NumOfPilots"]

        self.index = "pilotshistory_index"

        self.addMapping(
            {
                "GridSite": {"type": "keyword"},
                "ComputingElement": {"type": "keyword"},
                "GridType": {"type": "keyword"},
                "Status": {"type": "keyword"},
                "VO": {"type": "keyword"},
                "NumOfPilots": {"type": "long"},
            }
        )

        self.checkType()
