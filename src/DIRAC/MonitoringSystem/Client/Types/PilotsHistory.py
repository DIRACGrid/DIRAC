""" Definition for PilotsHistory Monitoring type.
    Filled by the agent "WorkloadManagement/PilotsHistoryAgent"
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

        self.keyFields = ["TaskQueueID", "GridSite", "GridType", "Status"]

        self.monitoringFields = ["Pilots"]

        self.index = "pilotshistory_index"

        self.addMapping(
            {
                "TaskQueueID": {"type": "keyword"},
                "GridSite": {"type": "keyword"},
                "GridType": {"type": "keyword"},
                "Status": {"type": "keyword"},
                "Pilots": {"type": "long"},
            }
        )

        self.checkType()
