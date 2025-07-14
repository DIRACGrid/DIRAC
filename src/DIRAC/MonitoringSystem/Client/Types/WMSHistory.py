""" Definition for WMSHistory Monitoring type.
    Drop-in replacement for the Accounting/WMSHistory accounting type.

    Filled by the agent "WorkloadManagement/StatesAccountingAgent"
"""

from DIRAC.MonitoringSystem.Client.Types.BaseType import BaseType


class WMSHistory(BaseType):

    """
    .. class:: WMSMonitorType
    """

    def __init__(self):
        """c'tor

        :param self: self reference
        """

        super().__init__()

        self.keyFields = [
            "Status",
            "Site",
            "User",
            "UserGroup",
            "JobGroup",
            "MinorStatus",
            "ApplicationStatus",
            "JobSplitType",
            "Tier",
            "Type",
        ]

        self.monitoringFields = ["Jobs", "Reschedules"]

        self.index = "wmshistory_index"

        self.addMapping(
            {
                "Status": {"type": "keyword"},
                "Site": {"type": "keyword"},
                "JobSplitType": {"type": "keyword"},
                "ApplicationStatus": {"type": "keyword"},
                "MinorStatus": {"type": "keyword"},
                "User": {"type": "keyword"},
                "JobGroup": {"type": "keyword"},
                "UserGroup": {"type": "keyword"},
                "Tier": {"type": "keyword"},
                "Type": {"type": "keyword"},
            }
        )
        # {'timestamp': {'type': 'date'}} will be added for all monitoring types

        self.checkType()
