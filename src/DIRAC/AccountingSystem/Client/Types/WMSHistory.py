""" MySQL based WMSHistory accounting.
    It's suggested to replace this with the OpenSearch based WMSHistory monitoring.

    Filled by the agent "WorkloadManagement/StatesAccountingAgent"
"""

from DIRAC.AccountingSystem.Client.Types.BaseAccountingType import BaseAccountingType


class WMSHistory(BaseAccountingType):
    def __init__(self):
        super().__init__()
        self.definitionKeyFields = [
            ("Status", "VARCHAR(128)"),
            ("Site", "VARCHAR(128)"),
            ("User", "VARCHAR(128)"),
            ("UserGroup", "VARCHAR(128)"),
            ("JobGroup", "VARCHAR(32)"),
            ("MinorStatus", "VARCHAR(128)"),
            ("ApplicationStatus", "VARCHAR(256)"),
            ("JobSplitType", "VARCHAR(32)"),
        ]
        self.definitionAccountingFields = [
            ("Jobs", "INT UNSIGNED"),
            ("Reschedules", "INT UNSIGNED"),
        ]
        self.bucketsLength = [
            (86400 * 2, 900),  # <2d = 15m
            (86400 * 10, 9000),  # <10d = 2.5h
            (86400 * 35, 18000),  # <35d = 5h
            (86400 * 30 * 6, 86400),  # >5d <6m = 1d
            (86400 * 600, 604800),  # >6m = 1w
        ]
        self.dataTimespan = 86400 * 30 * 14  # Only keep the last 14 months of data
        self.checkType()
        self.setValueByKey("ApplicationStatus", "unset")
