"""
Monitoring Type for Data Operation
"""

from DIRAC.MonitoringSystem.Client.Types.BaseType import BaseType
import DIRAC


class DataOperation(BaseType):
    def __init__(self):

        super().__init__()

        self.keyFields = [
            "OperationType",
            "User",
            "ExecutionSite",
            "Source",
            "Destination",
            "Protocol",
            "FinalStatus",
        ]

        self.monitoringFields = [
            "TransferSize",
            "TransferTime",
            "RegistrationTime",
            "TransferOK",
            "TransferTotal",
            "RegistrationOK",
            "RegistrationTotal",
        ]

        self.index = "dataoperation_index"

        self.addMapping(
            {
                "OperationType": {"type": "keyword"},
                "User": {"type": "keyword"},
                "ExecutionSite": {"type": "keyword"},
                "Source": {"type": "keyword"},
                "Destination": {"type": "keyword"},
                "Protocol": {"type": "keyword"},
                "FinalStatus": {"type": "keyword"},
            }
        )

        self.checkType()
        self.keyFields["ExecutionSite"] = DIRAC.siteName()
