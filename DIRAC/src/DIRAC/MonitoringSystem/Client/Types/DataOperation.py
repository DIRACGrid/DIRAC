"""
Monitoring Type for Data Operation
"""

from DIRAC.MonitoringSystem.Client.Types.BaseType import BaseType


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
            "Channel",
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
                "TransferSize": {"type": "long"},
                "TransferTime": {"type": "long"},
                "RegistrationTime": {"type": "long"},
                "TransferOK": {"type": "long"},
                "TransferTotal": {"type": "long"},
                "RegistrationOK": {"type": "long"},
                "RegistrationTotal": {"type": "long"},
            }
        )

        self.checkType()
