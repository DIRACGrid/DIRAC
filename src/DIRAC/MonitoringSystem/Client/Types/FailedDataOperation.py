"""
Monitoring Type for storing individual failures of data operations on LFN
"""

from DIRAC.MonitoringSystem.Client.Types.BaseType import BaseType


class FailedDataOperation(BaseType):
    def __init__(self):

        super().__init__()

        self.keyFields = [
            "LFN",  # LFN of the file with the failure
            "URL",  # URL used in the operation
            "OperationType",  # Type of the operation (getFile, putFile, etc)
            "User",  # Identity used
            "ExecutionSite",  # Where the operation was executed
            "TargetSE",  # Name of the SE that was targeted
            "Protocol",  # Protocol used
            "Error",  # Error message
            "Component",  # Which component sent the error (StorageElement, Stager, etc)
        ]

        self.monitoringFields = ["Errno"]  # if self.monitoringFields is empty, MonitoringDB complains

        self.index = "faileddataoperation_index"

        self.addMapping(
            {
                "LFN": {"type": "keyword"},
                "URL": {"type": "keyword"},
                "OperationType": {"type": "keyword"},
                "User": {"type": "keyword"},
                "ExecutionSite": {"type": "keyword"},
                "TargetSE": {"type": "keyword"},
                "Protocol": {"type": "keyword"},
                "Error": {"type": "text"},
                "Component": {"type": "keyword"},
            }
        )

        self.checkType()
