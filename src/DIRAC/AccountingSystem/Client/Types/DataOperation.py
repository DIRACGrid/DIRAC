from DIRAC.AccountingSystem.Client.Types.BaseAccountingType import BaseAccountingType
import DIRAC


class DataOperation(BaseAccountingType):
    def __init__(self):
        super().__init__()
        self.definitionKeyFields = [
            ("OperationType", "VARCHAR(32)"),
            ("User", "VARCHAR(64)"),
            ("ExecutionSite", "VARCHAR(256)"),
            ("Source", "VARCHAR(32)"),
            ("Destination", "VARCHAR(32)"),
            ("Protocol", "VARCHAR(32)"),
            ("FinalStatus", "VARCHAR(32)"),
        ]
        self.definitionAccountingFields = [
            ("TransferSize", "BIGINT UNSIGNED"),
            ("TransferTime", "FLOAT"),
            ("RegistrationTime", "FLOAT"),
            ("TransferOK", "INT UNSIGNED"),
            ("TransferTotal", "INT UNSIGNED"),
            ("RegistrationOK", "INT UNSIGNED"),
            ("RegistrationTotal", "INT UNSIGNED"),
        ]
        self.bucketsLength = [
            (86400 * 3, 900),  # <3d = 15m
            (86400 * 8, 3600),  # <1w+1d = 1h
            (15552000, 86400),  # >1w+1d <6m = 1d
            (31104000, 604800),  # >6m = 1w
        ]
        self.checkType()
        self.setValueByKey("ExecutionSite", DIRAC.siteName())
