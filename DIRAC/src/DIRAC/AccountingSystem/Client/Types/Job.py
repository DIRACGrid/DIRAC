""" Job accounting type.

    Filled by the JobWrapper (by the jobs) and by the agent "WorloadManagement/StalledJobAgent"
"""
import DIRAC
from DIRAC.AccountingSystem.Client.Types.BaseAccountingType import BaseAccountingType


class Job(BaseAccountingType):
    def __init__(self):
        super().__init__()
        self.definitionKeyFields = [
            ("User", "VARCHAR(64)"),
            ("UserGroup", "VARCHAR(32)"),
            ("JobGroup", "VARCHAR(64)"),
            ("JobType", "VARCHAR(32)"),
            ("JobClass", "VARCHAR(32)"),
            ("ProcessingType", "VARCHAR(256)"),
            ("Site", "VARCHAR(64)"),
            ("FinalMajorStatus", "VARCHAR(32)"),
            ("FinalMinorStatus", "VARCHAR(256)"),
        ]
        self.definitionAccountingFields = [
            ("CPUTime", "INT UNSIGNED"),  # utime + stime + cutime + cstime
            ("NormCPUTime", "INT UNSIGNED"),  # CPUTime * CPUNormalizationFactor
            ("ExecTime", "INT UNSIGNED"),  # elapsed_time (wall time) * numberOfProcessors
            ("InputDataSize", "BIGINT UNSIGNED"),
            ("OutputDataSize", "BIGINT UNSIGNED"),
            ("InputDataFiles", "INT UNSIGNED"),
            ("OutputDataFiles", "INT UNSIGNED"),
            ("DiskSpace", "BIGINT UNSIGNED"),
            ("InputSandBoxSize", "BIGINT UNSIGNED"),
            ("OutputSandBoxSize", "BIGINT UNSIGNED"),
            ("ProcessedEvents", "INT UNSIGNED"),  # unused (normally not filled)
        ]
        self.bucketsLength = [
            (86400 * 8, 3600),  # <1w+1d = 1h
            (86400 * 35, 3600 * 4),  # <35d = 4h
            (86400 * 30 * 6, 86400),  # <6m = 1d
            (86400 * 365, 86400 * 2),  # <1y = 2d
            (86400 * 600, 604800),  # >1y = 1w
        ]

        self.checkType()
        # Fill the site
        self.setValueByKey("Site", DIRAC.siteName())

    def checkRecord(self):
        result = self.getValue("ExecTime")
        if not result["OK"]:
            return result
        execTime = result["Value"]
        if execTime > 33350400:  # 1 year
            return DIRAC.S_ERROR("OOps. More than 1 year of cpu time smells fishy!")
        return DIRAC.S_OK()
