""" StorageOccupancy records the Storage Elements occupancy over time
"""
from DIRAC.AccountingSystem.Client.Types.BaseAccountingType import BaseAccountingType


class StorageOccupancy(BaseAccountingType):
    """StorageOccupancy as extension of BaseAccountingType.

    It is filled by the RSS Command FreeDiskSpace every time the
    command is executed (from Agent CacheFeederAgent)
    """

    def __init__(self):
        """constructor."""
        super().__init__()

        self.definitionKeyFields = [
            ("Site", "VARCHAR(64)"),
            ("Endpoint", "VARCHAR(255)"),
            ("StorageElement", "VARCHAR(64)"),
            ("SpaceType", "VARCHAR(64)"),
        ]  # (Total, Free, Used)

        self.definitionAccountingFields = [("Space", "BIGINT UNSIGNED")]

        self.bucketsLength = [
            (86400 * 2, 3600),  # <2d = 1h
            (86400 * 10, 3600 * 6),  # <10d = 6h
            (86400 * 40, 3600 * 12),  # <40d = 12h
            (86400 * 30 * 6, 86400 * 2),  # <6m = 2d
            (86400 * 600, 86400 * 7),  # >6m = 1w
        ]

        self.checkType()
