""" Class that encapsulate few commonalities of DIRAC DBs
    (MySQL-based, SQLAlchemy-based, ES-based)
"""

from DIRAC import gLogger, gConfig
from DIRAC.ConfigurationSystem.Client.PathFinder import getDatabaseSection


class DIRACDB:
    """Extended in DB, SQLAlchemyDB, ElasticDB"""

    def __init__(self, *args, **kwargs):
        """c'tor

        :param self: self reference
        """
        parentLogger = kwargs.pop("parentLogger", None)
        if not parentLogger:
            parentLogger = gLogger
        self.log = parentLogger.getSubLogger(self.fullname)  # pylint: disable=no-member
        super().__init__(*args, **kwargs)

    def getCSOption(self, optionName, defaultValue=None):
        cs_path = getDatabaseSection(self.fullname)  # pylint: disable=no-member
        return gConfig.getValue(f"/{cs_path}/{optionName}", defaultValue)
