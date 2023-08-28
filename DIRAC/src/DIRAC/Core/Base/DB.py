""" DB is a base class for multiple DIRAC databases that are based on MySQL.
    It uniforms the way the database objects are constructed
"""
from DIRAC.Core.Base.DIRACDB import DIRACDB
from DIRAC.Core.Utilities.MySQL import MySQL
from DIRAC.ConfigurationSystem.Client.Utilities import getDBParameters


class DB(DIRACDB, MySQL):
    """All DIRAC DB classes should inherit from this one (unless using sqlalchemy)"""

    def __init__(self, dbname, fullname, debug=False, parentLogger=None):
        self.fullname = fullname

        result = getDBParameters(fullname)
        if not result["OK"]:
            raise RuntimeError(f"Cannot get database parameters: {result['Message']}")

        dbParameters = result["Value"]
        self.dbHost = dbParameters["Host"]
        self.dbPort = dbParameters["Port"]
        self.dbUser = dbParameters["User"]
        self.dbPass = dbParameters["Password"]
        self.dbName = dbParameters["DBName"]

        super().__init__(
            hostName=self.dbHost,
            userName=self.dbUser,
            passwd=self.dbPass,
            dbName=self.dbName,
            port=self.dbPort,
            debug=debug,
            parentLogger=parentLogger,
        )

        if not self._connected:
            raise RuntimeError(f"Can not connect to DB '{self.dbName}', exiting...")

        self.log.info("===================== MySQL ======================")
        self.log.info("User:           " + self.dbUser)
        self.log.info("Host:           " + self.dbHost)
        self.log.info("Port:           " + str(self.dbPort))
        # self.log.info("Password:       "+ self.dbPass)
        self.log.info("DBName:         " + self.dbName)
        self.log.info("==================================================")
