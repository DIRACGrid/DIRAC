""" A test DB in DIRAC, using MySQL as backend
"""

from DIRAC.Core.Base.DB import DB

from DIRAC import gLogger, S_OK, S_ERROR


class UserDB(DB):
    """Database system for users"""

    def __init__(self, parentLogger=None):
        """
        Initialize the DB
        """

        super().__init__("UserDB", "Framework/UserDB", parentLogger=parentLogger)
        retVal = self.__initializeDB()
        if not retVal["OK"]:
            raise Exception(f"Can't create tables: {retVal['Message']}")

    def __initializeDB(self):
        """
        Create the tables
        """
        retVal = self._query("show tables")
        if not retVal["OK"]:
            return retVal

        tablesInDB = [t[0] for t in retVal["Value"]]
        tablesD = {}

        if "user_mytable" not in tablesInDB:
            tablesD["user_mytable"] = {
                "Fields": {"Id": "INTEGER NOT NULL AUTO_INCREMENT", "Name": "VARCHAR(64) NOT NULL"},
                "PrimaryKey": ["Id"],
            }

        return self._createTables(tablesD)

    def addUser(self, userName):
        """
        Add a user
          :param str userName: The name of the user we want to add
          :return: S_OK or S_ERROR
        """
        gLogger.verbose(f"Insert {userName} in DB")
        return self.insertFields("user_mytable", ["Name"], [userName])

    def editUser(self, uid, value):
        """
        Edit a user
        :param int uid: The Id of the user in database
        :param str value: New user name
        :return: S_OK or S_ERROR
        """
        return self.updateFields("user_mytable", updateDict={"Name": value}, condDict={"Id": uid})

    def getUserName(self, uid):
        """
        Get a user
        :param int uid: The Id of the user in database
        :return: S_OK with S_OK['Value'] = TheUserName or S_ERROR if not found
        """
        user = self.getFields("user_mytable", condDict={"Id": uid})
        if len(user["Value"]) == 1:
            return S_OK(user["Value"][0][1])
        return S_ERROR("USER NOT FOUND")

    def listUsers(self):
        """
        List all users
        :return: S_OK with S_OK['Value'] list of [UserId, UserName]
        """
        return self._query("SELECT * FROM user_mytable")
