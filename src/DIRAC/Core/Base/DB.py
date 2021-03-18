""" DB is a base class for multiple DIRAC databases that are based on MySQL.
    It uniforms the way the database objects are constructed
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Base.DIRACDB import DIRACDB
from DIRAC.Core.Utilities.MySQL import MySQL
from DIRAC.ConfigurationSystem.Client.Utilities import getDBParameters

__RCSID__ = "$Id$"


class DB(DIRACDB, MySQL):
  """ All DIRAC DB classes should inherit from this one (unless using sqlalchemy)
  """

  def __init__(self, dbname, fullname, debug=False):
    """ C'or

        :param str dbname: database name
        :param str fullname: full name
        :param bool debug: debug mode
    """
    self.versionDB = 0
    self.fullname = fullname
    self.versionTable = '%s_Version' % dbname

    result = getDBParameters(fullname)
    if not result['OK']:
      raise RuntimeError('Cannot get database parameters: %s' % result['Message'])

    dbParameters = result['Value']
    self.dbHost = dbParameters['Host']
    self.dbPort = dbParameters['Port']
    self.dbUser = dbParameters['User']
    self.dbPass = dbParameters['Password']
    self.dbName = dbParameters['DBName']

    super(DB, self).__init__(hostName=self.dbHost,
                             userName=self.dbUser,
                             passwd=self.dbPass,
                             dbName=self.dbName,
                             port=self.dbPort,
                             debug=debug)

    if not self._connected:
      raise RuntimeError("Can not connect to DB '%s', exiting..." % self.dbName)

    # Initialize version
    result = self._query("show tables")
    if result['OK']:
      if self.versionTable not in [t[0] for t in result['Value']]:
        result = self._createTables({self.versionTable: {'Fields': {'Version': 'INTEGER NOT NULL'},
                                                         'PrimaryKey': 'Version'}})
    if not result['OK']:
      raise RuntimeError("Can not initialize %s version: %s" % (self.dbName, result['Message']))
    result = self._query("SELECT Version FROM `%s`" % self.versionTable)
    if result['OK']:
      if len(result['Value']) > 0:
        self.versionDB = result['Value'][0][0]
      else:
        result = self._update("INSERT INTO `%s` (Version) VALUES (%s)" % (self.versionTable, self.versionDB))
    if not result['OK']:
      raise RuntimeError("Can not initialize %s version: %s" % (self.dbName, result['Message']))

    self.log.info("===================== MySQL ======================")
    self.log.info("User:           " + self.dbUser)
    self.log.info("Host:           " + self.dbHost)
    self.log.info("Port:           " + str(self.dbPort))
    #self.log.info("Password:       "+self.dbPass)
    self.log.info("DBName:         " + self.dbName)
    self.log.info("==================================================")

  def updateDBVersion(self, version):
    """ Update DB version

        :param int version: version number

        :return: S_OK()/S_ERROR()
    """
    result = self._query('DELETE FROM `%s_Version`' % self.dbName)
    if result['OK']:
      result = self._update("INSERT INTO `%s_Version` (Version) VALUES (%s)" % (self.dbName, version))
    if not result['OK']:
      return S_ERROR("Can not initialize %s version: %s" % (self.dbName, result['Message']))
    self.versionDB = version
    return S_OK()
