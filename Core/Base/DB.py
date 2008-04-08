########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/Base/DB.py,v 1.4 2008/04/08 10:25:00 atsareg Exp $
########################################################################

""" BaseDB is the base class for multiple DIRAC databases. It uniforms the
    way how the database objects are constructed
"""

__RCSID__ = "$Id: DB.py,v 1.4 2008/04/08 10:25:00 atsareg Exp $"

import sys
from DIRAC                           import gLogger, S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client.Config import gConfig
from DIRAC.Core.Utilities.MySQL      import MySQL
from DIRAC.ConfigurationSystem.Client.PathFinder import getDatabaseSection


########################################################################
class DB(MySQL):

  def __init__(self,dbname,fullname,maxQueueSize):

    self.database_name = dbname
    self.fullname = fullname
    self.cs_path = getDatabaseSection(fullname)

    self.log = gLogger.getSubLogger(self.database_name)

    self.dbHost = ''
    result = gConfig.getOption( self.cs_path+'/Host')
    if not result['OK']:
      self.log.fatal('Failed to get the configuration parameters: Host')
      return
    self.dbHost = result['Value']
    self.dbUser = ''
    result = gConfig.getOption( self.cs_path+'/User')
    if not result['OK']:
      self.log.fatal('Failed to get the configuration parameters: User')
      return
    self.dbUser = result['Value']
    self.dbPass = ''
    result = gConfig.getOption( self.cs_path+'/Password')
    if not result['OK']:
      self.log.fatal('Failed to get the configuration parameters: Password')
      return
    self.dbPass = result['Value']
    self.dbName = ''
    result = gConfig.getOption( self.cs_path+'/DBName')
    if not result['OK']:
      self.log.fatal('Failed to get the configuration parameters: DBName')
      return
    self.dbName = result['Value']
    self.maxQueueSize = maxQueueSize
    result = gConfig.getOption( self.cs_path+'/MaxQueueSize')
    if result['OK']:
      self.maxQueueSize = int(result['Value'])

    MySQL.__init__(self, self.dbHost, self.dbUser, self.dbPass,
                   self.dbName, maxQueueSize=maxQueueSize )

    if not self._connected:
      err = 'Can not connect to DB, exiting...'
      self.log.fatal(err)
      sys.exit(err)


    self.log.info("==================================================")
    #self.log.info("SystemInstance: "+self.system)
    self.log.info("User:           "+self.dbUser)
    self.log.info("Host:           "+self.dbHost)
    #self.log.info("Password:       "+self.dbPass)
    self.log.info("DBName:         "+self.dbName)
    self.log.info("MaxQueue:       "+`self.maxQueueSize`)
    self.log.info("==================================================")
