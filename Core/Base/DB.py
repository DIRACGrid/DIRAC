########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/Base/DB.py,v 1.2 2007/05/16 13:39:54 atsareg Exp $
########################################################################

""" BaseDB is the base class for multiple DIRAC databases. It uniforms the
    way how the database objects are constructed
"""

__RCSID__ = "$Id: DB.py,v 1.2 2007/05/16 13:39:54 atsareg Exp $"

import sys
from DIRAC                           import gLogger, gConfig, S_OK, S_ERROR
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


    self.log.always("==================================================")
    #self.log.always("SystemInstance: "+self.system)
    self.log.always("User:           "+self.dbUser)
    self.log.always("Host:           "+self.dbHost)
    #self.log.always("Password:       "+self.dbPass)
    self.log.always("DBName:         "+self.dbName)
    self.log.always("MaxQueue:       "+`self.maxQueueSize`)
    self.log.always("==================================================")
