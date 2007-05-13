########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/WorkloadManagementSystem/DB/Attic/BaseDB.py,v 1.1 2007/05/13 21:15:31 atsareg Exp $
########################################################################

""" BaseDB is the base class for multiple DIRAC databases. It uniforms the
    way how the database objects are constructed
"""

__RCSID__ = "$Id: BaseDB.py,v 1.1 2007/05/13 21:15:31 atsareg Exp $"

import sys
from DIRAC                           import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.Utilities.MySQL      import MySQL 

########################################################################
class BaseDB(MySQL):

  def __init__(self,dbname,systemInstance,maxQueueSize):
  
    self.database_name = dbname 
    self.system = systemInstance
    self.cs_path = '/Databases/'+self.database_name+'/'+self.system
    
    self.gLogger = gLogger.getSubLogger(self.database_name)
    self.gLogger.initialize(self.database_name,self.cs_path)
        
    self.dbHost = ''
    result = gConfig.getOption( self.cs_path+'/Host')
    if not result['OK']:
      self.gLogger.fatal('Failed to get the configuration parameters: Host')
      return
    self.dbHost = result['Value']
    self.dbUser = ''
    result = gConfig.getOption( self.cs_path+'/User')
    if not result['OK']:
      self.gLogger.fatal('Failed to get the configuration parameters: User')
      return    
    self.dbUser = result['Value']
    self.dbPass = ''
    result = gConfig.getOption( self.cs_path+'/Password')
    if not result['OK']:
      self.gLogger.fatal('Failed to get the configuration parameters: Password')
      return  
    self.dbPass = result['Value']
    self.dbName = ''  
    result = gConfig.getOption( self.cs_path+'/DBName')
    if not result['OK']:
      self.gLogger.fatal('Failed to get the configuration parameters: DBName')
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
      self.gLogger.fatal(err)
      sys.exit(err)
     
           
    self.gLogger.always("==================================================")
    self.gLogger.always("SystemInstance: "+self.system)
    self.gLogger.always("User:           "+self.dbUser)
    self.gLogger.always("Host:           "+self.dbHost)
    #self.gLogger.always("Password:       "+self.dbPass)
    self.gLogger.always("DBName:         "+self.dbName)
    self.gLogger.always("MaxQueue:       "+`self.maxQueueSize`)
    self.gLogger.always("==================================================") 
