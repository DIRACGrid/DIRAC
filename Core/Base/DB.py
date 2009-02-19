########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/Base/DB.py,v 1.7 2009/02/19 09:55:00 acasajus Exp $
########################################################################

""" BaseDB is the base class for multiple DIRAC databases. It uniforms the
    way how the database objects are constructed
"""

__RCSID__ = "$Id: DB.py,v 1.7 2009/02/19 09:55:00 acasajus Exp $"

import sys, types
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

########################################################################################
#
#  Utility functions
#
########################################################################################
  def buildCondition(self, condDict, older=None, newer=None, timeStamp=None):
    """ Build SQL condition statement from provided condDict and other extra check on
        a specified time stamp.
        The conditions dictionary specifies for each attribute one or a List of possible
        values
    """
    condition = ''
    conjunction = "WHERE"

    if condDict != None:
      for attrName, attrValue in condDict.items():
        if type(attrValue) == types.ListType:
          multiValue = ','.join(['"'+x.strip()+'"' for x in attrValue])
          condition = ' %s %s %s in (%s)' % ( condition,
                                              conjunction,
                                              str(attrName),
                                              multiValue  )
        else:
          condition = ' %s %s %s=\'%s\'' % ( condition,
                                             conjunction,
                                             str(attrName),
                                             str(attrValue)  )
        conjunction = "AND"

    if timeStamp:
      if older:
        condition = ' %s %s %s < \'%s\'' % ( condition,
                                             conjunction,
                                             timeStamp,
                                             str(older) )
        conjunction = "AND"

      if newer:
        condition = ' %s %s %s >= \'%s\'' %  ( condition,
                                               conjunction,
                                               timeStamp,
                                               str(newer) )

    return condition

#########################################################################################
  def getCounters(self, table, attrList, condDict, older=None, newer=None, timeStamp=None):
    """ Count the number of records on each distinct combination of AttrList, selected
        with condition defined by condDict and time stamps
    """

    cond = self.buildCondition( condDict, older, newer, timeStamp)
    attrNames = ','.join(map(lambda x: str(x),attrList ))
    cmd = 'SELECT %s,COUNT(*) FROM %s %s GROUP BY %s ' % (attrNames,table,cond,attrNames)
    result = self._query( cmd )
    if not result['OK']:
      return result

    resultList = []
    for raw in result['Value']:
      attrDict = {}
      for i in range(len(attrList)):
        attrDict[attrList[i]] = raw[i]
      item = (attrDict,raw[len(attrList)])
      resultList.append(item)
    return S_OK(resultList)

#############################################################################
  def getDistinctAttributeValues(self,table,attribute,condDict = {}, older = None, newer=None, timeStamp=None):
    """ Get distinct values of a table attribute under specified conditions
    """

    cmd = 'SELECT  DISTINCT(%s) FROM %s ORDER BY %s' % (attribute,table,attribute)
    cond = self.buildCondition( condDict, older=older, newer=newer, timeStamp=timeStamp )
    result = self._query( cmd + cond )
    if not result['OK']:
      return result

    attr_list = [ x[0] for x in result['Value'] ]
    return S_OK(attr_list)

#############################################################################
  def getCSOption( self, optionName, defaultValue = None ):
    return gConfig.getValue( "/%s/%s" % ( self.cs_path, optionName ), defaultValue )