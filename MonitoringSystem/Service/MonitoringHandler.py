# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/MonitoringSystem/Service/MonitoringHandler.py,v 1.1 2007/11/12 19:01:06 acasajus Exp $
__RCSID__ = "$Id: MonitoringHandler.py,v 1.1 2007/11/12 19:01:06 acasajus Exp $"
import types
import os
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.Core.Utilities.ReturnValues import S_OK, S_ERROR
from DIRAC.MonitoringSystem.Client.MonitoringClient import gMonitor
from DIRAC import gLogger, gConfig, rootPath
from DIRAC.ConfigurationSystem.Client import PathFinder
from DIRAC.MonitoringSystem.private.ActivitiesManager import ActivitiesManager

def initializeMonitoringHandler( serviceInfo ):
  #Check that the path is writable
  monitoringSection = PathFinder.getServiceSection( "Monitoring/Server" )
  #Get data location
  retDict = gConfig.getOption( "%s/DataLocation" % monitoringSection )
  if not retDict[ 'OK' ]:
    return retDict
  dataPath = retDict[ 'Value' ].strip()
  if "/" != dataPath[0]:
    dataPath = os.path.realpath( "%s/%s" % ( rootPath, dataPath ) )
  gLogger.info( "Data will be written into %s" % dataPath )
  try:
    os.makedirs( dataPath )
  except:
    pass
  try:
    fd = file( "%s/writableTest" % dataPath, "w" )
    fd.close()
  except IOError:
    gLogger.fatal( "Can't write to %s" % dataPath )
    return S_ERROR( "Data location is not writable" )
  #Check that the db can be started
  acManager = ActivitiesManager( dataPath )
  if not acManager.createSchema():
    return S_ERROR( "Can't start db engine" )
  #TODO: Load rrd dest
  return S_OK()

class MonitoringHandler( RequestHandler ):

  def __checkActivityDict( self, acDict ):
    validKeys = ( "name", "category", "unit", "operation", "buckets" )
    for key in acDict:
      if key not in validKeys:
        return False
    return True

  def __checkSourceDict( self, sourceDict ):
    validKeys = ( "setup", "site", "componentType", "componentLocation", "componentName" )
    for key in sourceDict:
      if key not in validKeys:
        return False
    return True

  types_registerActivities = [ types.DictType, types.DictType ]
  def export_registerActivities( self, sourceDict, activitiesDict ):
    acManager = ActivitiesManager()
    #Register source
    if not self.__checkSourceDict( sourceDict ):
      return S_ERROR( "Source definition is not valid" )
    sourceId = acManager.registerSource( sourceDict )
    #Register activities
    for name in activitiesDict:
      activitiesDict[ name ][ 'name' ] = name
      if not self.__checkActivityDict( activitiesDict[ name ] ):
        return S_ERROR( "Activity %s definition is not valid" % name )
      gLogger.info( "Received activity", "%s [%s]" % ( name, str( activitiesDict[ name ] ) ) )
      if not acManager.registerActivity( sourceId, name, activitiesDict[ name ] ):
        return S_ERROR( "Could not register activity %s" % name )
    return S_OK( sourceId )

  types_commitMarks = [ types.IntType, types.DictType ]
  def export_commitMarks( self, sourceId, activitiesDict ):
    print "RECEIVED %s" % str( activitiesDict )
    return S_OK()