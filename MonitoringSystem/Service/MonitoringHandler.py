# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/MonitoringSystem/Service/MonitoringHandler.py,v 1.2 2007/11/15 16:03:52 acasajus Exp $
__RCSID__ = "$Id: MonitoringHandler.py,v 1.2 2007/11/15 16:03:52 acasajus Exp $"
import types
import os
from DIRAC import gLogger, gConfig, rootPath
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.Core.Utilities.ReturnValues import S_OK, S_ERROR
from DIRAC.MonitoringSystem.Client.MonitoringClient import gMonitor
from DIRAC.ConfigurationSystem.Client import PathFinder

from DIRAC.MonitoringSystem.private.ServiceInterface import gServiceInterface

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
    testFile = "%s/mon.jarl.test" % dataPath
    fd = file( testFile, "w" )
    fd.close()
    os.unlink( testFile )
  except IOError:
    gLogger.fatal( "Can't write to %s" % dataPath )
    return S_ERROR( "Data location is not writable" )
  #Define globals
  gServiceInterface.initialize( dataPath )
  if not gServiceInterface.initializeDB():
    return S_ERROR( "Can't start db engine" )
  #TODO: Load rrd dest
  return S_OK()

class MonitoringHandler( RequestHandler ):

  types_registerActivities = [ types.DictType, types.DictType ]
  def export_registerActivities( self, sourceDict, activitiesDict ):
    return gServiceInterface.registerActivities( sourceDict, activitiesDict)

  types_commitMarks = [ types.IntType, types.DictType ]
  def export_commitMarks( self, sourceId, activitiesDict ):
    return gServiceInterface.commitMarks( sourceId, activitiesDict )

  types_getSources = []
  def export_getSources( self ):
    return gServiceInterface.getSources()