""" Commits monitoring information using gServiceInterface singleton
"""

__RCSID__ = "$Id$"

from types import StringTypes, DictType, IntType, ListType, LongType, TupleType
import os

from DIRAC import gLogger, gConfig, rootPath, S_OK, S_ERROR
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.Core.Utilities import DEncode, Time
from DIRAC.FrameworkSystem.Client.MonitoringClient import gMonitor
from DIRAC.ConfigurationSystem.Client import PathFinder
from DIRAC.FrameworkSystem.private.monitoring.ServiceInterface import gServiceInterface

def initializeMonitoringHandler( serviceInfo ):
  #Check that the path is writable
  monitoringSection = PathFinder.getServiceSection( "Framework/Monitoring" )
  #Get data location
  dataPath = gConfig.getValue( "%s/DataLocation" % monitoringSection, "data/monitoring" )
  dataPath = dataPath.strip()
  if "/" != dataPath[0]:
    dataPath = os.path.realpath( "%s/%s" % ( gConfig.getValue( '/LocalSite/InstancePath', rootPath ), dataPath ) )
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
  gMonitor.registerActivity( "cachedplots", "Cached plot images", "Monitoring plots", "plots", gMonitor.OP_SUM )
  gMonitor.registerActivity( "drawnplots", "Drawn plot images", "Monitoring plots", "plots", gMonitor.OP_SUM )
  return S_OK()

class MonitoringHandler( RequestHandler ):

  types_registerActivities = [ DictType, DictType ]
  def export_registerActivities( self, sourceDict, activitiesDict, componentExtraInfo = {} ):
    """
    Registers new activities
    """
    return gServiceInterface.registerActivities( sourceDict, activitiesDict, componentExtraInfo )

  types_commitMarks = [ IntType, DictType ]
  def export_commitMarks( self, sourceId, activitiesDict, componentExtraInfo = {} ):
    """
    Adds marks for activities
    """
    nowEpoch = Time.toEpoch()
    maxEpoch = nowEpoch + 7200
    minEpoch = nowEpoch - 86400
    invalidActivities = []
    for acName in activitiesDict:
      for time in activitiesDict[ acName ]:
        if time > maxEpoch or time < minEpoch:
          gLogger.info( "Time %s  ( [%s,%s] ) is invalid for activity %s" % ( time, minEpoch, maxEpoch, acName ) )
          invalidActivities.append( acName )
          break
    for acName in invalidActivities:
      gLogger.info( "Not commiting activity %s" % acName )
      del( activitiesDict[ acName ] )
    return gServiceInterface.commitMarks( sourceId, activitiesDict, componentExtraInfo )

  types_queryField = [ StringTypes, DictType ]
  def export_queryField( self, field, definedFields ):
    """
    Returns available values for a field., given a set of fields and values,
    """
    definedFields[ 'sources.setup' ] = self.serviceInfoDict[ 'clientSetup' ]
    return gServiceInterface.fieldValue( field, definedFields )

  types_tryView = [ IntType, IntType, StringTypes ]
  def export_tryView( self, fromSecs, toSecs, viewDescriptionStub ):
    """
      Generates plots based on a DEncoded view description
    """
    viewDescription = DEncode.decode( viewDescriptionStub )
    if not 'definition' in viewDescription:
      return S_ERROR( "No plot definition given" )
    defDict = viewDescription[ 'definition' ]
    defDict[ 'sources.setup' ] = self.serviceInfoDict[ 'clientSetup' ]
    return gServiceInterface.generatePlots( fromSecs, toSecs, viewDescription )

  types_saveView = [ StringTypes, StringTypes ]
  def export_saveView( self, viewName, viewDescriptionStub ):
    """
    Saves a view
    """
    if len( viewName ) == 0:
      return S_OK( "View name not valid" )
    viewDescription = DEncode.decode( viewDescriptionStub )
    if not 'definition' in viewDescription:
      return S_ERROR( "No plot definition given" )
    defDict = viewDescription[ 'definition' ]
    defDict[ 'sources.setup' ] = self.serviceInfoDict[ 'clientSetup' ]
    return gServiceInterface.saveView( viewName, viewDescription )

  types_getViews = []
  def export_getViews( self, onlyStatic = True ):
    """
    Returns a list of stored views
    """
    return gServiceInterface.getViews( onlyStatic )

  types_plotView = [ DictType ]
  def export_plotView( self, viewRequest ):
    """
    Generates plots for a view
    """
    for required in ( "fromSecs", "toSecs", "id" ):
      if required not in viewRequest:
        return S_ERROR( "Missing %s field in request" % required )
    for intFields in ( "fromSecs", "toSecs" ):
      viewRequest[ intFields ] = int( viewRequest[ intFields ] )
    if not "size" in viewRequest:
      viewRequest[ 'size' ] = 1
    if viewRequest[ 'size' ] not in ( 0, 1, 2, 3 ):
      return S_ERROR( "Invalid size" )
    return gServiceInterface.plotView( viewRequest )

  types_deleteView = [ IntType ]
  def export_deleteView( self, viewId ):
    """
    Deletes a view
    """
    return gServiceInterface.deleteView( viewId )

  types_deleteViews = [ ListType ]
  def export_deleteViews( self, viewList ):
    """
    Deletes a view
    """
    for viewId in viewList:
      result = gServiceInterface.deleteView( viewId )
      if not result[ 'OK' ]:
        return result
    return S_OK()

  types_getActivities = []
  def export_getActivities( self ):
    """
    Returns a list of defined activities
    """
    dbCondition = { 'sources.setup' : self.serviceInfoDict[ 'clientSetup' ] }
    return S_OK( gServiceInterface.getActivities( dbCondition ) )

  types_getActivitiesContents = [ DictType, ( ListType, TupleType ),
                       ( IntType, LongType ), ( IntType, LongType ) ]
  def export_getActivitiesContents( self, selDict, sortList, start, limit ):
    """
    Retrieve the contents of the activity db
    """
    setupCond = {'sources.setup' : self.serviceInfoDict[ 'clientSetup' ] }
    selDict.update( setupCond )
    result = gServiceInterface.getActivitiesContents( selDict, sortList, start, limit )
    if not result[ 'OK' ]:
      return result
    resultTuple = result[ 'Value' ]
    result = { 'Records' : resultTuple[0], 'Fields' : resultTuple[1]}
    result[ 'TotalRecords' ] = gServiceInterface.getNumberOfActivities( setupCond )
    return S_OK( result )

  types_deleteActivity = [ IntType, IntType ]
  def export_deleteActivity( self, sourceId, activityId ):
    """
    Deletes an activity
    """
    return gServiceInterface.deleteActivity( sourceId, activityId )

  types_deleteActivities = [ ListType ]
  def export_deleteActivities( self, deletionList ):
    """
    Deletes a list of activities
    """
    failed = []
    for acList in deletionList:
      retVal = gServiceInterface.deleteActivity( acList[0], acList[1] )
      if not retVal[ 'OK' ]:
        failed.append( retVal[ 'Message' ] )
    if failed:
      return S_ERROR( "\n".join( failed ) )
    return S_OK()

  #Component monitoring functions
  types_getComponentsStatus = [ DictType ]
  def export_getComponentsStatus( self, condDict ):
    if 'Setup' not in condDict:
      condDict[ 'Setup' ] = self.serviceInfoDict[ 'clientSetup' ]
    return gServiceInterface.getComponentsStatus( condDict )

  #Transfer files

  def transfer_toClient( self, fileId, token, fileHelper ):
    retVal = gServiceInterface.getGraphData( fileId )
    if not retVal[ 'OK' ]:
      return retVal
    retVal = fileHelper.sendData( retVal[ 'Value' ] )
    if not retVal[ 'OK' ]:
      return retVal
    fileHelper.sendEOF()
    return S_OK()
