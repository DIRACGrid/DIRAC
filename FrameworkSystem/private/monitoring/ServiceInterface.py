# $HeadURL$
__RCSID__ = "$Id$"
import DIRAC
from DIRAC import gLogger, rootPath, gConfig
from DIRAC.FrameworkSystem.private.monitoring.RRDManager import RRDManager
from DIRAC.Core.Utilities.ReturnValues import S_OK, S_ERROR
from DIRAC.Core.Utilities import DEncode, List

class ServiceInterface:

  __sourceToComponentIdMapping = {}

  def __init__( self ):
    self.dataPath = "%s/data/monitoring" % gConfig.getValue( '/LocalSite/InstancePath', rootPath )
    self.plotsPath = "%s/plots" % self.dataPath
    self.rrdPath = "%s/rrd" % self.dataPath
    self.srvUp = False
    self.compmonDB = False

  def __createRRDManager( self ):
    """
    Generate an RRDManager
    """
    return RRDManager( self.rrdPath, self.plotsPath )

  def __createCatalog( self ):
    """
    Creates a Monitoring catalog connector
    """
    from DIRAC.FrameworkSystem.private.monitoring.MonitoringCatalog import MonitoringCatalog
    return MonitoringCatalog( self.dataPath )

  def serviceRunning( self ):
    """
    Returns if monitoring service is running
    """
    return self.srvUp

  def initialize( self, dataPath ):
    """
    Initialize monitoring server
    """
    from DIRAC.FrameworkSystem.private.monitoring.PlotCache import PlotCache
    from DIRAC.FrameworkSystem.DB.ComponentMonitoringDB import ComponentMonitoringDB

    self.dataPath = dataPath
    self.plotCache = PlotCache( RRDManager( self.rrdPath, self.plotsPath ) )
    self.srvUp = True
    try:
      self.compmonDB = ComponentMonitoringDB()
    except Exception, e:
      gLogger.exception( "Cannot initialize component monitoring db" )

  def initializeDB( self ):
    """
    Initializes and creates monitoring catalog db if it doesn't exist
    """
    acCatalog = self.__createCatalog()
    if not acCatalog.createSchema():
      return False
    #Register default view if it's not there
    viewNames = [ str( v[1] ) for v in  acCatalog.getViews( False ) ]
    if 'Dynamic component view' in viewNames:
      return True
    return acCatalog.registerView( "Dynamic component view",
                                   DEncode.encode( { 'variable': ['sources.componentName'],
                                                     'definition': {},
                                                     'stacked': True,
                                                     'groupBy': ['activities.description'],
                                                     'label': '$SITE'} ),
                                   ['sources.componentName'] )

  def __checkSourceDict( self, sourceDict ):
    """
    Check that the dictionary is a valid source one
    """
    validKeys = ( "setup", "site", "componentType", "componentLocation", "componentName" )
    for key in validKeys:
      if key not in sourceDict:
        return False
    return True

  def __checkActivityDict( self, acDict ):
    """
    Check that the dictionary is a valid activity one
    """
    validKeys = ( 'category', 'description', 'bucketLength', 'type', 'unit' )
    for key in validKeys:
      if key not in acDict:
        return False
    return True

  def registerActivities( self, sourceDict, activitiesDict, componentExtraInfo ):
    """
    Register new activities in the database
    """
    acCatalog = self.__createCatalog()
    rrdManager = self.__createRRDManager()
    #Register source
    if not self.__checkSourceDict( sourceDict ):
      return S_ERROR( "Source definition is not valid" )
    sourceId = acCatalog.registerSource( sourceDict )
    #Register activities
    for name in activitiesDict:
      if not self.__checkActivityDict( activitiesDict[ name ] ):
        return S_ERROR( "Definition for activity %s is not valid" % name )
      activitiesDict[ name ][ 'name' ] = name
      if not 'bucketLength' in activitiesDict[ name ]:
        activitiesDict[ name ][ 'bucketLength' ] = 60
      if not self.__checkActivityDict( activitiesDict[ name ] ):
        return S_ERROR( "Activity %s definition is not valid" % name )
      gLogger.info( "Received activity", "%s [%s]" % ( name, str( activitiesDict[ name ] ) ) )
      rrdFile = acCatalog.registerActivity( sourceId, name, activitiesDict[ name ] )
      if not rrdFile:
        return S_ERROR( "Could not register activity %s" % name )
      retVal = rrdManager.create( activitiesDict[ name ][ 'type' ], rrdFile, activitiesDict[ name ][ 'bucketLength' ] )
      if not retVal[ 'OK' ]:
        return retVal
    self.__cmdb_registerComponent( sourceId, sourceDict, componentExtraInfo )
    return S_OK( sourceId )

  def commitMarks( self, sourceId, activitiesDict, componentExtraInfo ):
    """
    Adds marks to activities
    """
    gLogger.info( "Commiting marks", "From %s for %s" % ( sourceId, ", ".join( activitiesDict.keys() ) ) )
    acCatalog = self.__createCatalog()
    rrdManager = self.__createRRDManager()
    unregisteredActivities = []
    for acName in activitiesDict:
      acData = activitiesDict[ acName ]
      acInfo = acCatalog.findActivity( sourceId, acName )
      if not acInfo:
        unregisteredActivities.append( acName )
        gLogger.warn( "Cant find rrd filename", "%s:%s activity" % ( sourceId, acName ) )
        continue
      rrdFile = acInfo[6]
      if not rrdManager.existsRRDFile( rrdFile ):
        gLogger.error( "RRD file does not exist", "%s:%s activity (%s)" % ( sourceId, acName, rrdFile ) )
        unregisteredActivities.append( acName )
        continue
      gLogger.info( "Updating activity", "%s -> %s" % ( acName, rrdFile ) )
      timeList = acData.keys()
      timeList.sort()
      entries = []
      for instant in timeList:
        entries.append( ( instant , acData[ instant ] ) )
      if len( entries ) > 0:
        gLogger.verbose( "There are %s entries for %s" % ( len( entries ), acName ) )
        retDict = rrdManager.update( acInfo[4], rrdFile, acInfo[7], entries, long( acInfo[8] ) )
        if not retDict[ 'OK' ]:
          gLogger.error( "There was an error updating", "%s:%s activity [%s]" % ( sourceId, acName, rrdFile ) )
        else:
          acCatalog.setLastUpdate( sourceId, acName, retDict[ 'Value' ] )
    if not self.__cmdb_heartbeatComponent( sourceId, componentExtraInfo ):
      for acName in activitiesDict:
        if acName not in unregisteredActivities:
          unregisteredActivities.append( acName )
    return S_OK( unregisteredActivities )

  def fieldValue( self, field, definedFields ):
    """
    Return values for a field given a set of defined values for other fields
    """
    retList = self.__createCatalog().queryField( "DISTINCT %s" % field, definedFields )
    return S_OK( retList )

  def __getGroupedPlots( self, viewDescription ):
    """
    Calculate grouped plots for a view
    """
    plotsList = []
    acCatalog = self.__createCatalog()
    groupList = acCatalog.queryField( "DISTINCT %s" % ", ".join( viewDescription[ 'groupBy' ] ), viewDescription[ 'definition' ] )
    for grouping in groupList:
      gLogger.debug( "Grouped plot for combination %s" % str( grouping ) )
      groupDefinitionDict = dict( viewDescription[ 'definition' ] )
      for index in range( len( viewDescription[ 'groupBy' ] ) ):
        groupDefinitionDict[ viewDescription[ 'groupBy' ][index] ] = grouping[ index ]
      activityList = acCatalog.getMatchingActivities( groupDefinitionDict )
      for activity in activityList:
        activity.setGroup( grouping )
        activity.setLabel( viewDescription[ 'label' ] )
      plotsList.append( activityList )
    return plotsList

  def __generateGroupPlots( self, fromSecs, toSecs, viewDescription, size ):
    """
    Generate grouped plots for a view
    """
    plotList = self.__getGroupedPlots( viewDescription )
    filesList = []
    for groupPlot in plotList:
      retVal = self.plotCache.groupPlot( fromSecs, toSecs, groupPlot, viewDescription[ 'stacked' ], size )
      if not retVal[ 'OK' ]:
        gLogger.error( "There was a problem ploting", retVal[ 'Message' ] )
        return retVal
      graphFile = retVal[ 'Value' ]
      gLogger.verbose( "Generated graph", "file %s for group %s" % ( graphFile, str( groupPlot[0] ) ) )
      filesList.append( graphFile )
    return S_OK( filesList )

  def __getPlots( self, viewDescription ):
    """
    Calculate plots for a view
    """
    acCatalog = self.__createCatalog()
    return acCatalog.getMatchingActivities( viewDescription[ 'definition' ] )

  def __generatePlots( self, fromSecs, toSecs, viewDescription, size ):
    """
    Generate non grouped plots for a view
    """
    acList = self.__getPlots( viewDescription )
    filesList = []
    for activity in acList:
      activity.setLabel( viewDescription[ 'label' ] )
      retVal = self.plotCache.plot( fromSecs, toSecs, activity, viewDescription[ 'stacked' ], size )
      if not retVal[ 'OK' ]:
        gLogger.error( "There was a problem ploting", retVal[ 'Message' ] )
        return retVal
      graphFile = retVal[ 'Value' ]
      gLogger.verbose( "Generated graph", "file %s" % ( graphFile ) )
      filesList.append( graphFile )
    return S_OK( filesList )

  def generatePlots( self, fromSecs, toSecs, viewDescription, size = 1 ):
    """
    Generate plots for a view
    """
    gLogger.info( "Generating plots", str( viewDescription ) )
    if 'stacked' not in viewDescription :
      viewDescription[ 'stacked' ] = False
    if 'label' not in viewDescription :
      viewDescription[ 'label' ] = ""
    if 'groupBy' in viewDescription and len( viewDescription[ 'groupBy' ] ):
      return self.__generateGroupPlots( fromSecs, toSecs, viewDescription, size )
    return self.__generatePlots( fromSecs, toSecs, viewDescription, size )

  def getGraphData( self, filename ):
    """
    Read the contents of a plot file
    """
    try:
      fd = file( "%s/%s" % ( self.plotsPath, filename ) )
    except Exception, e:
      return S_ERROR( e )
    data = fd.read()
    fd.close()
    return S_OK( data )

  def saveView( self, viewName, viewDescription ):
    """
    Save a view in the catalog
    """
    if 'stacked' not in viewDescription :
      viewDescription[ 'stacked' ] = False
    if 'label' not in viewDescription:
      viewDescription[ 'label' ] = ""
    if 'variable' in viewDescription:
      for varField in viewDescription[ 'variable' ]:
        if varField in viewDescription[ 'definition' ]:
          del( viewDescription[ 'definition' ][ varField ] )
    else:
      viewDescription[ 'variable' ] = []
    acCatalog = self.__createCatalog()
    return acCatalog.registerView( viewName, DEncode.encode( viewDescription ), viewDescription[ 'variable' ] )

  def getViews( self, onlyStatic = True ):
    """
    Get all stored views
    """
    viewsList = self.__createCatalog().getViews( onlyStatic )
    return S_OK( viewsList )

  def plotView( self, viewRequest ):
    """
    Generate all plots for a view
    """
    views = self.__createCatalog().getViewById( viewRequest[ 'id' ] )
    if len( views ) == 0:
      return S_ERROR( "View does not exist" )
    viewData = views[0]
    viewDefinition = DEncode.decode( str( viewData[ 0 ] ) )[0]
    neededVarFields = List.fromChar( viewData[1], "," )
    if len( neededVarFields ) > 0:
      if not 'varData' in viewRequest:
        return S_ERROR( "Missing variable fields %s!" % ", ".join( neededVarFields ) )
      missingVarFields = []
      for neededField in neededVarFields:
        if neededField in viewRequest[ 'varData' ]:
          viewDefinition[ 'definition' ][ neededField ] = viewRequest[ 'varData' ][ neededField ]
        else:
          missingVarFields.append( neededField )
      if len( missingVarFields ) > 0:
        return S_ERROR( "Missing required fields %s!" % ", ".join( missingVarFields ) )
    return self.generatePlots( viewRequest[ 'fromSecs' ], viewRequest[ 'toSecs' ], viewDefinition, viewRequest[ 'size' ] )

  def deleteView( self, viewId ):
    """
    Delete a view
    """
    self.__createCatalog().deleteView( viewId )
    return S_OK()

  def getSources( self, dbCond = {}, fields = [] ):
    """
    Get a list of activities
    """
    catalog = self.__createCatalog()
    return catalog.getSources( dbCond, fields )

  def getActivities( self, dbCond = {} ):
    """
    Get a list of activities
    """
    acDict = {}
    catalog = self.__createCatalog()
    for sourceTuple in catalog.getSources( dbCond ):
      activityCond = { 'sourceId' : sourceTuple[0] }
      acDict[ sourceTuple ] = catalog.getActivities( activityCond )
    return acDict

  def getNumberOfActivities( self, dbCond = {} ):
    """
    Get a list of activities
    """
    acDict = {}
    catalog = self.__createCatalog()
    total = 0
    for sourceTuple in catalog.getSources( dbCond ):
      activityCond = { 'sourceId' : sourceTuple[0] }
      total += len( catalog.getActivities( activityCond ) )
    return total

  def getActivitiesContents( self, selDict, sortList, start, limit ):
    """
    DB query
    """
    return self.__createCatalog().activitiesQuery( selDict, sortList, start, limit )

  def deleteActivity( self, sourceId, activityId ):
    """
    Delete a view
    """
    retVal = self.__createCatalog().deleteActivity( sourceId, activityId )
    if not retVal[ 'OK' ]:
      return retVal
    self.__createRRDManager().deleteRRD( retVal[ 'Value' ] )
    return S_OK()

  #ComponentMonitoringDB functions

  def __cmdb__writeComponent( self, sourceId ):
    if sourceId not in ServiceInterface.__sourceToComponentIdMapping:
      if not self.__cmdb__loadComponentFromActivityDB( sourceId ):
        return False
    compDict = ServiceInterface.__sourceToComponentIdMapping[ sourceId ]
    result = self.compmonDB.registerComponent( compDict )
    if not result[ 'OK' ]:
      gLogger.error( "Cannot register component in ComponentMonitoringDB", result[ 'Message' ] )
      return False
    compDict[ 'compId' ] = result[ 'Value' ]
    self.__cmdb__writeHeartbeat( sourceId )
    gLogger.info( "Registered component in component monitoring db" )
    return True

  def __cmdb__merge( self, sourceId, extraDict ):
    """
    Merge the cached dict
    """
    compDict = ServiceInterface.__sourceToComponentIdMapping[ sourceId ]
    for field in self.compmonDB.getOptionalFields():
      if field in extraDict:
        compDict[ field ] = extraDict[ field ]
    ServiceInterface.__sourceToComponentIdMapping[ sourceId ] = compDict

  def __cmdb__loadComponentFromActivityDB( self, sourceId ):
    """
    Load the component dict from the activities it registered
    """
    sources = gServiceInterface.getSources( { 'id' : sourceId },
                                            [ 'componentType', 'componentName', 'componentLocation', 'setup' ] )
    if len ( sources ) == 0:
      return False
    source = [ ts for ts in sources if len( ts ) > 0 ][0]
    compDict = { 'type'          : source[0],
                 'componentName' : source[1],
                 'host'          : source[2],
                 'setup'         : source[3],
                }
    if compDict[ 'type' ] == 'service':
      loc = compDict[ 'host' ]
      loc = loc[ loc.find( "://" ) + 3 : ]
      loc = loc[ : loc.find( "/" ) ]
      compDict[ 'host' ] = loc[ :loc.find( ":" ) ]
      compDict[ 'port' ] = loc[ loc.find( ":" ) + 1: ]
    ServiceInterface.__sourceToComponentIdMapping[ sourceId ] = compDict
    return True

  def __cmdb__writeHeartbeat( self, sourceId ):
    compDict = ServiceInterface.__sourceToComponentIdMapping[ sourceId ]
    result = self.compmonDB.heartbeat( compDict )
    if not result[ 'OK' ]:
      gLogger.error( "Cannot heartbeat component in ComponentMonitoringDB", result[ 'Message' ] )

  def __cmdb_registerComponent( self, sourceId, sourceDict, componentExtraInfo ):
    if sourceDict[ 'componentType' ] not in ( 'service', 'agent' ):
      return
    compDict = { 'componentName' : sourceDict[ 'componentName' ],
                 'setup'         : sourceDict[ 'setup' ],
                 'type'          : sourceDict[ 'componentType' ],
                 'host'      : sourceDict[ 'componentLocation' ]
                }
    if compDict[ 'type' ] == 'service':
      loc = compDict[ 'host' ]
      loc = loc[ loc.find( "://" ) + 3 : ]
      loc = loc[ : loc.find( "/" ) ]
      compDict[ 'host' ] = loc[ :loc.find( ":" ) ]
      compDict[ 'port' ] = loc[ loc.find( ":" ) + 1: ]
    ServiceInterface.__sourceToComponentIdMapping[ sourceId ] = compDict
    self.__cmdb__merge( sourceId, componentExtraInfo )
    self.__cmdb__writeComponent( sourceId )

  def __cmdb_heartbeatComponent( self, sourceId, componentExtraInfo ):
    #Component heartbeat
    if sourceId not in ServiceInterface.__sourceToComponentIdMapping:
      if not self.__cmdb__loadComponentFromActivityDB( sourceId ):
        return False
    if ServiceInterface.__sourceToComponentIdMapping[ sourceId ][ 'type' ] not in ( 'service', 'agent' ):
      return  True
    self.__cmdb__merge( sourceId, componentExtraInfo )
    self.__cmdb__writeHeartbeat( sourceId )
    return True

  def getComponentsStatus( self, condDict = False ):
    if not condDict:
      condDict = {}
    return self.compmonDB.getComponentsStatus( condDict )

gServiceInterface = ServiceInterface()
