# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/MonitoringSystem/private/ServiceInterface.py,v 1.2 2007/12/19 18:04:34 acasajus Exp $
__RCSID__ = "$Id: ServiceInterface.py,v 1.2 2007/12/19 18:04:34 acasajus Exp $"
import DIRAC
from DIRAC import gLogger
from DIRAC.MonitoringSystem.private.MonitoringCatalog import MonitoringCatalog
from DIRAC.MonitoringSystem.private.RRDManager import RRDManager
from DIRAC.Core.Utilities.ReturnValues import S_OK, S_ERROR
from DIRAC.Core.Utilities import DEncode, List

class ServiceInterface:

  def __init__( self ):
    self.dataPath = "%s/data/monitoring" % DIRAC.rootPath
    self.plotsPath = "%s/plots" % self.dataPath
    self.rrdPath = "%s/rrd" % self.dataPath
    self.srvUp = False

  def __createRRDManager(self):
    """
    Generate an RRDManager
    """
    return RRDManager( self.rrdPath, self.plotsPath )

  def __createCatalog( self ):
    """
    Creates a Monitoring catalog connector
    """
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
    from DIRAC.MonitoringSystem.private.PlotCache import PlotCache
    self.dataPath = dataPath
    self.plotCache = PlotCache( RRDManager( self.rrdPath, self.plotsPath ) )
    self.srvUp = True

  def initializeDB( self ):
    """
    Initializes and creates monitoring catalog db if it doesn't exist
    """
    acCatalog = self.__createCatalog()
    return acCatalog.createSchema()

  def __checkActivityDict( self, acDict ):
    """
    Check that the dictionary is a valid activity one
    """
    validKeys = ( "name", "category", "unit", "type", "description" )
    for key in acDict:
      if key not in validKeys:
        return False
    return True

  def __checkSourceDict( self, sourceDict ):
    """
    Check that the dictionary is a valid source one
    """
    validKeys = ( "setup", "site", "componentType", "componentLocation", "componentName" )
    for key in sourceDict:
      if key not in validKeys:
        return False
    return True

  def registerActivities( self, sourceDict, activitiesDict ):
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
      activitiesDict[ name ][ 'name' ] = name
      if not self.__checkActivityDict( activitiesDict[ name ] ):
        return S_ERROR( "Activity %s definition is not valid" % name )
      gLogger.info( "Received activity", "%s [%s]" % ( name, str( activitiesDict[ name ] ) ) )
      rrdFile = acCatalog.registerActivity( sourceId, name, activitiesDict[ name ] )
      if not rrdFile:
        return S_ERROR( "Could not register activity %s" % name )
      rrdManager.create( activitiesDict[ name ][ 'type' ], rrdFile )
    return S_OK( sourceId )

  def commitMarks( self, sourceId, activitiesDict ):
    """
    Adds marks to activities
    """
    gLogger.info( "Commiting marks", "From %s for %s" % ( sourceId, ", ".join( activitiesDict.keys() ) ) )
    acCatalog = self.__createCatalog()
    rrdManager = self.__createRRDManager()
    unregisteredActivities = []
    for acName in activitiesDict:
      gLogger.info( "Updating activity", acName)
      acData = activitiesDict[ acName ]
      rrdFile = acCatalog.getFilename( sourceId, acName )
      if not rrdFile:
        unregisteredActivities.append( acName )
        gLogger.error( "Cant find rrd filename for %s:%s activity" % ( sourceId, acName ) )
        continue
      gLogger.info( "Updating activity", "%s -> %s" % ( acName, rrdFile ) )
      timeList = acData.keys()
      timeList.sort()
      entries = []
      for instant in timeList:
        entries.append( ( instant , acData[ instant ] ) )
      if len( entries ) > 0:
        gLogger.verbose( "There are %s entries for %s" % ( len( entries ), acName ) )
        retDict = rrdManager.update( rrdFile, entries )
        if not retDict[ 'OK' ]:
          gLogger.error( "There was an error updating %s:%s activity [%s]" % ( sourceId, acName, rrdFile ) )
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
    return self.generatePlots( viewRequest[ 'fromSecs' ], viewRequest[ 'toSecs' ], viewDefinition, viewRequest[ 'size' ])

  def deleteView( self, viewId ):
    """
    Delete a view
    """
    self.__createCatalog().deleteView( viewId )
    return S_OK()

gServiceInterface = ServiceInterface()