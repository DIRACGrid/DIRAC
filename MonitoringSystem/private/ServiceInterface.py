import DIRAC
from DIRAC import gLogger
from DIRAC.MonitoringSystem.private.ActivitiesCatalog import ActivitiesCatalog
from DIRAC.MonitoringSystem.private.RRDManager import RRDManager
from DIRAC.Core.Utilities.ReturnValues import S_OK, S_ERROR

class ServiceInterface:

  def __init__( self ):
    self.dataPath = "%s/data/monitoring" % DIRAC.rootPath
    self.srvUp = False

  def serviceRunning( self ):
    return self.srvUp

  def initialize( self, dataPath ):
    self.dataPath = dataPath
    self.srvUp = True

  def initializeDB( self ):
    acCatalog = ActivitiesCatalog( self.dataPath )
    return acCatalog.createSchema()

  def __checkActivityDict( self, acDict ):
    validKeys = ( "name", "category", "unit", "type", "description" )
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

  def registerActivities( self, sourceDict, activitiesDict ):
    acCatalog = ActivitiesCatalog( self.dataPath )
    rrdManager = RRDManager( self.dataPath )
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
    acCatalog = ActivitiesCatalog( self.dataPath )
    rrdManager = RRDManager( self.dataPath )
    for acName in activitiesDict:
      acData = activitiesDict[ acName ]
      rrdFile = acCatalog.getFilename( sourceId, acName )
      if not rrdFile:
        gLogger.error( "Cant find rrd filename for %s:%s activity" % ( sourceId, acName ) )
        continue
      timeList = acData.keys()
      timeList.sort()
      entries = []
      for instant in timeList:
        entries.append( ( instant , acData[ instant ] ) )
      if len( entries ) > 0:
        print entries
        if not rrdManager.update( rrdFile, entries ):
          gLogger.error( "There was an error updating %s:%s activity [%s]" % ( sourceId, acName, rrdFile ) )
    return S_OK()


  def getSources( self ):
    acCatalog = ActivitiesCatalog( self.dataPath )



gServiceInterface = ServiceInterface()