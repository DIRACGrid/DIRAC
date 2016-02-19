########################################################################
# $Id: $
########################################################################

"""

It creates the reports using Elasticsearch

"""

__RCSID__ = "$Id$"

from DIRAC.Core.DISET.RequestHandler              import RequestHandler
from DIRAC                                        import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.MonitoringSystem.DB.MonitoringDB       import MonitoringDB
from DIRAC.Core.Utilities                         import Time
from DIRAC.MonitoringSystem.private.MainReporter  import MainReporter
from DIRAC.AccountingSystem.private.DataCache     import gDataCache
from DIRAC.MonitoringSystem.private.FileCoding    import extractRequestFromFileId
import types


class MonitoringHandler( RequestHandler ):
  
  __reportRequestDict = { 'typeName' : types.StringType,
                        'reportName' : types.StringType,
                        'startTime' : Time._allDateTypes,
                        'endTime' : Time._allDateTypes,
                        'condDict' : types.DictType,
                        'grouping' : types.StringType
                      }
  
  __db = None
  
  @classmethod
  def initializeHandler( cls, serviceInfo ):
    cls.__db = MonitoringDB()
    return S_OK()
  
   
  types_listUniqueKeyValues = [ types.StringTypes ]
  def export_listUniqueKeyValues( self, typeName ):
    """
    List all values for all keys in a type
    Arguments:
      - none
    """
    setup = self.serviceInfoDict.get('clientSetup', None)
    if not setup:
      return S_ERROR("FATAL ERROR:  Problem with the service configuration!")
        #we can apply some policies if it will be needed!
    return self.__db.getKeyValues( typeName, setup)
    
  types_listReports = [ types.StringTypes ]
  def export_listReports( self, typeName ):
    """
    List all available plots
    Arguments:
      - none
    """
    reporter = MainReporter( self.__db, self.serviceInfoDict[ 'clientSetup' ] )
    return reporter.list( typeName )
  
  def transfer_toClient( self, fileId, token, fileHelper ):
    """
    Get graphs data
    """
    #First check if we've got to generate the plot
    if len( fileId ) > 5 and fileId[1] == ':':
      gLogger.info( "Seems the file request is a plot generation request!" )
      #Seems a request for a plot!
      try:
        result = self.__generatePlotFromFileId( fileId )
      except Exception as e:
        gLogger.exception( "Exception while generating plot" )
        result = S_ERROR( "Error while generating plot: %s" % str( e ) )
      if not result[ 'OK' ]:
        self.__sendErrorAsImg( result[ 'Message' ], fileHelper )
        fileHelper.sendEOF()
        return result
      fileId = result[ 'Value' ]
    retVal = gDataCache.getPlotData( fileId )
    if not retVal[ 'OK' ]:
      self.__sendErrorAsImg( retVal[ 'Message' ], fileHelper )
      return retVal
    retVal = fileHelper.sendData( retVal[ 'Value' ] )
    if not retVal[ 'OK' ]:
      return retVal
    fileHelper.sendEOF()
    return S_OK()
  
  def __generatePlotFromFileId( self, fileId ):
    result = extractRequestFromFileId( fileId )
    if not result[ 'OK' ]:
      return result
    plotRequest = result[ 'Value' ]
    gLogger.info( "Generating the plots.." )
    result = self.export_generatePlot( plotRequest )
    if not result[ 'OK' ]:
      gLogger.error( "Error while generating the plots", result[ 'Message' ] )
      return result
    fileToReturn = 'plot'
    if 'extraArgs' in plotRequest:
      extraArgs = plotRequest[ 'extraArgs' ]
      if 'thumbnail' in extraArgs and extraArgs[ 'thumbnail' ]:
        fileToReturn = 'thumbnail'
    gLogger.info( "Returning %s file: %s " % ( fileToReturn, result[ 'Value' ][ fileToReturn ] ) )
    return S_OK( result[ 'Value' ][ fileToReturn ] )