"""

It is used to create plots using Elasticsearch

"""

__RCSID__ = "$Id$"

from DIRAC.Core.DISET.RequestHandler              import RequestHandler
from DIRAC                                        import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.MonitoringSystem.DB.MonitoringDB       import MonitoringDB
from DIRAC.Core.Utilities                         import Time
from DIRAC.MonitoringSystem.private.MainReporter  import MainReporter
from DIRAC.Core.Utilities.Plotting                import gMonitoringDataCache
from DIRAC.Core.Utilities.Plotting.FileCoding     import extractRequestFromFileId
from DIRAC.Core.Utilities.Plotting.Plots          import generateErrorMessagePlot

import types
import datetime


class MonitoringHandler( RequestHandler ):
  
  """
  .. class:: MonitoringHandler

  :param dict __reportRequestDict contains the arguments used to create a certain plot
  :param object __db used to retrieve the data from the db.
  
  """
  
  __reportRequestDict = { 'typeName' : types.StringType,
                        'reportName' : types.StringType,
                        'startTime' : Time._allDateTypes,
                        'endTime' : Time._allDateTypes,
                        'condDict' : types.DictType,
                        'grouping' : types.StringType,
                        'extraArgs' : types.DictType
                      }
  
  __db = None
  
  @classmethod
  def initializeHandler( cls, serviceInfo ):
    cls.__db = MonitoringDB()
    return S_OK()
  
   
  types_listUniqueKeyValues = [ types.StringTypes ]
  def export_listUniqueKeyValues( self, typeName ):
    """
    :param str typeName is the monitoring type registered in the Types.
    
    :return: S_OK({key:[]}) or S_ERROR()   The key is element of the __keyFields of the BaseType
    """
    setup = self.serviceInfoDict.get( 'clientSetup', None )
    if not setup:
      return S_ERROR( "FATAL ERROR:  Problem with the service configuration!" )
    #NOTE: we can apply some policies if it will be needed!
    return self.__db.getKeyValues( typeName, setup )
    
  types_listReports = [ types.StringTypes ]
  def export_listReports( self, typeName ):
    """
    :param str typeName monitoring type for example WMSHistory
    
    :return S_OK([]) or S_ERROR() the list of available plots
    """
    
    reporter = MainReporter( self.__db, self.serviceInfoDict[ 'clientSetup' ] )
    return reporter.list( typeName )
  
  def transfer_toClient( self, fileId, token, fileHelper ):
    """
    Get graphs data
    
    :param str fileId encoded plot attributes
    :param object
    :param DIRAC.Core.DISET.private.FileHelper.FileHelper fileHelper
     
    """
    
    # First check if we've got to generate the plot
    if len( fileId ) > 5 and fileId[1] == ':':
      gLogger.info( "Seems the file request is a plot generation request!" )
      try:
        result = self.__generatePlotFromFileId( fileId )
      except Exception as e:
        gLogger.exception( "Exception while generating plot", e )
        result = S_ERROR( "Error while generating plot: %s" % str( e ) )
      if not result[ 'OK' ]:
        self.__sendErrorAsImg( result[ 'Message' ], fileHelper )
        fileHelper.sendEOF()
        return result
      fileId = result[ 'Value' ]
    
    retVal = gMonitoringDataCache.getPlotData( fileId )
    if not retVal[ 'OK' ]:
      self.__sendErrorAsImg( retVal[ 'Message' ], fileHelper )
      return retVal
    retVal = fileHelper.sendData( retVal[ 'Value' ] )
    if not retVal[ 'OK' ]:
      return retVal
    fileHelper.sendEOF()
    return S_OK()
  
  def __generatePlotFromFileId( self, fileId ):
    """
    It create the plots using the encode parameters
    :param str fileId the encoded plot attributes
    :return S_OK or S_ERROR returns the file name 
    """
    
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
  
  def __sendErrorAsImg( self, msgText, fileHelper ):
    """
    In case of an error message a whcite plot is created with the error message.
    """
    
    retVal = generateErrorMessagePlot( msgText )
    
    retVal = fileHelper.sendData( retVal[ 'Value' ] )
    if not retVal[ 'OK' ]:
      return retVal
    fileHelper.sendEOF()
    return S_OK()

  def __checkPlotRequest( self, reportRequest ):
    """
    It check the plot attributes. We have to make sure that all attributes which are needed are provided.
    
    :param dict reportRequest contains the plot attributes.
    
    """
    # If extraArgs is not there add it
    if 'extraArgs' not in reportRequest:
      reportRequest[ 'extraArgs' ] = {}
    if not isinstance( reportRequest[ 'extraArgs' ], self.__reportRequestDict[ 'extraArgs' ] ):
      return S_ERROR( "Extra args has to be of type %s" % self.__reportRequestDict[ 'extraArgs' ] )
    reportRequestExtra = reportRequest[ 'extraArgs' ]
    
    # Check sliding plots
    if 'lastSeconds' in reportRequestExtra:
      try:
        lastSeconds = long( reportRequestExtra[ 'lastSeconds' ] )
      except ValueError:
        gLogger.error( "lastSeconds key must be a number" )
        return S_ERROR( "Value Error" )
      #TODO: Maybe we can have last hour in the monitoring
      if lastSeconds < 3600:
        return S_ERROR( "lastSeconds must be more than 3600" )
      now = Time.dateTime()
      reportRequest[ 'endTime' ] = now
      reportRequest[ 'startTime' ] = now - datetime.timedelta( seconds = lastSeconds )
    else:
      # if end date is not there, just set it to now
      if not reportRequest.get( 'endTime', False ):
        reportRequest[ 'endTime' ] = Time.dateTime()
    # Check keys
    for key in self.__reportRequestDict:
      if not key in reportRequest:
        return S_ERROR( 'Missing mandatory field %s in plot reques' % key )

      if not isinstance( reportRequest[ key ], self.__reportRequestDict[ key ] ):
        return S_ERROR( "Type mismatch for field %s (%s), required one of %s" % ( key,
                                                                                  str( type( reportRequest[ key ] ) ),
                                                                                  str( self.__reportRequestDict[ key ] ) ) )
      if key in ( 'startTime', 'endTime' ):
        reportRequest[ key ] = int( Time.toEpoch( reportRequest[ key ] ) )
    
    return S_OK( reportRequest )

  types_generatePlot = [ types.DictType ]
  def export_generatePlot( self, reportRequest ):
    """
    It crated a plots for a given request
    :param dict reportRequest contains the plot arguments...
    """
    retVal = self.__checkPlotRequest( reportRequest )
    if not retVal[ 'OK' ]:
      return retVal
    reporter = MainReporter( self.__db, self.serviceInfoDict[ 'clientSetup' ] )
    reportRequest[ 'generatePlot' ] = True
    return reporter.generate( reportRequest, self.getRemoteCredentials() )
  
  types_getReport = [ types.DictType ]
  def export_getReport( self, reportRequest ):
    """
    It is used to get the raw data used to create a plot.
    :param str typeName the type of the monitoring
    :param str reportName the name of the plotter used to create the plot for example:  NumberOfJobs
    :param int startTime epoch time, start time of the plot
    :param int endTime epoch time, end time of the plot
    :param dict condDict is the conditions used to gnerate the plot: {'Status':['Running'],'grouping': ['Site'] }
    :param str grouping is the grouping of the data for example: 'Site'
    :paran dict extraArgs epoch time which can be last day, last week, last month
    :rerturn S_OK or S_ERROR
    """
    retVal = self.__checkPlotRequest( reportRequest )
    if not retVal[ 'OK' ]:
      return retVal
    reporter = MainReporter( self.__db, self.serviceInfoDict[ 'clientSetup' ] )
    reportRequest[ 'generatePlot' ] = False
    return reporter.generate( reportRequest, self.getRemoteCredentials() )
