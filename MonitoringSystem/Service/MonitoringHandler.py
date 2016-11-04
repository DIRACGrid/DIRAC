"""

It is used to create plots using Elasticsearch

"""
import datetime
import os

from DIRAC import gLogger, S_OK, S_ERROR, gConfig
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.Core.Utilities import Time
from DIRAC.Core.Utilities.Plotting import gDataCache
from DIRAC.Core.Utilities.Plotting.FileCoding import extractRequestFromFileId
from DIRAC.Core.Utilities.Plotting.Plots import generateErrorMessagePlot
from DIRAC.Core.Utilities.File import mkDir

from DIRAC.MonitoringSystem.DB.MonitoringDB import MonitoringDB
from DIRAC.MonitoringSystem.private.MainReporter import MainReporter

__RCSID__ = "$Id$"

class MonitoringHandler( RequestHandler ):

  """
  .. class:: MonitoringHandler

  :param dict __reportRequestDict contains the arguments used to create a certain plot
  :param object __db used to retrieve the data from the db.

  """

  __reportRequestDict = {'typeName' : basestring,
                         'reportName' : basestring,
                         'startTime' : Time._allDateTypes,
                         'endTime' : Time._allDateTypes,
                         'condDict' : dict,
                         'grouping' : basestring,
                         'extraArgs' : dict}

  __db = None

  @classmethod
  def initializeHandler( cls, serviceInfo ):
    cls.__db = MonitoringDB()
    reportSection = serviceInfo[ 'serviceSectionPath' ]
    dataPath = gConfig.getValue( "%s/DataLocation" % reportSection, "data/monitoringPlots" )
    gLogger.info( "Data will be written into %s" % dataPath )
    mkDir( dataPath )
    try:
      testFile = "%s/moni.plot.test" % dataPath
      with open( testFile, "w" ) as _fd:
        os.unlink( testFile )
    except IOError as err:
      gLogger.fatal( "Can't write to %s" % dataPath, err )
      return S_ERROR( "Data location is not writable: %s" % repr( err ) )
    gDataCache.setGraphsLocation( dataPath )

    return S_OK()


  types_listUniqueKeyValues = [ basestring ]
  def export_listUniqueKeyValues( self, typeName ):
    """
    :param str typeName is the monitoring type registered in the Types.

    :return: S_OK({key:[]}) or S_ERROR()   The key is element of the __keyFields of the BaseType
    """
    setup = self.serviceInfoDict.get( 'clientSetup', None )
    if not setup:
      return S_ERROR( "FATAL ERROR:  Problem with the service configuration!" )
    # NOTE: we can apply some policies if it will be needed!
    return self.__db.getKeyValues( typeName )

  types_listReports = [ basestring ]
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
      except Exception as e:  # pylint: disable=broad-except
        gLogger.exception( "Exception while generating plot", str( e ) )
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
    if not retVal:
      retVal = fileHelper.sendData( retVal[ 'Message' ] )
    else:
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
      if lastSeconds < 3600:
        return S_ERROR( "lastSeconds must be more than 3600" )
      now = Time.dateTime() #this is an UTC time
      reportRequest[ 'endTime' ] = now
      reportRequest[ 'startTime' ] = now - datetime.timedelta( seconds = lastSeconds )
    else:
      # if end date is not there, just set it to now
      if not reportRequest.get( 'endTime' ):
        # check the existence of the endTime it can be present and empty
        reportRequest[ 'endTime' ] = Time.dateTime()
    # Check keys
    for key in self.__reportRequestDict:
      if not key in reportRequest:
        return S_ERROR( 'Missing mandatory field %s in plot request' % key )

      if not isinstance( reportRequest[ key ], self.__reportRequestDict[ key ] ):
        return S_ERROR( "Type mismatch for field %s (%s), required one of %s" % ( key,
                                                                                  str( type( reportRequest[ key ] ) ),
                                                                                  str( self.__reportRequestDict[ key ] ) ) )
      if key in ( 'startTime', 'endTime' ):
        reportRequest[ key ] = int( Time.toEpoch( reportRequest[ key ] ) )

    return S_OK( reportRequest )

  types_generatePlot = [ dict ]
  def export_generatePlot( self, reportRequest ):
    """
    It creates a plots for a given request
    :param dict reportRequest contains the plot arguments...
    """
    retVal = self.__checkPlotRequest( reportRequest )
    if not retVal[ 'OK' ]:
      return retVal
    reporter = MainReporter( self.__db, self.serviceInfoDict[ 'clientSetup' ] )
    reportRequest[ 'generatePlot' ] = True
    return reporter.generate( reportRequest, self.getRemoteCredentials() )

  types_getReport = [ dict ]
  def export_getReport( self, reportRequest ):
    """
    It is used to get the raw data used to create a plot. The reportRequest has the following parameters:
    :param str typeName the type of the monitoring
    :param str reportName the name of the plotter used to create the plot for example:  NumberOfJobs
    :param int startTime epoch time, start time of the plot
    :param int endTime epoch time, end time of the plot
    :param dict condDict is the conditions used to gnerate the plot: {'Status':['Running'],'grouping': ['Site'] }
    :param str grouping is the grouping of the data for example: 'Site'
    :param dict extraArgs epoch time which can be last day, last week, last month
    :return S_OK or S_ERROR S_OK value is a dictionary which contains all values used to create the plot
    """
    retVal = self.__checkPlotRequest( reportRequest )
    if not retVal[ 'OK' ]:
      return retVal
    reporter = MainReporter( self.__db, self.serviceInfoDict[ 'clientSetup' ] )
    reportRequest[ 'generatePlot' ] = False
    return reporter.generate( reportRequest, self.getRemoteCredentials() )


  types_addMonitoringRecords = [basestring, basestring, list]
  def export_addMonitoringRecords( self, monitoringtype, doc_type, data ):
    """
    It is used to insert data directly to the given monitoring type
    :param str monitoringtype
    :param list data
    """

    retVal = self.__db.getIndexName( monitoringtype )
    if not retVal['OK']:
      return retVal
    prefix = retVal['Value']
    gLogger.debug( "addMonitoringRecords:", prefix )
    return self.__db.bulk_index( prefix, doc_type, data )

  types_addRecords = [basestring, basestring, list]
  def export_addRecords( self, indexname, doc_type, data ):
    """
    It is used to insert data directly to the database... The data will be inserted to the given index.
    :param str indexname
    :param list data
    """
    setup = self.serviceInfoDict.get( 'clientSetup', '' )
    indexname = "%s_%s" % ( setup.lower(), indexname )
    gLogger.debug( "Bulk index:", indexname )
    return self.__db.bulk_index( indexname, doc_type, data )

  types_deleteIndex = [basestring]
  def export_deleteIndex( self, indexName ):
    """
    It is used to delete an index!
    Note this is for experienced users!!!
    :param str indexName
    """
    setup = self.serviceInfoDict.get( 'clientSetup', '' )
    indexName = "%s_%s" % ( setup.lower(), indexName )
    gLogger.debug( "delete index:", indexName )
    return self.__db.deleteIndex( indexName )

  types_getLastDayData = [basestring, dict]
  def export_getLastDayData( self, typeName, condDict ):
    """
    It returns the data from the last day index. Note: we create daily indexes.
    :param str typeName name of the monitoring type
    :param dict condDict -> conditions for the query
                  key -> name of the field
		  value -> list of possible values
    """

    return self.__db.getLastDayData( typeName, condDict )

  types_getLimitedDat = [basestring, dict, int]
  def export_getLimitedData( self, typeName, condDict, size ):
    '''
    Returns a list of records for a given selection.
    :param str typeName name of the monitoring type
    :param dict condDict -> conditions for the query
                  key -> name of the field
                  value -> list of possible values
    :param int size: Indicates how many entries should be retrieved from the log
    :return: Up to size entries for the given component from the database
    '''
    return self.__db.getLimitedData( typeName, condDict, size )

  types_getDataForAGivenPeriod = [basestring, dict, basestring, basestring]
  def export_getDataForAGivenPeriod( self, typeName, condDict, initialDate = '', endDate = '' ):
    """
    Retrieves the history of logging entries for the given component during a given given time period
    :param: str typeName name of the monitoring type
    :param: dict condDict -> conditions for the query
                  key -> name of the field
                  value -> list of possible values
    :param str initialDate: Indicates the start of the time period in the format 'DD/MM/YYYY hh:mm'
    :param str endDate: Indicate the end of the time period in the format 'DD/MM/YYYY hh:mm'
    :return: Entries from the database for the given component recorded between the initial and the end dates

    """
    return self.__db.getDataForAGivenPeriod( typeName, condDict, initialDate, endDate )
  
  types_put = [list, basestring]
  def export_put( self, recordsToInsert, monitoringType ):
    
    """
    It is used to insert records to the db.
    :param list recordsToInsert records to be inserted to the db
    :param basestring monitoringType monitoring type...
    """
    
    return self.__db.put( recordsToInsert, monitoringType )
