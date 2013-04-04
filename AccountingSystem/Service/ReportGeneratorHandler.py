# $HeadURL$
__RCSID__ = "$Id$"
import types
import os
import datetime

from DIRAC import S_OK, S_ERROR, rootPath, gConfig, gLogger, gMonitor
from DIRAC.AccountingSystem.DB.AccountingDB import AccountingDB
from DIRAC.AccountingSystem.private.Summaries import Summaries
from DIRAC.AccountingSystem.private.DataCache import gDataCache
from DIRAC.AccountingSystem.private.MainReporter import MainReporter
from DIRAC.AccountingSystem.private.DBUtils import DBUtils
from DIRAC.AccountingSystem.private.Policies import gPoliciesList
from DIRAC.AccountingSystem.private.Plots import generateErrorMessagePlot
from DIRAC.AccountingSystem.private.FileCoding import extractRequestFromFileId
from DIRAC.ConfigurationSystem.Client import PathFinder
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.Core.Utilities import Time

gAccountingDB = False

def initializeReportGeneratorHandler( serviceInfo ):
  global gAccountingDB
  gAccountingDB = AccountingDB( readOnly = True )
  #Get data location
  reportSection = PathFinder.getServiceSection( "Accounting/ReportGenerator" )
  dataPath = gConfig.getValue( "%s/DataLocation" % reportSection, "data/accountingGraphs" )
  dataPath = dataPath.strip()
  if "/" != dataPath[0]:
    dataPath = os.path.realpath( "%s/%s" % ( gConfig.getValue( '/LocalSite/InstancePath', rootPath ), dataPath ) )
  gLogger.info( "Data will be written into %s" % dataPath )
  try:
    os.makedirs( dataPath )
  except:
    pass
  try:
    testFile = "%s/acc.jarl.test" % dataPath
    fd = file( testFile, "w" )
    fd.close()
    os.unlink( testFile )
  except IOError:
    gLogger.fatal( "Can't write to %s" % dataPath )
    return S_ERROR( "Data location is not writable" )
  gDataCache.setGraphsLocation( dataPath )
  gMonitor.registerActivity( "plotsDrawn", "Drawn plot images", "Accounting reports", "plots", gMonitor.OP_SUM )
  gMonitor.registerActivity( "reportsRequested", "Generated reports", "Accounting reports", "reports", gMonitor.OP_SUM )
  return S_OK()

class ReportGeneratorHandler( RequestHandler ):

  __reportRequestDict = { 'typeName' : types.StringType,
                        'reportName' : types.StringType,
                        'startTime' : Time._allDateTypes,
                        'endTime' : Time._allDateTypes,
                        'condDict' : types.DictType,
                        'grouping' : types.StringType,
                        'extraArgs' : types.DictType
                      }



  def __checkPlotRequest( self, reportRequest ):
    #If extraArgs is not there add it
    if 'extraArgs' not in reportRequest:
      reportRequest[ 'extraArgs' ] = {}
    if type( reportRequest[ 'extraArgs' ] ) != self.__reportRequestDict[ 'extraArgs' ]:
      return S_ERROR( "Extra args has to be of type %s" % self.__reportRequestDict[ 'extraArgs' ] )
    reportRequestExtra = reportRequest[ 'extraArgs' ]
    #Check sliding plots
    if 'lastSeconds' in reportRequestExtra:
      try:
        lastSeconds = long( reportRequestExtra[ 'lastSeconds' ] )
      except:
        return S_ERROR( "lastSeconds key must be a number" )
      if lastSeconds < 3600:
        return S_ERROR( "lastSeconds must be more than 3600" )
      now = Time.dateTime()
      reportRequest[ 'endTime' ] = now
      reportRequest[ 'startTime' ] = now - datetime.timedelta( seconds = lastSeconds )
    else:
      #if enddate is not there, just set it to now
      if not reportRequest.get( 'endTime', False ):
        reportRequest[ 'endTime' ] = Time.dateTime()
    #Check keys
    for key in self.__reportRequestDict:
      if not key in reportRequest:
        return S_ERROR( 'Missing mandatory field %s in plot reques' % key )
      requestKeyType = type( reportRequest[ key ] )
      if key in ( 'startTime', 'endTime' ):
        if requestKeyType not in self.__reportRequestDict[ key ]:
          return S_ERROR( "Type mismatch for field %s (%s), required one of %s" % ( key,
                                                                                    str( requestKeyType ),
                                                                                    str( self.__reportRequestDict[ key ] ) ) )
        reportRequest[ key ] = int( Time.toEpoch( reportRequest[ key ] ) )
      else:
        if requestKeyType != self.__reportRequestDict[ key ]:
          return S_ERROR( "Type mismatch for field %s (%s), required %s" % ( key,
                                                                             str( requestKeyType ),
                                                                             str( self.__reportRequestDict[ key ] ) ) )
    return S_OK( reportRequest )

  types_generatePlot = [ types.DictType ]
  def export_generatePlot( self, reportRequest ):
    """
    Plot a accounting
    Arguments:
      - viewName : Name of view (easy!)
      - startTime
      - endTime
      - argsDict : Arguments to the view.
      - grouping
      - extraArgs
    """
    retVal = self.__checkPlotRequest( reportRequest )
    if not retVal[ 'OK' ]:
      return retVal
    reporter = MainReporter( gAccountingDB, self.serviceInfoDict[ 'clientSetup' ] )
    gMonitor.addMark( "plotsDrawn" )
    reportRequest[ 'generatePlot' ] = True
    return reporter.generate( reportRequest, self.getRemoteCredentials() )

  types_getReport = [ types.DictType ]
  def export_getReport( self, reportRequest ):
    """
    Plot a accounting
    Arguments:
      - viewName : Name of view (easy!)
      - startTime
      - endTime
      - argsDict : Arguments to the view.
      - grouping
      - extraArgs
    """
    retVal = self.__checkPlotRequest( reportRequest )
    if not retVal[ 'OK' ]:
      return retVal
    reporter = MainReporter( gAccountingDB, self.serviceInfoDict[ 'clientSetup' ] )
    gMonitor.addMark( "reportsRequested" )
    reportRequest[ 'generatePlot' ] = False
    return reporter.generate( reportRequest, self.getRemoteCredentials() )

  types_listReports = [ types.StringType ]
  def export_listReports( self, typeName ):
    """
    List all available plots
    Arguments:
      - none
    """
    reporter = MainReporter( gAccountingDB, self.serviceInfoDict[ 'clientSetup' ] )
    return reporter.list( typeName )

  types_listUniqueKeyValues = [ types.StringType ]
  def export_listUniqueKeyValues( self, typeName ):
    """
    List all values for all keys in a type
    Arguments:
      - none
    """
    dbUtils = DBUtils( gAccountingDB, self.serviceInfoDict[ 'clientSetup' ] )
    credDict = self.getRemoteCredentials()
    if typeName in gPoliciesList:
      policyFilter = gPoliciesList[ typeName ]
      filterCond = policyFilter.getListingConditions( credDict )
    else:
      policyFilter = gPoliciesList[ 'Null' ]
      filterCond = {}
    retVal = dbUtils.getKeyValues( typeName, filterCond )
    if not policyFilter or not retVal[ 'OK' ]:
      return retVal
    return policyFilter.filterListingValues( credDict, retVal[ 'Value' ] )

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

  def __sendErrorAsImg( self, msgText, fileHelper ):
    retVal = generateErrorMessagePlot( msgText )
    retVal = fileHelper.sendData( retVal[ 'Value' ] )
    if not retVal[ 'OK' ]:
      return retVal
    fileHelper.sendEOF()
    return S_OK()

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
      except Exception, e:
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
