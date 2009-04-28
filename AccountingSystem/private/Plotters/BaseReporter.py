
import md5
from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.AccountingSystem.private.DBUtils import DBUtils
from DIRAC.AccountingSystem.private.DataCache import gDataCache
from DIRAC.Core.Utilities import Time
from DIRAC.AccountingSystem.private.Plots import *

class BaseReporter(DBUtils):

  _PARAM_CHECK_FOR_NONE = 'checkNone'
  _PARAM_CALCULATE_PROPORTIONAL_GAUGES = 'calculateProportionalGauges'
  _PARAM_CONVERT_TO_GRANULARITY = 'convertToGranularity'
  _VALID_PARAM_CONVERT_TO_GRANULARITY = ( 'sum', 'average' )
  _PARAM_CONSOLIDATION_FUNCTION = "consolidationFunction"

  _EA_THUMBNAIL = 'thumbnail'
  _EA_WIDTH = 'width'
  _EA_HEIGHT = 'height'
  _EA_THB_WIDTH = 'thbWidth'
  _EA_THB_HEIGHT = 'thbHeight'
  _EA_PADDING = 'figurePadding'
  _EA_TITLE = 'plotTitle'

  def __init__( self, db, setup, extraArgs = {} ):
    DBUtils.__init__( self, db, setup )
    self._extraArgs = extraArgs

  def _translateGrouping( self, grouping ):
    return [ grouping ]

  def generate( self, reportRequest ):
    reportRequest[ 'groupingFields' ] = self._translateGrouping( reportRequest[ 'grouping' ] )
    reportHash = reportRequest[ 'hash' ]
    gLogger.info( "Retrieving data for %s:%s" % ( reportRequest[ 'typeName' ], reportRequest[ 'reportName' ] ) )
    retVal = self.__retrieveReportData( reportRequest, reportHash )
    if not retVal[ 'OK' ]:
      return retVal
    if not reportRequest[ 'generatePlot' ]:
      return retVal
    reportData = retVal[ 'Value' ]
    gLogger.info( "Plotting data for %s:%s" % ( reportRequest[ 'typeName' ], reportRequest[ 'reportName' ] ) )
    retVal = self.__generatePlotForReport( reportRequest, reportHash, reportData )
    if not retVal[ 'OK' ]:
      return retVal
    plotDict = retVal[ 'Value' ]
    if 'retrieveReportData' in reportRequest[ 'extraArgs' ] and reportRequest[ 'extraArgs' ][ 'retrieveReportData' ]:
      plotDict[ 'reportData' ] = reportData
    return S_OK( plotDict )

  def plotsList( self ):
    viewList = []
    for attr in dir( self ):
      if attr.find( "_report" ) == 0:
        viewList.append( attr.replace( "_report", "" ) )
    viewList.sort()
    return viewList

  def __retrieveReportData( self, reportRequest, reportHash ):
    funcName = "_report%s" % reportRequest[ 'reportName' ]
    try:
      funcObj = getattr( self, funcName )
    except Exception, e:
      return S_ERROR( "Report %s is not defined" % reportRequest[ 'reportName' ] )
    return gDataCache.getReportData( reportRequest, reportHash, funcObj )

  def __generatePlotForReport( self, reportRequest, reportHash, reportData ):
    funcName = "_plot%s" % reportRequest[ 'reportName' ]
    try:
      funcObj = getattr( self, funcName )
    except Exception, e:
      return S_ERROR( "Plot function for report %s is not defined" % reportRequest[ 'reportName' ] )
    return gDataCache.getReportPlot( reportRequest, reportHash, reportData, funcObj )

###
# Helper functions for reporters
###

  def _getTimedData( self, startTime, endTime, selectFields, preCondDict, groupingFields, metadataDict ):
    condDict = {}
    #Check params
    if not self._PARAM_CHECK_FOR_NONE in metadataDict:
      metadataDict[ self._PARAM_CHECK_FOR_NONE ] = False
    if not self._PARAM_CONVERT_TO_GRANULARITY in metadataDict:
      metadataDict[ self._PARAM_CONVERT_TO_GRANULARITY ] = "sum"
    elif metadataDict[ self._PARAM_CONVERT_TO_GRANULARITY ] not in self._VALID_PARAM_CONVERT_TO_GRANULARITY:
      return S_ERROR( "%s field metadata is invalid" % self._PARAM_CONVERT_TO_GRANULARITY )
    if not self._PARAM_CALCULATE_PROPORTIONAL_GAUGES in metadataDict:
      metadataDict[ self._PARAM_CALCULATE_PROPORTIONAL_GAUGES ] = False
    #Make safe selections
    for keyword in self._typeKeyFields:
      if keyword in preCondDict:
        condDict[ keyword ] = preCondDict[ keyword ]
    #Query!
    retVal = self._retrieveBucketedData( self._typeName,
                                          startTime,
                                          endTime,
                                          selectFields,
                                          condDict,
                                          [ 'startTime' ] + groupingFields,
                                          [ 'startTime' ]
                                          )
    if not retVal[ 'OK' ]:
      return retVal
    dataDict = self._groupByField( 0, retVal[ 'Value' ] )
    coarsestGranularity = self._getBucketLengthForTime( self._typeName, startTime )
    #Transform!
    for keyField in dataDict:
      if metadataDict[ self._PARAM_CHECK_FOR_NONE ]:
        dataDict[ keyField ] = self._convertNoneToZero( dataDict[ keyField ] )
      if metadataDict[ self._PARAM_CONVERT_TO_GRANULARITY ] == "average":
        dataDict[ keyField ] = self._averageToGranularity( coarsestGranularity, dataDict[ keyField ] )
      if metadataDict[ self._PARAM_CONVERT_TO_GRANULARITY ] == "sum":
        dataDict[ keyField ] = self._sumToGranularity( coarsestGranularity, dataDict[ keyField ] )
      if self._PARAM_CONSOLIDATION_FUNCTION in metadataDict:
        dataDict[ keyField ] = self._executeConsolidation( metadataDict[ self._PARAM_CONSOLIDATION_FUNCTION ], dataDict[ keyField ] )
    if metadataDict[ self._PARAM_CALCULATE_PROPORTIONAL_GAUGES ]:
      dataDict  = self._calculateProportionalGauges( dataDict )
    return S_OK( ( dataDict, coarsestGranularity ) )

  def _executeConsolidation( self, functor, dataDict ):
    for timeKey in dataDict:
      dataDict[ timeKey ] = [ functor( *dataDict[ timeKey ] ) ]
    return dataDict

  def _getSummaryData( self, startTime, endTime, selectFields, preCondDict, groupingFields, metadataDict, reduceFunc = False ):
    condDict = {}
    #Make safe selections
    for keyword in self._typeKeyFields:
      if keyword in preCondDict:
        condDict[ keyword ] = preCondDict[ keyword ]
    #Query!
    retVal = self._retrieveBucketedData( self._typeName,
                                          startTime,
                                          endTime,
                                          selectFields,
                                          condDict,
                                          groupingFields,
                                          []
                                          )
    if not retVal[ 'OK' ]:
      return retVal
    dataDict = self._groupByField( 0, retVal[ 'Value' ] )
    for key in dataDict:
      if not reduceFunc:
        dataDict[ key ] = dataDict[ key ][0][0]
      else:
        dataDict[ key ] = reduceFunc( dataDict[ key ] )
    return S_OK( dataDict )

  def _getSQLStringForGrouping( self, groupingFields ):
    if len( groupingFields ) == 1:
      return "%s"
    else:
      return "CONCAT( %s, ' -> ', %s )"

  def __checkPlotMetadata( self, metadata ):
    if self._EA_WIDTH in self._extraArgs and self._extraArgs[ self._EA_WIDTH ]:
      try:
        metadata[ self._EA_WIDTH ] = min( 1600, max( 200, int( self._extraArgs[ self._EA_WIDTH ] ) ) )
      except:
        pass
    if self._EA_HEIGHT in self._extraArgs and self._extraArgs[ self._EA_HEIGHT ]:
      try:
        metadata[ self._EA_HEIGHT ] = min( 1600, max( 200, int( self._extraArgs[ self._EA_HEIGHT ] ) ) )
      except:
        pass
    if self._EA_TITLE in self._extraArgs and self._extraArgs[ self._EA_TITLE ]:
      metadata[ 'title' ] = self._extraArgs[ self._EA_TITLE ]
    print metadata

  def __checkThumbnailMetadata( self, metadata ):
    if self._EA_THUMBNAIL in self._extraArgs and self._extraArgs[ self._EA_THUMBNAIL ]:
      thbMD = dict( metadata )
      thbMD[ 'legend' ] = False
      if self._EA_THB_HEIGHT in self._extraArgs:
        thbMD[ self._EA_HEIGHT ] = self._extraArgs[ self._EA_THB_HEIGHT ]
      else:
        thbMD[ self._EA_HEIGHT ] = 125
      if self._EA_THB_WIDTH in self._extraArgs:
        thbMD[ self._EA_WIDTH ] = self._extraArgs[ self._EA_THB_WIDTH ]
      else:
        thbMD[ self._EA_WIDTH ] = 200
      thbMD[ self._EA_PADDING ] = 20
      for key in ( 'title', 'ylabel', 'xlabel' ):
        if key in thbMD:
          del( thbMD[ key ] )
      return thbMD
    return False

  def __plotData( self, filename, dataDict, metadata, funcToPlot ):
    self.__checkPlotMetadata( metadata )
    plotFileName = "%s.png" % filename
    finalResult = funcToPlot( plotFileName, dataDict, metadata )
    if not finalResult[ 'OK' ]:
      return finalResult
    thbMD = self.__checkThumbnailMetadata( metadata )
    if not thbMD:
      return S_OK( { 'plot' : True, 'thumbnail' : False } )
    thbFilename = "%s.thb.png" % filename
    retVal = funcToPlot( thbFilename, dataDict, thbMD )
    if not retVal[ 'OK' ]:
      return retVal
    return S_OK( { 'plot' : True, 'thumbnail' : True } )

  def _generateTimedStackedBarPlot( self, filename, dataDict, metadata ):
    return self.__plotData( filename, dataDict, metadata, generateTimedStackedBarPlot )

  def _generateQualityPlot( self, filename, dataDict, metadata ):
    return self.__plotData( filename, dataDict, metadata, generateQualityPlot )

  def _generateCumulativePlot( self, filename, dataDict, metadata ):
    return self.__plotData( filename, dataDict, metadata, generateCumulativePlot )

  def _generatePiePlot( self, filename, dataDict, metadata ):
    return self.__plotData( filename, dataDict, metadata, generatePiePlot )
