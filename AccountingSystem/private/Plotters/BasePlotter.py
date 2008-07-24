
from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.AccountingSystem.private.DBUtils import DBUtils
from DIRAC.AccountingSystem.private.PlotsCache import gPlotsCache
from DIRAC.Core.Utilities import Time
from DIRAC.AccountingSystem.private.Plots import *

class BasePlotter(DBUtils):

  requiredParams = ()
  _PARAM_CHECK_FOR_NONE = 'checkNone'
  _PARAM_CONVERT_TO_GRANULARITY = 'convertToGranularity'
  _VALID_PARAM_CONVERT_TO_GRANULARITY = ( 'sum', 'average' )

  _EA_THUMBNAIL = 'thumbnail'
  _EA_WIDTH = 'width'
  _EA_HEIGHT = 'height'
  _EA_PADDING = 'figure_padding'

  def __init__( self, db, setup, extraArgs = {} ):
    DBUtils.__init__( self, db, setup )
    self._extraArgs = extraArgs

  def _translateGrouping( self, grouping ):
    return [ grouping ]

  def generate( self, plotName, startTime, endTime, argsDict, grouping ):
    missing = []
    for param in self.requiredParams:
      if param not in argsDict:
        missing.append( param )
    if missing:
      return S_ERROR( "Argument(s) %s missing" % ", ".join( missing ) )
    funcName = "_plot%s" % plotName
    try:
      funcObj = getattr( self, funcName )
    except Exception, e:
      return S_ERROR( "Plot  %s is not defined" % plotName )
    return gPlotsCache.generatePlot( plotName,
                                     startTime,
                                     endTime,
                                     argsDict,
                                     self._translateGrouping( grouping ),
                                     funcObj )

  def plotsList( self ):
    viewList = []
    for attr in dir( self ):
      if attr.find( "_plot" ) == 0:
        viewList.append( attr.replace( "_plot", "" ) )
    viewList.sort()
    return viewList


  def _getTypeData( self, startTime, endTime, selectFields, preCondDict, groupingFields, metadataDict ):
    condDict = {}
    #Check params
    if not self._PARAM_CHECK_FOR_NONE in metadataDict:
      metadataDict[ self._PARAM_CHECK_FOR_NONE ] = False
    if not self._PARAM_CONVERT_TO_GRANULARITY in metadataDict:
      metadataDict[ self._PARAM_CONVERT_TO_GRANULARITY ] = "sum"
    elif metadataDict[ self._PARAM_CONVERT_TO_GRANULARITY ] not in self._VALID_PARAM_CONVERT_TO_GRANULARITY:
      return S_ERROR( "%s field metadata is invalid" % self._PARAM_CONVERT_TO_GRANULARITY )
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
    return S_OK( ( dataDict, coarsestGranularity ) )


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
    if self._EA_THUMBNAIL in self._extraArgs and self._extraArgs[ self._EA_THUMBNAIL ]:
      metadata[ 'legend' ] = False
      #metadata[ 'watermark' ] = False
      if not self._EA_HEIGHT in metadata:
        metadata[ self._EA_HEIGHT ] = 200
      if not self._EA_WIDTH in metadata:
        metadata[ self._EA_WIDTH ] = 300
      if not self._EA_PADDING in metadata:
        metadata[ self._EA_PADDING ] = 20
      for key in ( 'title', 'ylabel', 'xlabel' ):
        if key in metadata:
          del( metadata[ key ] )

  def _generateTimedStackedBarPlot( self, filename, dataDict, metadata ):
    self.__checkPlotMetadata( metadata )
    return generateTimedStackedBarPlot( filename, dataDict, metadata )

  def _generateQualityPlot( self, filename, dataDict, metadata ):
    self.__checkPlotMetadata( metadata )
    return generateQualityPlot( filename, dataDict, metadata )

  def _generateCumulativePlot( self, filename, dataDict, metadata ):
    self.__checkPlotMetadata( metadata )
    return generateCumulativePlot( filename, dataDict, metadata )