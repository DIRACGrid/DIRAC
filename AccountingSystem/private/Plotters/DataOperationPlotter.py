
from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.AccountingSystem.Client.Types.DataOperation import DataOperation
from DIRAC.AccountingSystem.private.Plotters.BasePlotter import BasePlotter
from DIRAC.AccountingSystem.private.Plots import *
from DIRAC.Core.Utilities import Time

class DataOperationPlotter(BasePlotter):

  __typeName = "DataOperation"

  def _plotSuceededTransfersBySource( self, startTime, endTime, argsDict, filename ):
    return self.__generateDataOperationSuceededTransfersPlot(startTime, endTime, [ 'Source' ], argsDict, filename)

  def _plotSuceededTransfersByDestination( self, startTime, endTime, argsDict, filename ):
    return self.__generateDataOperationSuceededTransfersPlot(startTime, endTime, [ 'Destination' ], argsDict, filename)

  def _plotSuceededTransfersByChannel( self, startTime, endTime, argsDict, filename ):
    return self.__generateDataOperationSuceededTransfersPlot(startTime, endTime, [ 'Source', 'Destination' ], argsDict, filename)

  def __generateDataOperationSuceededTransfersPlot( self, startTime, endTime, keyNameList, argsDict, filename ):
    retVal = self.__getDataOperationTransfers( startTime, endTime, keyNameList, argsDict )
    if not retVal[ 'OK' ]:
      return retVal
    dataDict, granularity = retVal[ 'Value' ]
    strippedData = self.stripDataField( dataDict, 0 )
    if strippedData:
      dataDict[ 'Failed' ] = strippedData[0]
    dataDict = self._fillWithZero( granularity, startTime, endTime, dataDict )
    gLogger.info( "Generating plot", "%s with granularity of %s" % ( filename, granularity ) )
    metadata = { 'title' : 'Suceeded Transfers by %s' % " -> ".join( keyNameList ) ,
                 'ylabel' : 'files',
                 'starttime' : startTime,
                 'endtime' : endTime,
                 'span' : granularity }
    return generateTimedStackedBarPlot( filename, dataDict, metadata )

  def _plotFailedTransfersBySource( self, startTime, endTime, argsDict, filename ):
    return self.__generateDataOperationFailedTransfersPlot(startTime, endTime, [ 'Source' ], argsDict, filename)

  def _plotFailedTransfersByDestination( self, startTime, endTime, argsDict, filename ):
    return self.__generateDataOperationFailedTransfersPlot(startTime, endTime, [ 'Destination' ], argsDict, filename)

  def _plotFailedTransfersByChannel( self, startTime, endTime, argsDict, filename ):
    return self.__generateDataOperationFailedTransfersPlot(startTime, endTime, [ 'Source', 'Destination' ], argsDict, filename)

  def __generateDataOperationFailedTransfersPlot( self, startTime, endTime, keyNameList, argsDict, filename ):
    retVal = self.__getDataOperationTransfers( startTime, endTime, keyNameList, argsDict )
    if not retVal[ 'OK' ]:
      return retVal
    dataDict, granularity = retVal[ 'Value' ]
    strippedData = self.stripDataField( dataDict, 1 )
    if strippedData:
      dataDict[ 'Suceeded' ] = strippedData[0]
    dataDict = self._fillWithZero( granularity, startTime, endTime, dataDict )
    gLogger.info( "Generating plot", "%s with granularity of %s" % ( filename, granularity ) )
    metadata = { 'title' : 'Failed Transfers by %s' % " -> ".join( keyNameList ) ,
                 'ylabel' : 'files',
                 'starttime' : startTime,
                 'endtime' : endTime,
                 'span' : granularity }
    return generateTimedStackedBarPlot( filename, dataDict, metadata )

  def __getDataOperationTransfers( self, startTime, endTime, keyNameList, argsDict ):
    typeName = "DataOperation"
    condDict = {}
    for keyword in keyNameList:
      if keyword in argsDict:
        condDict[ keyword ] = argsDict[ keyword ]
    if len( keyNameList ) == 1:
      keySQLString = "%s"
    else:
      keySQLString = "CONCAT( %s, ' -> ', %s )"
    selectFields = ( keySQLString + ", %s, %s, SUM(%s), SUM(%s)-SUM(%s)",
                     keyNameList + [ 'startTime', 'bucketLength',
                       'TransferOK', 'TransferTotal', 'TransferOK',
                      ]
                   )
    retVal = self._retrieveBucketedData( "DataOperation",
                                          startTime,
                                          endTime,
                                          selectFields,
                                          condDict,
                                          [ 'startTime' ] + keyNameList,
                                          [ 'startTime' ]
                                          )
    if not retVal[ 'OK' ]:
      return retVal
    dataDict = self._groupByField( 0, retVal[ 'Value' ] )
    coarsestGranularity = self._getBucketLengthForTime( typeName, startTime )
    for keyField in dataDict:
      dataDict[ keyField ] = self._sumToGranularity( coarsestGranularity, dataDict[ keyField ] )
    return S_OK( ( dataDict, coarsestGranularity ) )

  def _plotQualityBySource( self, startTime, endTime, argsDict, filename ):
    return self.__generateDataOperationQualityPlot(startTime, endTime, [ 'Source' ], argsDict, filename)

  def _plotQualityByDestination( self, startTime, endTime, argsDict, filename ):
    return self.__generateDataOperationQualityPlot(startTime, endTime, [ 'Destination' ], argsDict, filename)

  def _plotQualityByChannel( self, startTime, endTime, argsDict, filename ):
    return self.__generateDataOperationQualityPlot(startTime, endTime, [ 'Source', 'Destination' ], argsDict, filename)


  def __generateDataOperationQualityPlot( self, startTime, endTime, keyNameList, argsDict, filename ):
    retVal = self.__getDataOperationQuality( startTime, endTime, keyNameList, argsDict )
    if not retVal[ 'OK' ]:
      return retVal
    dataDict, granularity = retVal[ 'Value' ]
    self.stripDataField( dataDict, 0 )
    gLogger.info( "Generating plot", "%s with granularity of %s" % ( filename, granularity ) )
    metadata = { 'title' : 'Transfer quality by %s' % " -> ".join( keyNameList ) ,
                 'starttime' : startTime,
                 'endtime' : endTime,
                 'span' : granularity }
    return generateQualityPlot( filename, dataDict, metadata )

  def __getDataOperationQuality( self, startTime, endTime, keyNameList, argsDict ):
    typeName = "DataOperation"
    condDict = {}
    for keyword in keyNameList:
      if keyword in argsDict:
        condDict[ keyword ] = argsDict[ keyword ]
    if len( keyNameList ) == 1:
      keySQLString = "%s"
    else:
      keySQLString = "CONCAT( %s, ' -> ', %s )"
    selectFields = ( keySQLString + ", %s, %s, SUM(%s)/SUM(%s)",
                     keyNameList + [ 'startTime', 'bucketLength',
                                    'TransferOK', 'TransferTotal'
                                   ]
                   )
    retVal = self._retrieveBucketedData( "DataOperation",
                                          startTime,
                                          endTime,
                                          selectFields,
                                          condDict,
                                          [ 'startTime' ] + keyNameList,
                                          [ 'startTime' ]
                                          )
    if not retVal[ 'OK' ]:
      return retVal
    dataDict = self._groupByField( 0, retVal[ 'Value' ] )
    coarsestGranularity = self._getBucketLengthForTime( typeName, startTime )
    for keyField in dataDict:
      dataDict[ keyField ] = self._convertNoneToZero( dataDict[ keyField ] )
      dataDict[ keyField ] = self._averageToGranularity( coarsestGranularity, dataDict[ keyField ] )
    return S_OK( ( dataDict, coarsestGranularity ) )


  def _plotTransferedDataBySource( self, startTime, endTime, argsDict, filename ):
    return self.__generateDataOperationTransferedDataPlot(startTime, endTime, [ 'Source' ], argsDict, filename)

  def _plotTransferedDataByDestination( self, startTime, endTime, argsDict, filename ):
    return self.__generateDataOperationTransferedDataPlot(startTime, endTime, [ 'Destination' ], argsDict, filename)

  def _plotTransferedDataByChannel( self, startTime, endTime, argsDict, filename ):
    return self.__generateDataOperationTransferedDataPlot(startTime, endTime, [ 'Source', 'Destination' ], argsDict, filename)


  def __generateDataOperationTransferedDataPlot( self, startTime, endTime, keyNameList, argsDict, filename ):
    retVal = self.__getDataOperationTransferedData( startTime, endTime, keyNameList, argsDict )
    if not retVal[ 'OK' ]:
      return retVal
    dataDict, granularity = retVal[ 'Value' ]
    startTime = startTime - startTime % granularity
    self.stripDataField( dataDict, 0 )
    gLogger.info( "Generating plot", "%s with granularity of %s" % ( filename, granularity ) )
    metadata = { 'title' : 'Transfered data by %s' % " -> ".join( keyNameList ) ,
                 'starttime' : startTime,
                 'endtime' : endTime,
                 'span' : granularity,
                 'ylabel' : "Gbyte",
                 'is_cumulative' : False }
    return generateCumulativePlot( filename, dataDict, metadata )

  def __getDataOperationTransferedData( self, startTime, endTime, keyNameList, argsDict ):
    typeName = "DataOperation"
    condDict = {}
    for keyword in keyNameList:
      if keyword in argsDict:
        condDict[ keyword ] = argsDict[ keyword ]
    if len( keyNameList ) == 1:
      keySQLString = "%s"
    else:
      keySQLString = "CONCAT( %s, ' -> ', %s )"
    selectFields = ( keySQLString + ", %s, %s, %s/1000000000",
                     keyNameList + [ 'startTime', 'bucketLength',
                                    'TransferSize'
                                   ]
                   )
    retVal = self._retrieveBucketedData( "DataOperation",
                                          startTime,
                                          endTime,
                                          selectFields,
                                          condDict,
                                          [ 'startTime' ] + keyNameList,
                                          [ 'startTime' ]
                                          )
    if not retVal[ 'OK' ]:
      return retVal
    dataDict = self._groupByField( 0, retVal[ 'Value' ] )
    coarsestGranularity = self._getBucketLengthForTime( typeName, startTime )
    for keyField in dataDict:
      dataDict[ keyField ] = self._sumToGranularity( coarsestGranularity, dataDict[ keyField ] )
    return S_OK( ( dataDict, coarsestGranularity ) )

  def _plotThroughputBySource( self, startTime, endTime, argsDict, filename ):
    return self.__generateDataOperationThroughputPlot(startTime, endTime, [ 'Source' ], argsDict, filename)

  def _plotThroughputByDestination( self, startTime, endTime, argsDict, filename ):
    return self.__generateDataOperationThroughputPlot(startTime, endTime, [ 'Destination' ], argsDict, filename)

  def _plotThroughputByChannel( self, startTime, endTime, argsDict, filename ):
    return self.__generateDataOperationThroughputPlot(startTime, endTime, [ 'Source', 'Destination' ], argsDict, filename)

  def __generateDataOperationThroughputPlot( self, startTime, endTime, keyNameList, argsDict, filename ):
    retVal = self.__getDataOperationThroughput( startTime, endTime, keyNameList, argsDict )
    if not retVal[ 'OK' ]:
      return retVal
    dataDict, granularity = retVal[ 'Value' ]
    self.stripDataField( dataDict, 0 )
    gLogger.info( "Generating plot", "%s with granularity of %s" % ( filename, granularity ) )
    metadata = { 'title' : 'Throughput by %s' % " -> ".join( keyNameList ) ,
                 'ylabel' : 'Mbyte/s',
                 'starttime' : startTime,
                 'endtime' : endTime,
                 'span' : granularity }
    return generateTimedStackedBarPlot( filename, dataDict, metadata )

  def __getDataOperationThroughput( self, startTime, endTime, keyNameList, argsDict ):
    condDict = {}
    for keyword in keyNameList:
      if keyword in argsDict:
        condDict[ keyword ] = argsDict[ keyword ]
    if len( keyNameList ) == 1:
      keySQLString = "%s"
    else:
      keySQLString = "CONCAT( %s, ' -> ', %s )"
    selectFields = ( keySQLString + ", %s, %s, SUM(%s)/SUM(%s)",
                     keyNameList + [ 'startTime', 'bucketLength',
                       'TransferSize', 'bucketLength',
                      ]
                   )
    retVal = self._retrieveBucketedData( self.__typeName,
                                          startTime,
                                          endTime,
                                          selectFields,
                                          condDict,
                                          [ 'startTime' ] + keyNameList,
                                          [ 'startTime' ]
                                          )
    if not retVal[ 'OK' ]:
      return retVal
    dataDict = self._groupByField( 0, retVal[ 'Value' ] )
    coarsestGranularity = self._getBucketLengthForTime( self.__typeName, startTime )
    for keyField in dataDict:
      dataDict[ keyField ] = self._averageToGranularity( coarsestGranularity, dataDict[ keyField ] )
    return S_OK( ( dataDict, coarsestGranularity ) )