
from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.AccountingSystem.Client.Types.DataOperation import DataOperation
from DIRAC.AccountingSystem.private.Plotters.BaseReporter import BaseReporter
from DIRAC.Core.Utilities import Time

class DataOperationPlotter( BaseReporter ):

  _typeName = "DataOperation"
  _typeKeyFields = [ dF[0] for dF in DataOperation().definitionKeyFields ]

  def _translateGrouping( self, grouping ):
    if grouping == "Channel":
      return ( "%s, %s", [ 'Source', 'Destination' ], "CONCAT( %s, ' -> ', %s )" )
    else:
      return ( "%s", [ grouping ] )

  def _reportSuceededTransfers( self, reportRequest ):
    return self.__reportTransfers( reportRequest, 'Suceeded', ( 'Failed', 0 ) )

  def _reportFailedTransfers( self, reportRequest ):
    return self.__reportTransfers( reportRequest, 'Failed', ( 'Suceeded', 1 ) )

  def __reportTransfers( self, reportRequest, titleType, togetherFieldsToPlot ):
    selectFields = ( self._getSelectStringForGrouping( reportRequest[ 'groupingFields' ] ) + ", %s, %s, SUM(%s), SUM(%s)-SUM(%s)",
                     reportRequest[ 'groupingFields' ][1] + [ 'startTime', 'bucketLength',
                       'TransferOK', 'TransferTotal', 'TransferOK',
                      ]
                   )
    retVal = self._getTimedData( reportRequest[ 'startTime' ],
                                reportRequest[ 'endTime' ],
                                selectFields,
                                reportRequest[ 'condDict' ],
                                reportRequest[ 'groupingFields' ],
                                {} )
    if not retVal[ 'OK' ]:
      return retVal
    dataDict, granularity = retVal[ 'Value' ]
    strippedData = self.stripDataField( dataDict, togetherFieldsToPlot[1] )
    if strippedData:
      dataDict[ togetherFieldsToPlot[0] ] = strippedData[0]
    dataDict = self._fillWithZero( granularity, reportRequest[ 'startTime' ], reportRequest[ 'endTime' ], dataDict )
    return S_OK( { 'data' : dataDict, 'granularity' : granularity } )

  def _plotSuceededTransfers( self, reportRequest, plotInfo, filename ):
    return self.__plotTransfers( reportRequest, plotInfo, filename, 'Suceeded', ( 'Failed', 0 ) )

  def _plotFailedTransfers( self, reportRequest, plotInfo, filename ):
    return self.__plotTransfers( reportRequest, plotInfo, filename, 'Failed', ( 'Suceeded', 1 ) )

  def __plotTransfers( self, reportRequest, plotInfo, filename, titleType, togetherFieldsToPlot ):
    metadata = { 'title' : '%s Transfers by %s' % ( titleType, reportRequest[ 'grouping' ] ),
                 'ylabel' : 'files',
                 'starttime' : reportRequest[ 'startTime' ],
                 'endtime' : reportRequest[ 'endTime' ],
                 'span' : plotInfo[ 'granularity' ] }
    return self._generateTimedStackedBarPlot( filename, plotInfo[ 'data' ], metadata )

  def _reportQuality( self, reportRequest ):
    selectFields = ( self._getSelectStringForGrouping( reportRequest[ 'groupingFields' ] ) + ", %s, %s, SUM(%s), SUM(%s)",
                     reportRequest[ 'groupingFields' ][1] + [ 'startTime', 'bucketLength',
                                    'TransferOK', 'TransferTotal'
                                   ]
                   )
    retVal = self._getTimedData( reportRequest[ 'startTime' ],
                                reportRequest[ 'endTime' ],
                                selectFields,
                                reportRequest[ 'condDict' ],
                                reportRequest[ 'groupingFields' ],
                                { 'checkNone' : True,
                                  'convertToGranularity' : 'sum',
                                  'calculateProportionalGauges' : False,
                                  'consolidationFunction' : self._efficiencyConsolidation } )
    if not retVal[ 'OK' ]:
      return retVal
    dataDict, granularity = retVal[ 'Value' ]
    self.stripDataField( dataDict, 0 )
    if len( dataDict ) > 1:
      #Get the total for the plot
      selectFields = ( "'Total', %s, %s, SUM(%s),SUM(%s)",
                       [ 'startTime', 'bucketLength',
                         'TransferOK', 'TransferTotal'
                       ]
                     )
      retVal = self._getTimedData( reportRequest[ 'startTime' ],
                                  reportRequest[ 'endTime' ],
                                  selectFields,
                                  reportRequest[ 'condDict' ],
                                  reportRequest[ 'groupingFields' ],
                                  { 'checkNone' : True,
                                    'convertToGranularity' : 'sum',
                                    'calculateProportionalGauges' : False,
                                    'consolidationFunction' : self._efficiencyConsolidation } )
      if not retVal[ 'OK' ]:
        return retVal
      totalDict = retVal[ 'Value' ][0]
      self.stripDataField( totalDict, 0 )
      for key in totalDict:
        dataDict[ key ] = totalDict[ key ]
    return S_OK( { 'data' : dataDict, 'granularity' : granularity } )

  def _plotQuality( self, reportRequest, plotInfo, filename ):
    metadata = { 'title' : 'Transfer quality by %s' % reportRequest[ 'grouping' ] ,
                 'starttime' : reportRequest[ 'startTime' ],
                 'endtime' : reportRequest[ 'endTime' ],
                 'span' : plotInfo[ 'granularity' ] }
    return self._generateQualityPlot( filename, plotInfo[ 'data' ], metadata )

  def _reportTransferedData( self, reportRequest ):
    selectFields = ( self._getSelectStringForGrouping( reportRequest[ 'groupingFields' ] ) + ", %s, %s, SUM(%s)/1000000000",
                     reportRequest[ 'groupingFields' ][1] + [ 'startTime', 'bucketLength',
                                    'TransferSize'
                                   ]
                   )
    retVal = self._getTimedData( reportRequest[ 'startTime' ],
                                reportRequest[ 'endTime' ],
                                selectFields,
                                reportRequest[ 'condDict' ],
                                reportRequest[ 'groupingFields' ],
                                {} )
    if not retVal[ 'OK' ]:
      return retVal
    dataDict, granularity = retVal[ 'Value' ]
    self.stripDataField( dataDict, 0 )
    dataDict = self._fillWithZero( granularity, reportRequest[ 'startTime' ], reportRequest[ 'endTime' ], dataDict )
    return S_OK( { 'data' : dataDict, 'granularity' : granularity } )

  def _plotTransferedData( self, reportRequest, plotInfo, filename ):
    metadata = { 'title' : 'Transfered data by %s' % reportRequest[ 'grouping' ] ,
                 'starttime' : reportRequest[ 'startTime' ],
                 'endtime' : reportRequest[ 'endTime' ],
                 'span' : plotInfo[ 'granularity' ],
                 'ylabel' : "Gbyte",
                 'sort_labels' : 'last_value' }
    return self._generateCumulativePlot( filename, plotInfo[ 'data' ], metadata )

  def _reportThroughput( self, reportRequest ):
    selectFields = ( self._getSelectStringForGrouping( reportRequest[ 'groupingFields' ] ) + ", %s, %s, SUM(%s)/1000000",
                     reportRequest[ 'groupingFields' ][1] + [ 'startTime', 'bucketLength',
                       'TransferSize'
                      ]
                   )
    retVal = self._getTimedData( reportRequest[ 'startTime' ],
                                reportRequest[ 'endTime' ],
                                selectFields,
                                reportRequest[ 'condDict' ],
                                reportRequest[ 'groupingFields' ],
                                {} )
    if not retVal[ 'OK' ]:
      return retVal
    dataDict, granularity = retVal[ 'Value' ]
    self.stripDataField( dataDict, 0 )
    dataDict = self._fillWithZero( granularity, reportRequest[ 'startTime' ], reportRequest[ 'endTime' ], dataDict )
    dataDict = self._transformToRate( granularity, dataDict )
    return S_OK( { 'data' : dataDict, 'granularity' : granularity } )

  def _plotThroughput( self, reportRequest, plotInfo, filename ):
    metadata = { 'title' : 'Throughput by %s' % reportRequest[ 'grouping' ] ,
                 'ylabel' : 'Mbyte/s',
                 'starttime' : reportRequest[ 'startTime' ],
                 'endtime' : reportRequest[ 'endTime' ],
                 'span' : plotInfo[ 'granularity' ] }
    return self._generateTimedStackedBarPlot( filename, plotInfo[ 'data' ], metadata )

  def _reportDataTransfered( self, reportRequest ):
    selectFields = ( self._getSelectStringForGrouping( reportRequest[ 'groupingFields' ] ) + ", SUM(%s)",
                     reportRequest[ 'groupingFields' ][1] + [ 'TransferSize'
                                   ]
                   )
    retVal = self._getSummaryData( reportRequest[ 'startTime' ],
                                reportRequest[ 'endTime' ],
                                selectFields,
                                reportRequest[ 'condDict' ],
                                reportRequest[ 'groupingFields' ],
                                {} )
    if not retVal[ 'OK' ]:
      return retVal
    dataDict = retVal[ 'Value' ]
    for key in dataDict:
      dataDict[ key ] = int( dataDict[ key ] )
    return S_OK( { 'data' : dataDict  } )

  def _plotDataTransfered( self, reportRequest, plotInfo, filename ):
    metadata = { 'title' : 'Total data transfered by %s' % reportRequest[ 'grouping' ],
                 'ylabel' : 'bytes'
                }
    return self._generatePiePlot( filename, plotInfo[ 'data'], metadata )

