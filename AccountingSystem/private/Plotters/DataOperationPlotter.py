
from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.AccountingSystem.Client.Types.DataOperation import DataOperation
from DIRAC.AccountingSystem.private.Plotters.BasePlotter import BasePlotter
from DIRAC.Core.Utilities import Time

class DataOperationPlotter(BasePlotter):

  _typeName = "DataOperation"
  _typeKeyFields = [ dF[0] for dF in DataOperation().definitionKeyFields ]

  def _translateGrouping( self, grouping ):
    if grouping == "Channel":
      return [ 'Source', 'Destination' ]
    else:
      return [ grouping ]

  def _plotSuceededTransfers( self, startTime, endTime, condDict, groupingFields, filename ):
    return self._realPlotSuceededFailedTransfers( startTime, endTime, condDict, groupingFields, filename, 'Suceeded', ( 'Failed', 0 ) )

  def _plotFailedTransfers( self, startTime, endTime, condDict, groupingFields, filename ):
    return self._realPlotSuceededFailedTransfers( startTime, endTime, condDict, groupingFields, filename, 'Failed', ( 'Suceeded', 1 ) )

  def _realPlotSuceededFailedTransfers( self, startTime, endTime, condDict, groupingFields, filename, titleType, togetherFieldsToPlot ):
    selectFields = ( self._getSQLStringForGrouping( groupingFields) + ", %s, %s, SUM(%s), SUM(%s)-SUM(%s)",
                     groupingFields + [ 'startTime', 'bucketLength',
                       'TransferOK', 'TransferTotal', 'TransferOK',
                      ]
                   )
    retVal = self._getTypeData( startTime,
                                endTime,
                                selectFields,
                                condDict,
                                groupingFields,
                                {} )
    if not retVal[ 'OK' ]:
      return retVal
    dataDict, granularity = retVal[ 'Value' ]
    strippedData = self.stripDataField( dataDict, togetherFieldsToPlot[1] )
    if strippedData:
      dataDict[ togetherFieldsToPlot[0] ] = strippedData[0]
    dataDict = self._fillWithZero( granularity, startTime, endTime, dataDict )
    gLogger.info( "Generating plot", "%s with granularity of %s" % ( filename, granularity ) )
    metadata = { 'title' : '%s Transfers by %s' % ( titleType, " -> ".join( groupingFields ) ),
                 'ylabel' : 'files',
                 'starttime' : startTime,
                 'endtime' : endTime,
                 'span' : granularity }
    return self._generateTimedStackedBarPlot( filename, dataDict, metadata )

  def _plotQuality( self, startTime, endTime, condDict, groupingFields, filename ):
    selectFields = ( self._getSQLStringForGrouping( groupingFields) + ", %s, %s, SUM(%s)/SUM(%s)",
                     groupingFields + [ 'startTime', 'bucketLength',
                                    'TransferOK', 'TransferTotal'
                                   ]
                   )
    retVal = self._getTypeData( startTime,
                                endTime,
                                selectFields,
                                condDict,
                                groupingFields,
                                { 'checkNone' : True, 'convertToGranularity' : 'average' } )
    if not retVal[ 'OK' ]:
      return retVal
    dataDict, granularity = retVal[ 'Value' ]
    self.stripDataField( dataDict, 0 )
    if len( dataDict ) > 1:
      #Get the total for the plot
      selectFields = ( "'Total', %s, %s, SUM(%s)/SUM(%s)",
                       [ 'startTime', 'bucketLength',
                         'TransferOK', 'TransferTotal'
                       ]
                     )
      retVal = self._getTypeData( startTime,
                                  endTime,
                                  selectFields,
                                  condDict,
                                  groupingFields,
                                  { 'checkNone' : True, 'convertToGranularity' : 'average' } )
      if not retVal[ 'OK' ]:
        return retVal
      totalDict = retVal[ 'Value' ][0]
      self.stripDataField( totalDict, 0 )
      for key in totalDict:
        dataDict[ key ] = totalDict[ key ]
    gLogger.info( "Generating plot", "%s with granularity of %s" % ( filename, granularity ) )
    metadata = { 'title' : 'Transfer quality by %s' % " -> ".join( groupingFields ) ,
                 'starttime' : startTime,
                 'endtime' : endTime,
                 'span' : granularity }
    return self._generateQualityPlot( filename, dataDict, metadata )

  def _plotTransferedData( self, startTime, endTime, condDict, groupingFields, filename ):
    selectFields = ( self._getSQLStringForGrouping( groupingFields) + ", %s, %s, SUM(%s)/1000000000",
                     groupingFields + [ 'startTime', 'bucketLength',
                                    'TransferSize'
                                   ]
                   )
    retVal = self._getTypeData( startTime,
                                endTime,
                                selectFields,
                                condDict,
                                groupingFields,
                                {} )
    if not retVal[ 'OK' ]:
      return retVal
    dataDict, granularity = retVal[ 'Value' ]
    self.stripDataField( dataDict, 0 )
    dataDict = self._acumulate( granularity, startTime, endTime, dataDict )
    gLogger.info( "Generating plot", "%s with granularity of %s" % ( filename, granularity ) )
    metadata = { 'title' : 'Transfered data by %s' % " -> ".join( groupingFields ) ,
                 'starttime' : startTime,
                 'endtime' : endTime,
                 'span' : granularity,
                 'ylabel' : "Gbyte",
                 'is_cumulative' : True }
    return self._generateCumulativePlot( filename, dataDict, metadata )

  def _plotThroughput( self, startTime, endTime, condDict, groupingFields, filename ):
    selectFields = ( self._getSQLStringForGrouping( groupingFields) + ", %s, %s, (SUM(%s)/SUM(%s))/1000000",
                     groupingFields + [ 'startTime', 'bucketLength',
                       'TransferSize', 'bucketLength',
                      ]
                   )
    retVal = self._getTypeData( startTime,
                                endTime,
                                selectFields,
                                condDict,
                                groupingFields,
                                { 'convertToGranularity' : 'average' } )
    if not retVal[ 'OK' ]:
      return retVal
    dataDict, granularity = retVal[ 'Value' ]
    self.stripDataField( dataDict, 0 )
    dataDict = self._fillWithZero( granularity, startTime, endTime, dataDict )
    gLogger.info( "Generating plot", "%s with granularity of %s" % ( filename, granularity ) )
    metadata = { 'title' : 'Throughput by %s' % " -> ".join( groupingFields ) ,
                 'ylabel' : 'Mbyte/s',
                 'starttime' : startTime,
                 'endtime' : endTime,
                 'span' : granularity }
    return self._generateTimedStackedBarPlot( filename, dataDict, metadata )
