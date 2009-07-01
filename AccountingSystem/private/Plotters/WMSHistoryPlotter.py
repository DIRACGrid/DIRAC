
from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.AccountingSystem.Client.Types.WMSHistory import WMSHistory
from DIRAC.AccountingSystem.private.Plotters.BaseReporter import BaseReporter
from DIRAC.Core.Utilities import Time

class WMSHistoryPlotter(BaseReporter):

  _typeName = "WMSHistory"
  _typeKeyFields = [ dF[0] for dF in WMSHistory().definitionKeyFields ]

  def _translateGrouping( self, grouping ):
    if grouping == "Country":
      sqlRepr = 'upper( substring( %s, locate( ".", %s, length( %s ) - 4 ) + 1 ) )'
      return ( sqlRepr, [ 'Site', 'Site', 'Site' ], sqlRepr )
    elif grouping == "Grid":
      return ( 'substring_index( %s, ".", 1 )', [ 'Site' ] )
    else:
      return ( "%s", [ grouping ] )

  def _reportNumberOfJobs( self, reportRequest ):
    selectFields = ( self._getSelectStringForGrouping( reportRequest[ 'groupingFields' ] ) + ", %s, %s, SUM(%s/%s)",
                     reportRequest[ 'groupingFields' ][1] + [ 'startTime', 'bucketLength',
                                    'Jobs', 'entriesInBucket'
                                   ]
                   )
    retVal = self._getTimedData( reportRequest[ 'startTime' ],
                                reportRequest[ 'endTime' ],
                                selectFields,
                                reportRequest[ 'condDict' ],
                                reportRequest[ 'groupingFields' ],
                                { 'convertToGranularity' : 'average', 'checkNone' : True } )
    if not retVal[ 'OK' ]:
      return retVal
    dataDict, granularity = retVal[ 'Value' ]
    self.stripDataField( dataDict, 0 )
    return S_OK( { 'data' : dataDict, 'granularity' : granularity } )

  def _plotNumberOfJobs( self, reportRequest, plotInfo, filename ):
    metadata = { 'title' : 'Jobs by %s' % reportRequest[ 'grouping' ] ,
                 'starttime' : reportRequest[ 'startTime' ],
                 'endtime' : reportRequest[ 'endTime' ],
                 'span' : plotInfo[ 'granularity' ],
                 'skipEdgeColor' : True,
                 'ylabel' : "jobs"  }
    return self._generateTimedStackedBarPlot( filename, plotInfo[ 'data' ], metadata )


  def _reportNumberOfReschedules( self, reportRequest ):
    selectFields = ( self._getSelectStringForGrouping( reportRequest[ 'groupingFields' ] ) + ", %s, %s, SUM(%s/%s)",
                     reportRequest[ 'groupingFields' ][1] + [ 'startTime', 'bucketLength',
                                    'Reschedules', 'entriesInBucket'
                                   ]
                   )
    retVal = self._getTimedData( reportRequest[ 'startTime' ],
                                reportRequest[ 'endTime' ],
                                selectFields,
                                reportRequest[ 'condDict' ],
                                reportRequest[ 'groupingFields' ],
                                { 'convertToGranularity' : 'average', 'checkNone' : True } )
    if not retVal[ 'OK' ]:
      return retVal
    dataDict, granularity = retVal[ 'Value' ]
    self.stripDataField( dataDict, 0 )
    return S_OK( { 'data' : dataDict, 'granularity' : granularity } )

  def _plotNumberOfReschedules( self, reportRequest, plotInfo, filename ):
    metadata = { 'title' : 'Reschedules by %s' % reportRequest[ 'grouping' ] ,
                 'starttime' : reportRequest[ 'startTime' ],
                 'endtime' : reportRequest[ 'endTime' ],
                 'span' : plotInfo[ 'granularity' ],
                 'skipEdgeColor' : True,
                 'ylabel' : "reschedules"  }
    return self._generateTimedStackedBarPlot( filename, plotInfo[ 'data' ], metadata )

  def _reportAverageNumberOfJobs( self, reportRequest ):
    selectFields = ( self._getSelectStringForGrouping( reportRequest[ 'groupingFields' ]) + ", SUM(%s)",
                     reportRequest[ 'groupingFields' ][1] + [ 'Jobs', 'entriesInBucket'
                                   ]
                   )
    retVal = self._getSummaryData( reportRequest[ 'startTime' ],
                                reportRequest[ 'endTime' ],
                                selectFields,
                                reportRequest[ 'condDict' ],
                                reportRequest[ 'groupingFields' ],
                                {},
                                reduceFunc = self._averageConsolidation )
    if not retVal[ 'OK' ]:
      return retVal
    dataDict = retVal[ 'Value' ]
    return S_OK( { 'data' : dataDict  } )

  def _plotAverageNumberOfJobs( self, reportRequest, plotInfo, filename ):
    metadata = { 'title' : 'Average Number of Jobs by %s' % reportRequest[ 'grouping' ],
                 'ylabel' : 'Jobs'
                }
    return self._generatePiePlot( filename, plotInfo[ 'data'], metadata )