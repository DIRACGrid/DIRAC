
from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.AccountingSystem.Client.Types.Pilot import Pilot
from DIRAC.AccountingSystem.private.Plotters.BaseReporter import BaseReporter
from DIRAC.Core.Utilities import Time

class PilotPlotter(BaseReporter):

  _typeName = "Pilot"
  _typeKeyFields = [ dF[0] for dF in Pilot().definitionKeyFields ]

  def _reportCumulativeNumberOfJobs( self, reportRequest ):
    selectFields = ( self._getSQLStringForGrouping( reportRequest[ 'groupingFields' ] ) + ", %s, %s, SUM(%s)",
                     reportRequest[ 'groupingFields' ] + [ 'startTime', 'bucketLength',
                                    'Jobs'
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
    dataDict = self._acumulate( granularity, reportRequest[ 'startTime' ], reportRequest[ 'endTime' ], dataDict )
    return S_OK( { 'data' : dataDict, 'granularity' : granularity } )

  def _plotCumulativeNumberOfJobs( self, reportRequest, plotInfo, filename ):
    metadata = { 'title' : 'Cumulative Jobs by %s' % " -> ".join( reportRequest[ 'groupingFields' ] ) ,
                 'starttime' : reportRequest[ 'startTime' ],
                 'endtime' : reportRequest[ 'endTime' ],
                 'span' : plotInfo[ 'granularity' ],
                 'ylabel' : "jobs",
                 'is_cumulative' : True }
    return self._generateCumulativePlot( filename, plotInfo[ 'data' ], metadata )

  def _reportNumberOfJobs( self, reportRequest ):
    selectFields = ( self._getSQLStringForGrouping( reportRequest[ 'groupingFields' ] ) + ", %s, %s, SUM(%s)",
                     reportRequest[ 'groupingFields' ] + [ 'startTime', 'bucketLength',
                                    'Jobs'
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

  def _plotNumberOfJobs( self, reportRequest, plotInfo, filename ):
    metadata = { 'title' : 'Jobs by %s' % " -> ".join( reportRequest[ 'groupingFields' ] ) ,
                 'starttime' : reportRequest[ 'startTime' ],
                 'endtime' : reportRequest[ 'endTime' ],
                 'span' : plotInfo[ 'granularity' ],
                 'ylabel' : "jobs"  }
    return self._generateTimedStackedBarPlot( filename, plotInfo[ 'data' ], metadata )

  def _reportCumulativeNumberOfPilots( self, reportRequest ):
    selectFields = ( self._getSQLStringForGrouping( reportRequest[ 'groupingFields' ]) + ", %s, %s, SUM(%s)",
                     reportRequest[ 'groupingFields' ] + [ 'startTime', 'bucketLength',
                                    'entriesInBucket'
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
    dataDict = self._acumulate( granularity, reportRequest[ 'startTime' ], reportRequest[ 'endTime' ], dataDict )
    return S_OK( { 'data' : dataDict, 'granularity' : granularity } )

  def _plotCumulativeNumberOfPilots( self, reportRequest, plotInfo, filename ):
    metadata = { 'title' : 'Cumulative Pilots by %s' % " -> ".join( reportRequest[ 'groupingFields' ] ) ,
                 'starttime' : reportRequest[ 'startTime' ],
                 'endtime' : reportRequest[ 'endTime' ],
                 'span' : plotInfo[ 'granularity' ],
                 'ylabel' : "pilots",
                 'is_cumulative' : True }
    return self._generateCumulativePlot( filename, plotInfo[ 'data' ], metadata )

  def _reportNumberOfPilots( self, reportRequest ):
    selectFields = ( self._getSQLStringForGrouping( reportRequest[ 'groupingFields' ]) + ", %s, %s, SUM(%s)",
                     reportRequest[ 'groupingFields' ] + [ 'startTime', 'bucketLength',
                                    'entriesInBucket'
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

  def _plotNumberOfPilots( self, reportRequest, plotInfo, filename ):
    metadata = { 'title' : 'Pilots by %s' % " -> ".join( reportRequest[ 'groupingFields' ] ) ,
                 'starttime' : reportRequest[ 'startTime' ],
                 'endtime' : reportRequest[ 'endTime' ],
                 'span' : plotInfo[ 'granularity' ],
                 'ylabel' : "pilots"  }
    return self._generateTimedStackedBarPlot( filename, plotInfo[ 'data' ], metadata )

  def _reportJobsPerPilot( self, reportRequest ):
    selectFields = ( self._getSQLStringForGrouping( reportRequest[ 'groupingFields' ]) + ", %s, %s, SUM(%s), SUM(%s)",
                     reportRequest[ 'groupingFields' ] + [ 'startTime', 'bucketLength',
                                    'Jobs', 'entriesInBucket'
                                   ]
                   )
    retVal = self._getTimedData( reportRequest[ 'startTime' ],
                                reportRequest[ 'endTime' ],
                                selectFields,
                                reportRequest[ 'condDict' ],
                                reportRequest[ 'groupingFields' ],
                                { 'checkNone' : True,
                                  'convertToGranularity' : 'sum',
                                  'calculateProportionalGauges' : True }  )
    if not retVal[ 'OK' ]:
      return retVal
    dataDict, granularity = retVal[ 'Value' ]
    self.stripDataField( dataDict, 0 )
    dataDict = self._fillWithZero( granularity, reportRequest[ 'startTime' ], reportRequest[ 'endTime' ], dataDict )
    return S_OK( { 'data' : dataDict, 'granularity' : granularity } )

  def _plotJobsPerPilot( self, reportRequest, plotInfo, filename ):
    metadata = { 'title' : 'Jobs per pilot by %s' % " -> ".join( reportRequest[ 'groupingFields' ] ) ,
                 'starttime' : reportRequest[ 'startTime' ],
                 'endtime' : reportRequest[ 'endTime' ],
                 'span' : plotInfo[ 'granularity' ],
                 'ylabel' : "jobs/pilot" }
    return self._generateTimedStackedBarPlot( filename, plotInfo[ 'data' ], metadata )