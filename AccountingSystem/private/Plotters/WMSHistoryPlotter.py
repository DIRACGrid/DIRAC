
from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.AccountingSystem.Client.Types.WMSHistory import WMSHistory
from DIRAC.AccountingSystem.private.Plotters.BaseReporter import BaseReporter
from DIRAC.Core.Utilities import Time

class WMSHistoryPlotter(BaseReporter):

  _typeName = "WMSHistory"
  _typeKeyFields = [ dF[0] for dF in WMSHistory().definitionKeyFields ]

  def _reportNumberOfJobs( self, reportRequest ):
    selectFields = ( self._getSQLStringForGrouping( reportRequest[ 'groupingFields' ] ) + ", %s, %s, SUM(%s/%s)",
                     reportRequest[ 'groupingFields' ] + [ 'startTime', 'bucketLength',
                                    'Jobs', 'entriesInBucket'
                                   ]
                   )
    retVal = self._getTypeData( reportRequest[ 'startTime' ],
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
                 'skipEdgeColor' : True,
                 'ylabel' : "jobs"  }
    return self._generateTimedStackedBarPlot( filename, plotInfo[ 'data' ], metadata )

  def _reportNumberOfReschedules( self, reportRequest ):
    selectFields = ( self._getSQLStringForGrouping( reportRequest[ 'groupingFields' ]) + ", %s, %s, SUM(%s/%s)",
                     reportRequest[ 'groupingFields' ] + [ 'startTime', 'bucketLength',
                                    'Reschedules', 'entriesInBucket'
                                   ]
                   )
    retVal = self._getTypeData( reportRequest[ 'startTime' ],
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

  def _plotNumberOfReschedules( self, reportRequest, plotInfo, filename ):
    metadata = { 'title' : 'Reschedules by %s' % " -> ".join( reportRequest[ 'groupingFields' ] ) ,
                 'starttime' : reportRequest[ 'startTime' ],
                 'endtime' : reportRequest[ 'endTime' ],
                 'span' : plotInfo[ 'granularity' ],
                 'skipEdgeColor' : True,
                 'ylabel' : "reschedules"  }
    return self._generateTimedStackedBarPlot( filename, plotInfo[ 'data' ], metadata )