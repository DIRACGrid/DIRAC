
from DIRAC                                                import S_OK, S_ERROR
from DIRAC.MonitoringSystem.Client.Types.WMSHistory       import WMSHistory
from DIRAC.MonitoringSystem.private.Plotters.BasePlotter  import BasePlotter

class WMSHistoryPlotter( BasePlotter ):

  _typeName = "WMSHistory"
  _typeKeyFields =  WMSHistory().getKeyFields() 

  def _reportNumberOfJobs( self, reportRequest ):
    
    selectFields = ['Jobs']
    retVal = self._getTimedData( reportRequest[ 'startTime' ],
                                reportRequest[ 'endTime' ],
                                selectFields,
                                reportRequest[ 'condDict' ])
    if not retVal[ 'OK' ]:
      return retVal
    dataDict, granularity = retVal[ 'Value' ]    
    return S_OK( { 'data' : dataDict, 'granularity' : granularity } )

  def _plotNumberOfJobs( self, reportRequest, plotInfo, filename ):
    metadata = { 'title' : 'Jobs by %s' % reportRequest[ 'grouping' ] ,
                 'starttime' : reportRequest[ 'startTime' ],
                 'endtime' : reportRequest[ 'endTime' ],
                 'span' : plotInfo[ 'granularity' ],
                 'skipEdgeColor' : True,
                 'ylabel' : "jobs"  }
    #plotInfo[ 'data' ] = self._fillWithZero( plotInfo[ 'granularity' ], reportRequest[ 'startTime' ], reportRequest[ 'endTime' ], plotInfo[ 'data' ] )    
    return self._generateStackedLinePlot( filename, plotInfo[ 'data' ], metadata )


  def _reportNumberOfReschedules( self, reportRequest ):
    selectFields = ['Reschedules']
    retVal = self._getTimedData( reportRequest[ 'startTime' ],
                                reportRequest[ 'endTime' ],
                                selectFields,
                                reportRequest[ 'condDict' ])
    
    if not retVal[ 'OK' ]:
      return retVal
    dataDict, granularity = retVal[ 'Value' ]
    return S_OK( { 'data' : dataDict, 'granularity' : granularity } )

  def _plotNumberOfReschedules( self, reportRequest, plotInfo, filename ):
    metadata = { 'title' : 'Reschedules by %s' % reportRequest[ 'grouping' ] ,
                 'starttime' : reportRequest[ 'startTime' ],
                 'endtime' : reportRequest[ 'endTime' ],
                 'span' : plotInfo[ 'granularity' ],
                 'skipEdgeColor' : True,
                 'ylabel' : "reschedules"  }
    plotInfo[ 'data' ] = self._fillWithZero( plotInfo[ 'granularity' ], reportRequest[ 'startTime' ], reportRequest[ 'endTime' ], plotInfo[ 'data' ] )
    return self._generateStackedLinePlot( filename, plotInfo[ 'data' ], metadata )

  def _reportAverageNumberOfJobs( self, reportRequest ):
    selectFields = ['Jobs']
                   
    retVal = self._getSummaryData( reportRequest[ 'startTime' ],
                                reportRequest[ 'endTime' ],
                                selectFields,
                                reportRequest[ 'condDict' ],
                                {"metric": "avg"} )
    if not retVal[ 'OK' ]:
      return retVal
    dataDict = retVal[ 'Value' ]
    return S_OK( { 'data' : dataDict  } )

  def _plotAverageNumberOfJobs( self, reportRequest, plotInfo, filename ):
    metadata = { 'title' : 'Average Number of Jobs by %s' % reportRequest[ 'grouping' ],
                 'ylabel' : 'Jobs',
                 'starttime' : reportRequest[ 'startTime' ],
                 'endtime' : reportRequest[ 'endTime' ]
                }
    return self._generatePiePlot( filename, plotInfo[ 'data'], metadata )
