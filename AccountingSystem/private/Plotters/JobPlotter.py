
from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.AccountingSystem.Client.Types.Job import Job
from DIRAC.AccountingSystem.private.Plotters.BaseReporter import BaseReporter
from DIRAC.Core.Utilities import Time

class JobPlotter(BaseReporter):

  _typeName = "Job"
  _typeKeyFields = [ dF[0] for dF in Job().definitionKeyFields ]

  def _reportCPUEfficiency( self, reportRequest ):
    selectFields = ( self._getSQLStringForGrouping( reportRequest[ 'groupingFields' ] ) + ", %s, %s, SUM(%s)/SUM(%s)",
                     reportRequest[ 'groupingFields' ] + [ 'startTime', 'bucketLength',
                                    'CPUTime', 'ExecTime'
                                   ]
                   )

    retVal = self._getTimedData( reportRequest[ 'startTime' ],
                                reportRequest[ 'endTime' ],
                                selectFields,
                                reportRequest[ 'condDict' ],
                                reportRequest[ 'groupingFields' ],
                                { 'checkNone' : True, 'convertToGranularity' : 'average' } )
    if not retVal[ 'OK' ]:
      return retVal
    dataDict, granularity = retVal[ 'Value' ]
    self.stripDataField( dataDict, 0 )
    if len( dataDict ) > 1:
      #Get the total for the plot
      selectFields = ( "'Total', %s, %s, SUM(%s)/SUM(%s)",
                        [ 'startTime', 'bucketLength',
                          'CPUTime', 'ExecTime'
                        ]
                     )

      retVal = self._getTimedData( reportRequest[ 'startTime' ],
                                  reportRequest[ 'endTime' ],
                                  selectFields,
                                  reportRequest[ 'condDict' ],
                                  reportRequest[ 'groupingFields' ],
                                  { 'checkNone' : True, 'convertToGranularity' : 'average' } )
      if not retVal[ 'OK' ]:
        return retVal
      totalDict = retVal[ 'Value' ][0]
      self.stripDataField( totalDict, 0 )
      for key in totalDict:
        dataDict[ key ] = totalDict[ key ]
    return S_OK( { 'data' : dataDict, 'granularity' : granularity } )

  def _plotCPUEfficiency( self, reportRequest, plotInfo, filename ):
    metadata = { 'title' : 'Job CPU efficiency by %s' % " -> ".join( reportRequest[ 'groupingFields' ] ) ,
                 'starttime' : reportRequest[ 'startTime' ],
                 'endtime' : reportRequest[ 'endTime' ],
                 'span' : plotInfo[ 'granularity' ] }
    return self._generateQualityPlot( filename, plotInfo[ 'data' ], metadata )

  def _reportCPUUsed( self, reportRequest ):
    selectFields = ( self._getSQLStringForGrouping( reportRequest[ 'groupingFields' ]) + ", %s, %s, SUM(%s)/86400",
                     reportRequest[ 'groupingFields' ] + [ 'startTime', 'bucketLength',
                                    'CPUTime'
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

  def _plotCPUUsed( self, reportRequest, plotInfo, filename ):
    metadata = { 'title' : 'CPU used by %s' % " -> ".join( reportRequest[ 'groupingFields' ] ) ,
                 'starttime' : reportRequest[ 'startTime' ],
                 'endtime' : reportRequest[ 'endTime' ],
                 'span' : plotInfo[ 'granularity' ],
                 'ylabel' : "days",
                 'is_cumulative' : True }
    return self._generateCumulativePlot( filename, plotInfo[ 'data'], metadata )

  def _reportCPUUsage( self, reportRequest ):
    selectFields = ( self._getSQLStringForGrouping( reportRequest[ 'groupingFields' ]) + ", %s, %s, SUM(%s)/86400",
                     reportRequest[ 'groupingFields' ] + [ 'startTime', 'bucketLength',
                                    'CPUTime'
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

  def _plotCPUUsage( self, reportRequest, plotInfo, filename ):
    metadata = { 'title' : 'CPU usage by %s' % " -> ".join( reportRequest[ 'groupingFields' ] ) ,
                 'starttime' : reportRequest[ 'startTime' ],
                 'endtime' : reportRequest[ 'endTime' ],
                 'span' : plotInfo[ 'granularity' ],
                 'ylabel' : "days" }
    return self._generateTimedStackedBarPlot( filename, plotInfo[ 'data'], metadata )

  def _reportCumulativeNumberOfJobs( self, reportRequest ):
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

  def _plotCumulativeNumberOfJobs( self, reportRequest, plotInfo, filename ):
    metadata = { 'title' : 'Cumulative Jobs by %s' % " -> ".join( reportRequest[ 'groupingFields' ] ) ,
                 'starttime' : reportRequest[ 'startTime' ],
                 'endtime' : reportRequest[ 'endTime' ],
                 'span' : plotInfo[ 'granularity' ],
                 'ylabel' : "jobs",
                 'is_cumulative' : True }
    return self._generateCumulativePlot( filename, plotInfo[ 'data'], metadata )

  def _reportNumberOfJobs( self, reportRequest ):
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

  def _plotNumberOfJobs( self, reportRequest, plotInfo, filename ):
    metadata = { 'title' : 'Jobs by %s' % " -> ".join( reportRequest[ 'groupingFields' ] ) ,
                 'starttime' : reportRequest[ 'startTime' ],
                 'endtime' : reportRequest[ 'endTime' ],
                 'span' : plotInfo[ 'granularity' ],
                 'ylabel' : "jobs"  }
    return self._generateTimedStackedBarPlot( filename, plotInfo[ 'data'], metadata )

  def _reportInputSandboxSize( self, reportRequest ):
    return self.__reportFieldSizeinMB( reportRequest, ( "InputSandBoxSize", "Input sand box size" ) )

  def _reportOutputSandboxSize( self, reportRequest ):
    return self.__reportFieldSizeinMB( reportRequest, ( "OutputSandBoxSize", "Output sand box size" ) )

  def _reportDiskSpaceSize( self, reportRequest ):
    return self.__reportFieldSizeinMB( reportRequest, ( "DiskSpace", "Used disk space" ) )

  def _reportInputDataSize( self, reportRequest ):
    return self.__reportFieldSizeinMB( reportRequest, ( "InputDataSize", "Input data" ) )

  def _reportOutputDataSize( self, reportRequest ):
    return self.__reportFieldSizeinMB( reportRequest, ( "OutputDataSize", "Output data" ) )

  def __reportFieldSizeinMB( self, reportRequest, fieldTuple ):
    selectFields = ( self._getSQLStringForGrouping( reportRequest[ 'groupingFields' ]) + ", %s, %s, SUM(%s)/1000000",
                     reportRequest[ 'groupingFields' ] + [ 'startTime', 'bucketLength', fieldTuple[0] ]
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

  def _plotInputSandboxSize( self, reportRequest, plotInfo, filename ):
    return self.__plotFieldSizeinMB( reportRequest, plotInfo, filename, ( "InputSandBoxSize", "Input sand box size" ) )

  def _plotOutputSandboxSize( self, reportRequest, plotInfo, filename ):
    return self.__plotFieldSizeinMB( reportRequest, plotInfo, filename, ( "OutputSandBoxSize", "Output sand box size" ) )

  def _plotDiskSpaceSize( self, reportRequest, plotInfo, filename ):
    return self.__plotFieldSizeinMB( reportRequest, plotInfo, filename, ( "DiskSpace", "Used disk space" ) )

  def _plotInputDataSize( self, reportRequest, plotInfo, filename ):
    return self.__plotFieldSizeinMB( reportRequest, plotInfo, filename, ( "InputDataSize", "Input data" ) )

  def _plotOutputDataSize( self, reportRequest, plotInfo, filename ):
    return self.__plotFieldSizeinMB( reportRequest, plotInfo, filename, ( "OutputDataSize", "Output data" ) )

  def __plotFieldSizeinMB( self, reportRequest, plotInfo, filename, fieldTuple ):
    metadata = { 'title' : '%s by %s' % ( fieldTuple[1], " -> ".join( reportRequest[ 'groupingFields' ] ) ),
                 'starttime' : reportRequest[ 'startTime' ],
                 'endtime' : reportRequest[ 'endTime' ],
                 'span' : plotInfo[ 'granularity' ],
                 'ylabel' : "MB" }
    return self._generateTimedStackedBarPlot( filename, plotInfo[ 'data'], metadata )

  def _reportInputDataFiles( self, reportRequest ):
    return self.__reportDataFiles( reportRequest, ( "InputDataFiles", "Input files" ) )

  def _reportOuputDataFiles( self, reportRequest ):
    return self.__reportDataFiles( reportRequest, ( "OutputDataFiles", "Output files" ) )

  def __reportDataFiles( self, reportRequest, fieldTuple ):
    selectFields = ( self._getSQLStringForGrouping( reportRequest[ 'groupingFields' ]) + ", %s, %s, SUM(%s)",
                     reportRequest[ 'groupingFields' ] + [ 'startTime', 'bucketLength', fieldTuple[0] ]
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

  def _plotInputDataFiles( self, reportRequest, plotInfo, filename ):
    return self.__plotDataFiles( reportRequest, plotInfo, filename, ( "InputDataFiles", "Input files" ) )

  def _plotOuputDataFiles( self, reportRequest, plotInfo, filename ):
    return self.__plotDataFiles( reportRequest, plotInfo, filename, ( "OutputDataFiles", "Output files" ) )

  def __plotDataFiles( self, reportRequest, plotInfo, filename, fieldTuple ):
    metadata = { 'title' : '%s by %s' % ( fieldTuple[1], " -> ".join( reportRequest[ 'groupingFields' ] ) ),
                 'starttime' : reportRequest[ 'startTime' ],
                 'endtime' : reportRequest[ 'endTime' ],
                 'span' : plotInfo[ 'granularity' ],
                 'ylabel' : "files" }
    return self._generateTimedStackedBarPlot( filename, plotInfo[ 'data'], metadata )

  def _reportTotalCPUUsed( self, reportRequest ):
    selectFields = ( self._getSQLStringForGrouping( reportRequest[ 'groupingFields' ]) + ", SUM(%s)/86400",
                     reportRequest[ 'groupingFields' ] + [ 'CPUTime'
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
    return S_OK( { 'data' : dataDict  } )

  def _plotTotalCPUUsed( self, reportRequest, plotInfo, filename ):
    print plotInfo
    metadata = { 'title' : 'CPU days used by %s' % " -> ".join( reportRequest[ 'groupingFields' ] ) ,
                 'ylabel' : 'cpu days'
                }
    return self._generatePiePlot( filename, plotInfo[ 'data'], metadata )
