
from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.AccountingSystem.Client.Types.Job import Job
from DIRAC.AccountingSystem.private.Plotters.BaseReporter import BaseReporter
from DIRAC.Core.Utilities import Time

class JobPlotter( BaseReporter ):

  _typeName = "Job"
  _typeKeyFields = [ dF[0] for dF in Job().definitionKeyFields ]

  def _translateGrouping( self, grouping ):
    if grouping == "Country":
      sqlRepr = 'upper( substring( %s, locate( ".", %s, length( %s ) - 4 ) + 1 ) )'
      return ( sqlRepr, [ 'Site', 'Site', 'Site' ], sqlRepr )
    elif grouping == "Grid":
      return ( 'substring_index( %s, ".", 1 )', [ 'Site' ] )
    else:
      return ( "%s", [ grouping ] )

  def _reportCPUEfficiency( self, reportRequest ):
    selectFields = ( self._getSelectStringForGrouping( reportRequest[ 'groupingFields' ] ) + ", %s, %s, SUM(%s), SUM(%s)",
                     reportRequest[ 'groupingFields' ][1] + [ 'startTime', 'bucketLength',
                                    'CPUTime', 'ExecTime'
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
                          'CPUTime', 'ExecTime'
                        ]
                     )

      retVal = self._getTimedData( reportRequest[ 'startTime' ],
                                  reportRequest[ 'endTime' ],
                                  selectFields,
                                  reportRequest[ 'condDict' ],
                                  reportRequest[ 'groupingFields' ],
                                  { 'scheckNone' : True,
                                  'convertToGranularity' : 'sum',
                                  'calculateProportionalGauges' : False,
                                  'consolidationFunction' : self._efficiencyConsolidation  } )
      if not retVal[ 'OK' ]:
        return retVal
      totalDict = retVal[ 'Value' ][0]
      self.stripDataField( totalDict, 0 )
      for key in totalDict:
        dataDict[ key ] = totalDict[ key ]
    return S_OK( { 'data' : dataDict, 'granularity' : granularity } )

  def _plotCPUEfficiency( self, reportRequest, plotInfo, filename ):
    metadata = { 'title' : 'Job CPU efficiency by %s' % reportRequest[ 'grouping' ],
                 'starttime' : reportRequest[ 'startTime' ],
                 'endtime' : reportRequest[ 'endTime' ],
                 'span' : plotInfo[ 'granularity' ] }
    return self._generateQualityPlot( filename, plotInfo[ 'data' ], metadata )

  def _reportCPUUsed( self, reportRequest ):
    selectFields = ( self._getSelectStringForGrouping( reportRequest[ 'groupingFields' ] ) + ", %s, %s, SUM(%s)",
                     reportRequest[ 'groupingFields' ][1] + [ 'startTime', 'bucketLength',
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
    dataDict = self._acumulate( granularity, reportRequest[ 'startTime' ], reportRequest[ 'endTime' ], dataDict )
    dataDict, maxValue, unitName = self._findSuitableUnit( dataDict, self._getAccumulationMaxValue( dataDict ), "time" )
    return S_OK( { 'data' : dataDict, 'granularity' : granularity, 'unit' : unitName } )

  def _plotCPUUsed( self, reportRequest, plotInfo, filename ):
    metadata = { 'title' : 'CPU used by %s' % reportRequest[ 'grouping' ],
                 'starttime' : reportRequest[ 'startTime' ],
                 'endtime' : reportRequest[ 'endTime' ],
                 'span' : plotInfo[ 'granularity' ],
                 'ylabel' : plotInfo[ 'unit' ],
                 'sort_labels' : 'last_value' }
    return self._generateCumulativePlot( filename, plotInfo[ 'data'], metadata )

  def _reportCPUUsage( self, reportRequest ):
    selectFields = ( self._getSelectStringForGrouping( reportRequest[ 'groupingFields' ] ) + ", %s, %s, SUM(%s)",
                     reportRequest[ 'groupingFields' ][1] + [ 'startTime', 'bucketLength',
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
    dataDict, maxValue = self._divideByFactor( dataDict, granularity )
    dataDict, maxValue, unitName = self._findSuitableRateUnit( dataDict, maxValue, "time" )
    dataDict = self._fillWithZero( granularity, reportRequest[ 'startTime' ], reportRequest[ 'endTime' ], dataDict )
    return S_OK( { 'data' : dataDict, 'granularity' : granularity, 'unit' : unitName } )

  def _plotCPUUsage( self, reportRequest, plotInfo, filename ):
    metadata = { 'title' : 'CPU usage by %s' % reportRequest[ 'grouping' ],
                 'starttime' : reportRequest[ 'startTime' ],
                 'endtime' : reportRequest[ 'endTime' ],
                 'span' : plotInfo[ 'granularity' ],
                 'ylabel' : plotInfo[ 'unit' ] }
    return self._generateStackedLinePlot( filename, plotInfo[ 'data'], metadata )

  #WallTime
  def _reportWallTime( self, reportRequest ):
    selectFields = ( self._getSelectStringForGrouping( reportRequest[ 'groupingFields' ] ) + ", %s, %s, SUM(%s)",
                     reportRequest[ 'groupingFields' ][1] + [ 'startTime', 'bucketLength',
                                    'ExecTime'
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
    dataDict, maxValue = self._divideByFactor( dataDict, granularity )
    dataDict, maxValue, unitName = self._findSuitableRateUnit( dataDict, maxValue, "time" )
    dataDict = self._fillWithZero( granularity, reportRequest[ 'startTime' ], reportRequest[ 'endTime' ], dataDict )
    return S_OK( { 'data' : dataDict, 'granularity' : granularity, 'unit' : unitName } )

  def _plotWallTime( self, reportRequest, plotInfo, filename ):
    metadata = { 'title' : 'Wall Time by %s' % reportRequest[ 'grouping' ],
                 'starttime' : reportRequest[ 'startTime' ],
                 'endtime' : reportRequest[ 'endTime' ],
                 'span' : plotInfo[ 'granularity' ],
                 'ylabel' : plotInfo[ 'unit' ] }
    return self._generateTimedStackedBarPlot( filename, plotInfo[ 'data'], metadata )

  def _reportAccumulatedWallTime( self, reportRequest ):
    selectFields = ( self._getSelectStringForGrouping( reportRequest[ 'groupingFields' ] ) + ", %s, %s, SUM(%s)",
                     reportRequest[ 'groupingFields' ][1] + [ 'startTime', 'bucketLength',
                                    'ExecTime'
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
    dataDict = self._acumulate( granularity, reportRequest[ 'startTime' ], reportRequest[ 'endTime' ], dataDict )
    dataDict, maxValue, unitName = self._findSuitableUnit( dataDict, self._getAccumulationMaxValue( dataDict ), "time" )
    return S_OK( { 'data' : dataDict, 'granularity' : granularity, 'unit' : unitName } )


  def _plotAccumulatedWallTime( self, reportRequest, plotInfo, filename ):
    metadata = { 'title' : 'Accumulated Wall Time by %s' % reportRequest[ 'grouping' ],
                 'starttime' : reportRequest[ 'startTime' ],
                 'endtime' : reportRequest[ 'endTime' ],
                 'span' : plotInfo[ 'granularity' ],
                 'ylabel' : plotInfo[ 'unit' ],
                 'sort_labels' : 'last_value' }
    return self._generateCumulativePlot( filename, plotInfo[ 'data'], metadata )

  def _reportTotalWallTime( self, reportRequest ):
    selectFields = ( self._getSelectStringForGrouping( reportRequest[ 'groupingFields' ] ) + ", SUM(%s)/86400",
                     reportRequest[ 'groupingFields' ][1] + [ 'ExecTime'
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

  def _plotTotalWallTime( self, reportRequest, plotInfo, filename ):
    metadata = { 'title' : 'Wall Time used by %s' % reportRequest[ 'grouping' ],
                 'ylabel' : 'CPU days',
                 'starttime' : reportRequest[ 'startTime' ],
                 'endtime' : reportRequest[ 'endTime' ]
                }
    return self._generatePiePlot( filename, plotInfo[ 'data'], metadata )

  def _reportCumulativeNumberOfJobs( self, reportRequest ):
    selectFields = ( self._getSelectStringForGrouping( reportRequest[ 'groupingFields' ] ) + ", %s, %s, SUM(%s)",
                     reportRequest[ 'groupingFields' ][1] + [ 'startTime', 'bucketLength',
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
    dataDict = self._acumulate( granularity, reportRequest[ 'startTime' ], reportRequest[ 'endTime' ], dataDict )
    dataDict, maxValue, unitName = self._findSuitableUnit( dataDict,
                                                           self._getAccumulationMaxValue( dataDict ),
                                                           "jobs" )
    return S_OK( { 'data' : dataDict, 'granularity' : granularity, 'unit' : unitName } )


  def _plotCumulativeNumberOfJobs( self, reportRequest, plotInfo, filename ):
    metadata = { 'title' : 'Cumulative Jobs by %s' % reportRequest[ 'grouping' ],
                 'starttime' : reportRequest[ 'startTime' ],
                 'endtime' : reportRequest[ 'endTime' ],
                 'span' : plotInfo[ 'granularity' ],
                 'ylabel' : plotInfo[ 'unit' ],
                 'sort_labels' : 'last_value' }
    return self._generateCumulativePlot( filename, plotInfo[ 'data'], metadata )

  def _reportNumberOfJobs( self, reportRequest ):
    selectFields = ( self._getSelectStringForGrouping( reportRequest[ 'groupingFields' ] ) + ", %s, %s, SUM(%s)",
                     reportRequest[ 'groupingFields' ][1] + [ 'startTime', 'bucketLength',
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
    dataDict, maxValue = self._divideByFactor( dataDict, granularity )
    dataDict, maxValue, unitName = self._findSuitableRateUnit( dataDict, maxValue, "jobs" )
    dataDict = self._fillWithZero( granularity, reportRequest[ 'startTime' ], reportRequest[ 'endTime' ], dataDict )
    return S_OK( { 'data' : dataDict, 'granularity' : granularity, 'unit' : unitName } )

  def _plotNumberOfJobs( self, reportRequest, plotInfo, filename ):
    metadata = { 'title' : 'Jobs by %s' % reportRequest[ 'grouping' ],
                 'starttime' : reportRequest[ 'startTime' ],
                 'endtime' : reportRequest[ 'endTime' ],
                 'span' : plotInfo[ 'granularity' ],
                 'ylabel' : plotInfo[ 'unit' ]  }
    return self._generateStackedLinePlot( filename, plotInfo[ 'data'], metadata )

  def _reportTotalNumberOfJobs( self, reportRequest ):
    selectFields = ( self._getSelectStringForGrouping( reportRequest[ 'groupingFields' ] ) + ", SUM(%s)",
                     reportRequest[ 'groupingFields' ][1] + [ 'entriesInBucket'
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

  def _plotTotalNumberOfJobs( self, reportRequest, plotInfo, filename ):
    metadata = { 'title' : 'Total Number of Jobs by %s' % reportRequest[ 'grouping' ],
                 'ylabel' : 'Jobs',
                 'starttime' : reportRequest[ 'startTime' ],
                 'endtime' : reportRequest[ 'endTime' ]
                }
    return self._generatePiePlot( filename, plotInfo[ 'data'], metadata )

  def _reportProcessingBandwidth( self, reportRequest ):
    selectFields = ( self._getSelectStringForGrouping( reportRequest[ 'groupingFields' ] ) + ", %s, %s, SUM((%s)/(%s))/SUM(%s)",
                     reportRequest[ 'groupingFields' ][1] + [ 'startTime', 'bucketLength', 'InputDataSize', 'CPUTime', 'entriesInBucket' ]
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
    dataDict, maxValue = self._divideByFactor( dataDict, granularity )
    dataDict, maxValue, unitName = self._findSuitableRateUnit( dataDict, maxValue, "bytes" )
    dataDict = self._fillWithZero( granularity, reportRequest[ 'startTime' ], reportRequest[ 'endTime' ], dataDict )
    return S_OK( { 'data' : dataDict, 'granularity' : granularity, 'unit' : unitName } )

  def _plotProcessingBandwidth( self, reportRequest, plotInfo, filename ):
    metadata = { 'title' : 'Processing Bandwidth by %s' % reportRequest[ 'grouping' ],
                 'starttime' : reportRequest[ 'startTime' ],
                 'endtime' : reportRequest[ 'endTime' ],
                 'span' : plotInfo[ 'granularity' ],
                 'ylabel' : plotInfo[ 'unit' ]  }
    return self._generateStackedLinePlot( filename, plotInfo[ 'data'], metadata )

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
    selectFields = ( self._getSelectStringForGrouping( reportRequest[ 'groupingFields' ] ) + ", %s, %s, SUM(%s)",
                     reportRequest[ 'groupingFields' ][1] + [ 'startTime', 'bucketLength', fieldTuple[0] ]
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
    dataDict, maxValue = self._divideByFactor( dataDict, granularity )
    dataDict, maxValue, unitName = self._findSuitableRateUnit( dataDict, maxValue, "bytes" )
    dataDict = self._fillWithZero( granularity, reportRequest[ 'startTime' ], reportRequest[ 'endTime' ], dataDict )
    return S_OK( { 'data' : dataDict, 'granularity' : granularity, 'unit' : unitName } )

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
    metadata = { 'title' : '%s by %s' % ( fieldTuple[1], reportRequest[ 'grouping' ] ),
                 'starttime' : reportRequest[ 'startTime' ],
                 'endtime' : reportRequest[ 'endTime' ],
                 'span' : plotInfo[ 'granularity' ],
                 'ylabel' : plotInfo[ 'unit' ] }
    return self._generateStackedLinePlot( filename, plotInfo[ 'data'], metadata )

  def _reportInputDataFiles( self, reportRequest ):
    return self.__reportDataFiles( reportRequest, ( "InputDataFiles", "Input files" ) )

  def _reportOuputDataFiles( self, reportRequest ):
    return self.__reportDataFiles( reportRequest, ( "OutputDataFiles", "Output files" ) )

  def __reportDataFiles( self, reportRequest, fieldTuple ):
    selectFields = ( self._getSelectStringForGrouping( reportRequest[ 'groupingFields' ] ) + ", %s, %s, SUM(%s)",
                     reportRequest[ 'groupingFields' ][1] + [ 'startTime', 'bucketLength', fieldTuple[0] ]
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
    dataDict, maxValue = self._divideByFactor( dataDict, granularity )
    dataDict, maxValue, unitName = self._findSuitableRateUnit( dataDict, maxValue, "files" )
    dataDict = self._fillWithZero( granularity, reportRequest[ 'startTime' ], reportRequest[ 'endTime' ], dataDict )
    return S_OK( { 'data' : dataDict, 'granularity' : granularity, 'unit' : unitName } )

  def _plotInputDataFiles( self, reportRequest, plotInfo, filename ):
    return self.__plotDataFiles( reportRequest, plotInfo, filename, ( "InputDataFiles", "Input files" ) )

  def _plotOuputDataFiles( self, reportRequest, plotInfo, filename ):
    return self.__plotDataFiles( reportRequest, plotInfo, filename, ( "OutputDataFiles", "Output files" ) )

  def __plotDataFiles( self, reportRequest, plotInfo, filename, fieldTuple ):
    metadata = { 'title' : '%s by %s' % ( fieldTuple[1], reportRequest[ 'grouping' ] ),
                 'starttime' : reportRequest[ 'startTime' ],
                 'endtime' : reportRequest[ 'endTime' ],
                 'span' : plotInfo[ 'granularity' ],
                 'ylabel' : plotInfo[ 'unit' ] }
    return self._generateStackedLinePlot( filename, plotInfo[ 'data'], metadata )

  def _reportTotalCPUUsed( self, reportRequest ):
    selectFields = ( self._getSelectStringForGrouping( reportRequest[ 'groupingFields' ] ) + ", SUM(%s)/86400",
                     reportRequest[ 'groupingFields' ][1] + [ 'CPUTime'
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
    metadata = { 'title' : 'CPU used by %s' % reportRequest[ 'grouping' ],
                 'ylabel' : 'CPU days',
                 'starttime' : reportRequest[ 'startTime' ],
                 'endtime' : reportRequest[ 'endTime' ]
                }
    return self._generatePiePlot( filename, plotInfo[ 'data'], metadata )
