
from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.AccountingSystem.Client.Types.Job import Job
from DIRAC.AccountingSystem.private.Plotters.BasePlotter import BasePlotter
from DIRAC.Core.Utilities import Time

class JobPlotter(BasePlotter):

  _typeName = "Job"
  _typeKeyFields = [ dF[0] for dF in Job().definitionKeyFields ]

  def _plotCPUEfficiency( self, startTime, endTime, condDict, groupingFields, filename ):
    selectFields = ( self._getSQLStringForGrouping( groupingFields) + ", %s, %s, SUM(%s)/SUM(%s)",
                     groupingFields + [ 'startTime', 'bucketLength',
                                    'CPUTime', 'ExecTime'
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
                          'CPUTime', 'ExecTime'
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
    metadata = { 'title' : 'Job CPU efficiency by %s' % " -> ".join( groupingFields ) ,
                 'starttime' : startTime,
                 'endtime' : endTime,
                 'span' : granularity }
    return self._generateQualityPlot( filename, dataDict, metadata )

  def _plotCPUUsed( self, startTime, endTime, condDict, groupingFields, filename ):
    selectFields = ( self._getSQLStringForGrouping( groupingFields) + ", %s, %s, SUM(%s)/86400",
                     groupingFields + [ 'startTime', 'bucketLength',
                                    'CPUTime'
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
    metadata = { 'title' : 'CPU used by %s' % " -> ".join( groupingFields ) ,
                 'starttime' : startTime,
                 'endtime' : endTime,
                 'span' : granularity,
                 'ylabel' : "days",
                 'is_cumulative' : True }
    return self._generateCumulativePlot( filename, dataDict, metadata )

  def _plotCPUUsage( self, startTime, endTime, condDict, groupingFields, filename ):
    selectFields = ( self._getSQLStringForGrouping( groupingFields) + ", %s, %s, SUM(%s)/86400",
                     groupingFields + [ 'startTime', 'bucketLength',
                                    'CPUTime'
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
    dataDict = self._fillWithZero( granularity, startTime, endTime, dataDict )
    gLogger.info( "Generating plot", "%s with granularity of %s" % ( filename, granularity ) )
    metadata = { 'title' : 'CPU usage by %s' % " -> ".join( groupingFields ) ,
                 'starttime' : startTime,
                 'endtime' : endTime,
                 'span' : granularity,
                 'ylabel' : "days" }
    return self._generateTimedStackedBarPlot( filename, dataDict, metadata )

  def _plotCumulativeNumberOfJobs( self, startTime, endTime, condDict, groupingFields, filename ):
    selectFields = ( self._getSQLStringForGrouping( groupingFields) + ", %s, %s, SUM(%s)",
                     groupingFields + [ 'startTime', 'bucketLength',
                                    'entriesInBucket'
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
    metadata = { 'title' : 'Total Jobs by %s' % " -> ".join( groupingFields ) ,
                 'starttime' : startTime,
                 'endtime' : endTime,
                 'span' : granularity,
                 'ylabel' : "jobs",
                 'is_cumulative' : True }
    return self._generateCumulativePlot( filename, dataDict, metadata )

  def _plotNumberOfJobs( self, startTime, endTime, condDict, groupingFields, filename ):
    selectFields = ( self._getSQLStringForGrouping( groupingFields) + ", %s, %s, SUM(%s)",
                     groupingFields + [ 'startTime', 'bucketLength',
                                    'entriesInBucket'
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
    dataDict = self._fillWithZero( granularity, startTime, endTime, dataDict )
    gLogger.info( "Generating plot", "%s with granularity of %s" % ( filename, granularity ) )
    metadata = { 'title' : 'Jobs by %s' % " -> ".join( groupingFields ) ,
                 'starttime' : startTime,
                 'endtime' : endTime,
                 'span' : granularity,
                 'ylabel' : "jobs"  }
    return self._generateTimedStackedBarPlot( filename, dataDict, metadata )

  def _plotInputSandboxSize( self, startTime, endTime, condDict, groupingFields, filename ):
    return self.__plotFieldSizeinMB( startTime, endTime,
                                 condDict, groupingFields,
                                 filename, ( "InputSandBoxSize", "Input sand box size" ) )

  def _plotOutputSandboxSize( self, startTime, endTime, condDict, groupingFields, filename ):
    return self.__plotFieldSizeinMB( startTime, endTime,
                                 condDict, groupingFields,
                                 filename, ( "OutputSandBoxSize", "Output sand box size" ) )

  def _plotDiskSpaceSize( self, startTime, endTime, condDict, groupingFields, filename ):
    return self.__plotFieldSizeinMB( startTime, endTime,
                                 condDict, groupingFields,
                                 filename, ( "DiskSpace", "Used disk space" ) )

  def _plotInputDataSize( self, startTime, endTime, condDict, groupingFields, filename ):
    return self.__plotFieldSizeinMB( startTime, endTime,
                                 condDict, groupingFields,
                                 filename, ( "InputDataSize", "Input data" ) )

  def _plotOutputDataSize( self, startTime, endTime, condDict, groupingFields, filename ):
    return self.__plotFieldSizeinMB( startTime, endTime,
                                 condDict, groupingFields,
                                 filename, ( "OutputDataSize", "Output data" ) )

  def __plotFieldSizeinMB( self, startTime, endTime, condDict, groupingFields, filename, fieldTuple ):
    selectFields = ( self._getSQLStringForGrouping( groupingFields) + ", %s, %s, SUM(%s)/1000000",
                     groupingFields + [ 'startTime', 'bucketLength', fieldTuple[0] ]
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
    dataDict = self._fillWithZero( granularity, startTime, endTime, dataDict )
    gLogger.info( "Generating plot", "%s with granularity of %s" % ( filename, granularity ) )
    metadata = { 'title' : '%s by %s' % ( fieldTuple[1], " -> ".join( groupingFields ) ),
                 'starttime' : startTime,
                 'endtime' : endTime,
                 'span' : granularity,
                 'ylabel' : "MB" }
    return self._generateTimedStackedBarPlot( filename, dataDict, metadata )

  def _plotInputDataFiles( self, startTime, endTime, condDict, groupingFields, filename ):
    return self.__plotDataFiles( startTime, endTime,
                                 condDict, groupingFields,
                                 filename, ( "InputDataFiles", "Input files" ) )

  def _plotOuputDataFiles( self, startTime, endTime, condDict, groupingFields, filename ):
    return self.__plotDataFiles( startTime, endTime,
                                 condDict, groupingFields,
                                 filename, ( "OutputDataFiles", "Output files" ) )

  def __plotDataFiles( self, startTime, endTime, condDict, groupingFields, filename, fieldTuple ):
    selectFields = ( self._getSQLStringForGrouping( groupingFields) + ", %s, %s, SUM(%s)",
                     groupingFields + [ 'startTime', 'bucketLength', fieldTuple[0] ]
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
    dataDict = self._fillWithZero( granularity, startTime, endTime, dataDict )
    gLogger.info( "Generating plot", "%s with granularity of %s" % ( filename, granularity ) )
    metadata = { 'title' : '%s by %s' % ( fieldTuple[1], " -> ".join( groupingFields ) ),
                 'starttime' : startTime,
                 'endtime' : endTime,
                 'span' : granularity,
                 'ylabel' : "files" }
    return self._generateTimedStackedBarPlot( filename, dataDict, metadata )
