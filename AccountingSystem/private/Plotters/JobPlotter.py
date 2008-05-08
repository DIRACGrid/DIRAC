
from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.AccountingSystem.Client.Types.DataOperation import DataOperation
from DIRAC.AccountingSystem.private.Plotters.BasePlotter import BasePlotter
from DIRAC.AccountingSystem.private.Plots import *
from DIRAC.Core.Utilities import Time

class JobPlotter(BasePlotter):

  __typeName = "Job"

  def _plotCPUEfficiency( self, startTime, endTime, argsDict, grouping, filename ):
    return self.__generateCPUEfficiencyJobPlot(startTime, endTime, grouping, argsDict, filename)

  def __generateCPUEfficiencyJobPlot( self, startTime, endTime, keyNameList, argsDict, filename ):
    retVal = self.__getCPUEfficiencyJob( startTime, endTime, keyNameList, argsDict )
    if not retVal[ 'OK' ]:
      return retVal
    dataDict, granularity = retVal[ 'Value' ]
    self.stripDataField( dataDict, 0 )
    gLogger.info( "Generating plot", "%s with granularity of %s" % ( filename, granularity ) )
    metadata = { 'title' : 'Job CPU efficiency by %s' % " -> ".join( keyNameList ) ,
                 'starttime' : startTime,
                 'endtime' : endTime,
                 'span' : granularity }
    return generateQualityPlot( filename, dataDict, metadata )

  def __getCPUEfficiencyJob( self, startTime, endTime, keyNameList, argsDict ):
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
                                    'CPUTime', 'ExecTime'
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
      dataDict[ keyField ] = self._convertNoneToZero( dataDict[ keyField ] )
      dataDict[ keyField ] = self._averageToGranularity( coarsestGranularity, dataDict[ keyField ] )
    return S_OK( ( dataDict, coarsestGranularity ) )

  def _plotCPUUsed( self, startTime, endTime, argsDict, grouping, filename ):
    return self.__generateJobCPUUsedPlot(startTime, endTime, grouping, argsDict, filename)

  def __generateJobCPUUsedPlot( self, startTime, endTime, keyNameList, argsDict, filename ):
    retVal = self.__getJobCPUUsedData( startTime, endTime, keyNameList, argsDict )
    if not retVal[ 'OK' ]:
      return retVal
    dataDict, granularity = retVal[ 'Value' ]
    startTime = startTime - startTime % granularity
    self.stripDataField( dataDict, 0 )
    dataDict = self._fillWithZero( granularity, startTime, endTime, dataDict )
    gLogger.info( "Generating plot", "%s with granularity of %s" % ( filename, granularity ) )
    metadata = { 'title' : 'CPU used by %s' % " -> ".join( keyNameList ) ,
                 'starttime' : startTime,
                 'endtime' : endTime,
                 'span' : granularity,
                 'ylabel' : "secs",
                 'is_cumulative' : False }
    return generateCumulativePlot( filename, dataDict, metadata )

  def __getJobCPUUsedData( self, startTime, endTime, keyNameList, argsDict ):
    condDict = {}
    for keyword in keyNameList:
      if keyword in argsDict:
        condDict[ keyword ] = argsDict[ keyword ]
    if len( keyNameList ) == 1:
      keySQLString = "%s"
    else:
      keySQLString = "CONCAT( %s, ' -> ', %s )"
    selectFields = ( keySQLString + ", %s, %s, %s",
                     keyNameList + [ 'startTime', 'bucketLength',
                                    'CPUTime'
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
      dataDict[ keyField ] = self._sumToGranularity( coarsestGranularity, dataDict[ keyField ] )
    return S_OK( ( dataDict, coarsestGranularity ) )

  def _plotCPUUsage( self, startTime, endTime, argsDict, grouping, filename ):
    return self.__generateCPUUsageJobPlot(startTime, endTime, grouping, argsDict, filename)

  def __generateCPUUsageJobPlot( self, startTime, endTime, keyNameList, argsDict, filename ):
    retVal = self.__getJobCPUUsedData( startTime, endTime, keyNameList, argsDict )
    if not retVal[ 'OK' ]:
      return retVal
    dataDict, granularity = retVal[ 'Value' ]
    self.stripDataField( dataDict, 0 )
    dataDict = self._fillWithZero( granularity, startTime, endTime, dataDict )
    gLogger.info( "Generating plot", "%s with granularity of %s" % ( filename, granularity ) )
    metadata = { 'title' : 'CPU usage by %s' % " -> ".join( keyNameList ) ,
                 'ylabel' : 'secs',
                 'starttime' : startTime,
                 'endtime' : endTime,
                 'span' : granularity }
    return generateTimedStackedBarPlot( filename, dataDict, metadata )


  def _plotNumberOfJobs( self, startTime, endTime, argsDict, grouping, filename ):
    return self.__generateJobNumberJobsPlot(startTime, endTime, grouping, argsDict, filename)

  def __generateJobNumberJobsPlot( self, startTime, endTime, keyNameList, argsDict, filename ):
    retVal = self.__getJobNumJobsData( startTime, endTime, keyNameList, argsDict )
    if not retVal[ 'OK' ]:
      return retVal
    dataDict, granularity = retVal[ 'Value' ]
    startTime = startTime - startTime % granularity
    self.stripDataField( dataDict, 0 )
    dataDict = self._fillWithZero( granularity, startTime, endTime, dataDict )
    gLogger.info( "Generating plot", "%s with granularity of %s" % ( filename, granularity ) )
    metadata = { 'title' : 'Jobs by %s' % " -> ".join( keyNameList ) ,
                 'starttime' : startTime,
                 'endtime' : endTime,
                 'span' : granularity,
                 'ylabel' : "jobs",
                 'is_cumulative' : False }
    return generateCumulativePlot( filename, dataDict, metadata )

  def __getJobNumJobsData( self, startTime, endTime, keyNameList, argsDict ):
    condDict = {}
    for keyword in keyNameList:
      if keyword in argsDict:
        condDict[ keyword ] = argsDict[ keyword ]
    if len( keyNameList ) == 1:
      keySQLString = "%s"
    else:
      keySQLString = "CONCAT( %s, ' -> ', %s )"
    selectFields = ( keySQLString + ", %s, %s, %s",
                     keyNameList + [ 'startTime', 'bucketLength',
                                    'entriesInBucket'
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
      dataDict[ keyField ] = self._sumToGranularity( coarsestGranularity, dataDict[ keyField ] )
    return S_OK( ( dataDict, coarsestGranularity ) )
