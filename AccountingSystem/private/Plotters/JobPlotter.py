
from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.AccountingSystem.Client.Types.Job import Job
from DIRAC.AccountingSystem.private.Plotters.BasePlotter import BasePlotter
from DIRAC.AccountingSystem.private.Plots import *
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
    startTime = startTime - startTime % granularity
    self.stripDataField( dataDict, 0 )
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
    return generateQualityPlot( filename, dataDict, metadata )

  def _plotCPUUsed( self, startTime, endTime, condDict, groupingFields, filename ):
    selectFields = ( self._getSQLStringForGrouping( groupingFields) + ", %s, %s, SUM(%s)",
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
    startTime = startTime - startTime % granularity
    self.stripDataField( dataDict, 0 )
    dataDict = self._fillWithZero( granularity, startTime, endTime, dataDict )
    gLogger.info( "Generating plot", "%s with granularity of %s" % ( filename, granularity ) )
    metadata = { 'title' : 'CPU used by %s' % " -> ".join( groupingFields ) ,
                 'starttime' : startTime,
                 'endtime' : endTime,
                 'span' : granularity,
                 'ylabel' : "secs",
                 'is_cumulative' : False }
    return generateCumulativePlot( filename, dataDict, metadata )

  def _plotCPUUsage( self, startTime, endTime, condDict, groupingFields, filename ):
    selectFields = ( self._getSQLStringForGrouping( groupingFields) + ", %s, %s, SUM(%s)",
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
    startTime = startTime - startTime % granularity
    self.stripDataField( dataDict, 0 )
    dataDict = self._fillWithZero( granularity, startTime, endTime, dataDict )
    gLogger.info( "Generating plot", "%s with granularity of %s" % ( filename, granularity ) )
    metadata = { 'title' : 'CPU usage by %s' % " -> ".join( groupingFields ) ,
                 'starttime' : startTime,
                 'endtime' : endTime,
                 'span' : granularity,
                 'ylabel' : "secs" }
    return generateTimedStackedBarPlot( filename, dataDict, metadata )

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
    startTime = startTime - startTime % granularity
    self.stripDataField( dataDict, 0 )
    dataDict = self._fillWithZero( granularity, startTime, endTime, dataDict )
    gLogger.info( "Generating plot", "%s with granularity of %s" % ( filename, granularity ) )
    metadata = { 'title' : 'Jobs by %s' % " -> ".join( groupingFields ) ,
                 'starttime' : startTime,
                 'endtime' : endTime,
                 'span' : granularity,
                 'ylabel' : "jobs",
                 'is_cumulative' : False }
    return generateCumulativePlot( filename, dataDict, metadata )
