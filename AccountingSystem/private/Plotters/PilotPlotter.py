
from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.AccountingSystem.Client.Types.Pilot import Pilot
from DIRAC.AccountingSystem.private.Plotters.BasePlotter import BasePlotter
from DIRAC.AccountingSystem.private.Plots import *
from DIRAC.Core.Utilities import Time

class PilotPlotter(BasePlotter):

  _typeName = "Pilot"
  _typeKeyFields = [ dF[0] for dF in Pilot().definitionKeyFields ]

  def _plotTotalNumberOfJobs( self, startTime, endTime, condDict, groupingFields, filename ):
    selectFields = ( self._getSQLStringForGrouping( groupingFields) + ", %s, %s, SUM(%s)",
                     groupingFields + [ 'startTime', 'bucketLength',
                                    'Jobs'
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
    return generateCumulativePlot( filename, dataDict, metadata )

  def _plotNumberOfJobs( self, startTime, endTime, condDict, groupingFields, filename ):
    selectFields = ( self._getSQLStringForGrouping( groupingFields) + ", %s, %s, SUM(%s)",
                     groupingFields + [ 'startTime', 'bucketLength',
                                    'Jobs'
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
    return generateTimedStackedBarPlot( filename, dataDict, metadata )

  def _plotTotalNumberOfPilots( self, startTime, endTime, condDict, groupingFields, filename ):
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
    metadata = { 'title' : 'Total Pilots by %s' % " -> ".join( groupingFields ) ,
                 'starttime' : startTime,
                 'endtime' : endTime,
                 'span' : granularity,
                 'ylabel' : "pilots",
                 'is_cumulative' : True }
    return generateCumulativePlot( filename, dataDict, metadata )

  def _plotNumberOfPilots( self, startTime, endTime, condDict, groupingFields, filename ):
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
    metadata = { 'title' : 'Pilots by %s' % " -> ".join( groupingFields ) ,
                 'starttime' : startTime,
                 'endtime' : endTime,
                 'span' : granularity,
                 'ylabel' : "pilots"  }
    return generateTimedStackedBarPlot( filename, dataDict, metadata )