
from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.AccountingSystem.Client.Types.WMSHistory import WMSHistory
from DIRAC.AccountingSystem.private.Plotters.BasePlotter import BasePlotter
from DIRAC.AccountingSystem.private.Plots import *
from DIRAC.Core.Utilities import Time

class WMSHistoryPlotter(BasePlotter):

  _typeName = "WMSHistory"
  _typeKeyFields = [ dF[0] for dF in WMSHistory().definitionKeyFields ]

  def _plotNumberOfJobs( self, startTime, endTime, condDict, groupingFields, filename ):
    selectFields = ( self._getSQLStringForGrouping( groupingFields) + ", %s, %s, SUM(%s/%s)",
                     groupingFields + [ 'startTime', 'bucketLength',
                                    'Jobs', 'entriesInBucket'
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
                 'skipEdgeColor' : True,
                 'ylabel' : "jobs"  }
    return generateTimedStackedBarPlot( filename, dataDict, metadata )

  def _plotNumberOfReschedules( self, startTime, endTime, condDict, groupingFields, filename ):
    selectFields = ( self._getSQLStringForGrouping( groupingFields) + ", %s, %s, SUM(%s/%s)",
                     groupingFields + [ 'startTime', 'bucketLength',
                                    'Reschedules', 'entriesInBucket'
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
    metadata = { 'title' : 'Reschedules by %s' % " -> ".join( groupingFields ) ,
                 'starttime' : startTime,
                 'endtime' : endTime,
                 'span' : granularity,
                 'skipEdgeColor' : True,
                 'ylabel' : "reschedules"  }
    return generateTimedStackedBarPlot( filename, dataDict, metadata )
