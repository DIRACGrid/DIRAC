
from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.AccountingSystem.Client.Types.Job import Job
from DIRAC.AccountingSystem.private.Plotters.BasePlotter import BasePlotter
from DIRAC.AccountingSystem.private.Plots import *
from DIRAC.Core.Utilities import Time

class WMSHistoryPlotter(BasePlotter):

  _typeName = "WMSHistory"
  _typeKeyFields = [ dF[0] for dF in Job().definitionKeyFields ]

  def _plotNumberOfJobs( self, startTime, endTime, condDict, groupingFields, filename ):
    selectFields = ( self._getSQLStringForGrouping( groupingFields) + ", %s, %s, SUM(%s)/(SUM(%s)/COUNT(%s))",
                     groupingFields + [ 'startTime', 'bucketLength',
                                    'Jobs', 'entriesInBucket', 'entriesInBucket'
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