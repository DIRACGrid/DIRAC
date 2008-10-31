
from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.AccountingSystem.Client.Types.SRMSpaceTokenDeployment import SRMSpaceTokenDeployment
from DIRAC.AccountingSystem.private.Plotters.BaseReporter import BaseReporter
from DIRAC.Core.Utilities import Time

class SRMSpaceTokenDeploymentPlotter(BaseReporter):

  _typeName = "SRMSpaceTokenDeployment"
  _typeKeyFields = [ dF[0] for dF in SRMSpaceTokenDeployment().definitionKeyFields ]

  def _reportAvailableSpace( self, reportRequest ):
    return self._historicReport( reportRequest, "AvailableSpace" )

  def _reportUsedSpace( self, reportRequest ):
    return self._historicReport( reportRequest, "UsedSpace" )

  def _reportTotalOnlineSpace( self, reportRequest ):
    return self._historicReport( reportRequest, "TotalOnline" )

  def _reportUsedOnlineSpace( self, reportRequest ):
    return self._historicReport( reportRequest, "UsedOnline" )

  def _reportFreeOnlineSpace( self, reportRequest ):
    return self._historicReport( reportRequest, "FreeOnline" )

  def _reportTotalNearlineSpace( self, reportRequest ):
    return self._historicReport( reportRequest, "TotalNearline" )

  def _reportUsedNearlineSpace( self, reportRequest ):
    return self._historicReport( reportRequest, "UsedNearline" )

  def _reportFreeNearlineSpace( self, reportRequest ):
    return self._historicReport( reportRequest, "FreeNearline" )

  def _reportReservedNearlineSpace( self, reportRequest ):
    return self._historicReport( reportRequest, "ReservedNearline" )

  def _historicReport( self, reportRequest, fieldToBeReported ):
    selectFields = ( self._getSQLStringForGrouping( reportRequest[ 'groupingFields' ] ) + ", %s, %s, SUM(%s/%s)/1073741824",
                     reportRequest[ 'groupingFields' ] + [ 'startTime', 'bucketLength',
                                    fieldToBeReported, 'entriesInBucket'
                                   ]
                   )
    retVal = self._getTimedData( reportRequest[ 'startTime' ],
                                reportRequest[ 'endTime' ],
                                selectFields,
                                reportRequest[ 'condDict' ],
                                reportRequest[ 'groupingFields' ],
                                { 'convertToGranularity' : 'average', 'checkNone' : True } )
    if not retVal[ 'OK' ]:
      return retVal
    dataDict, granularity = retVal[ 'Value' ]
    self.stripDataField( dataDict, 0 )
    dataDict = self._fillWithZero( granularity, reportRequest[ 'startTime' ], reportRequest[ 'endTime' ], dataDict )
    return S_OK( { 'data' : dataDict, 'granularity' : granularity } )

  def _plotAvailableSpace( self, reportRequest, plotInfo, filename ):
    title = 'Available space by %s' % " -> ".join( reportRequest[ 'groupingFields' ] )
    return self._historicPlot( title, reportRequest, plotInfo, filename )

  def _plotUsedSpace( self, reportRequest, plotInfo, filename ):
    title = 'Used space by %s' % " -> ".join( reportRequest[ 'groupingFields' ] )
    return self._historicPlot( title, reportRequest, plotInfo, filename )

  def _plotTotalOnlineSpace( self, reportRequest, plotInfo, filename ):
    title = 'Total online space by %s' % " -> ".join( reportRequest[ 'groupingFields' ] )
    return self._historicPlot( title, reportRequest, plotInfo, filename )

  def _plotUsedOnlineSpace( self, reportRequest, plotInfo, filename ):
    title = 'Used online space by %s' % " -> ".join( reportRequest[ 'groupingFields' ] )
    return self._historicPlot( title, reportRequest, plotInfo, filename )

  def _plotFreeOnlineSpace( self, reportRequest, plotInfo, filename ):
    title = 'Free online space by %s' % " -> ".join( reportRequest[ 'groupingFields' ] )
    return self._historicPlot( title, reportRequest, plotInfo, filename )

  def _plotTotalNearlineSpace( self, reportRequest, plotInfo, filename ):
    title = 'Total nearline space by %s' % " -> ".join( reportRequest[ 'groupingFields' ] )
    return self._historicPlot( title, reportRequest, plotInfo, filename )

  def _plotUsedNearlineSpace( self, reportRequest, plotInfo, filename ):
    title = 'Used nearline space by %s' % " -> ".join( reportRequest[ 'groupingFields' ] )
    return self._historicPlot( title, reportRequest, plotInfo, filename )

  def _plotFreeNearlineSpace( self, reportRequest, plotInfo, filename ):
    title = 'Free nearline space by %s' % " -> ".join( reportRequest[ 'groupingFields' ] )
    return self._historicPlot( title, reportRequest, plotInfo, filename )

  def _plotReservedNearlineSpace( self, reportRequest, plotInfo, filename ):
    title = 'Reserved nearline space by %s' % " -> ".join( reportRequest[ 'groupingFields' ] )
    return self._historicPlot( title, reportRequest, plotInfo, filename )

  def _historicPlot( self, title, reportRequest, plotInfo, filename ):
    metadata = { 'title' : title,
                 'starttime' : reportRequest[ 'startTime' ],
                 'endtime' : reportRequest[ 'endTime' ],
                 'span' : plotInfo[ 'granularity' ],
                 'ylabel' : "GiB"  }
    return self._generateTimedStackedBarPlot( filename, plotInfo[ 'data' ], metadata )