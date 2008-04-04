import md5
from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.AccountingSystem.Client.Types.DataOperation import DataOperation
from DIRAC.AccountingSystem.private.DBUtils import DBUtils
from DIRAC.AccountingSystem.private.Plots import *
from DIRAC.AccountingSystem.private.ViewsCache import gViewsCache
from DIRAC.Core.Utilities import Time

class ViewPlotter(DBUtils):

  requiredParams = ()

  def __init__( self, db, setup ):
    DBUtils.__init__( self, db, setup )

  def generate( self, viewName, startTime, endTime, argsDict ):
    missing = []
    for param in self.requiredParams:
      if param not in argsDict:
        missing.append( param )
    if missing:
      return S_ERROR( "Argument(s) %s missing" % ", ".join( missing ) )
    funcName = "_view%s" % viewName
    try:
      funcObj = getattr( self, funcName )
    except Exception, e:
      return S_ERROR( "View  %s is not defined" % viewName )
    return gViewsCache.generateView( viewName, startTime, endTime, argsDict, funcObj )

  def viewsList( self ):
    viewList = []
    for attr in dir( self ):
      if attr.find( "_view" ) == 0:
        viewList.append( attr.replace( "_view", "" ) )
    viewList.sort()
    return viewList

  def _viewSuceededTransfersBySource( self, startTime, endTime, argsDict, filename ):
    return self.__generateDataOperationSuceededTransfersPlot(startTime, endTime, [ 'Source' ], argsDict, filename)

  def _viewSuceededTransfersByDestination( self, startTime, endTime, argsDict, filename ):
    return self.__generateDataOperationSuceededTransfersPlot(startTime, endTime, [ 'Destination' ], argsDict, filename)

  def _viewSuceededTransfersByChannel( self, startTime, endTime, argsDict, filename ):
    return self.__generateDataOperationSuceededTransfersPlot(startTime, endTime, [ 'Source', 'Destination' ], argsDict, filename)

  def __generateDataOperationSuceededTransfersPlot( self, startTime, endTime, keyNameList, argsDict, filename ):
    retVal = self.__getDataOperationTransfers( startTime, endTime, keyNameList, argsDict )
    if not retVal[ 'OK' ]:
      return retVal
    dataDict, granularity = retVal[ 'Value' ]
    dataDict[ 'Failed' ] = self.stripDataField( dataDict, 0 )[0]
    dataDict = self._fillWithZero( granularity, startTime, endTime, dataDict )
    gLogger.info( "Generating plot", "%s with granularity of %s" % ( filename, granularity ) )
    metadata = { 'title' : 'Suceeded Transfers by %s' % " -> ".join( keyNameList ) ,
                 'ylabel' : 'files',
                 'starttime' : startTime,
                 'endtime' : endTime,
                 'span' : granularity }
    return generateTimedStackedBarPlot( filename, dataDict, metadata )

  def _viewFailedTransfersBySource( self, startTime, endTime, argsDict, filename ):
    return self.__generateDataOperationFailedTransfersPlot(startTime, endTime, [ 'Source' ], argsDict, filename)

  def _viewFailedTransfersByDestination( self, startTime, endTime, argsDict, filename ):
    return self.__generateDataOperationFailedTransfersPlot(startTime, endTime, [ 'Destination' ], argsDict, filename)

  def _viewFailedTransfersByChannel( self, startTime, endTime, argsDict, filename ):
    return self.__generateDataOperationFailedTransfersPlot(startTime, endTime, [ 'Source', 'Destination' ], argsDict, filename)

  def __generateDataOperationFailedTransfersPlot( self, startTime, endTime, keyNameList, argsDict, filename ):
    retVal = self.__getDataOperationTransfers( startTime, endTime, keyNameList, argsDict )
    if not retVal[ 'OK' ]:
      return retVal
    dataDict, granularity = retVal[ 'Value' ]
    dataDict[ 'Suceeded' ] = self.stripDataField( dataDict, 1 )[0]
    dataDict = self._fillWithZero( granularity, startTime, endTime, dataDict )
    gLogger.info( "Generating plot", "%s with granularity of %s" % ( filename, granularity ) )
    metadata = { 'title' : 'Failed Transfers by %s' % " -> ".join( keyNameList ) ,
                 'ylabel' : 'files',
                 'starttime' : startTime,
                 'endtime' : endTime,
                 'span' : granularity }
    return generateTimedStackedBarPlot( filename, dataDict, metadata )

  def __getDataOperationTransfers( self, startTime, endTime, keyNameList, argsDict ):
    typeName = "DataOperation"
    condDict = {}
    for keyword in keyNameList:
      if keyword in argsDict:
        condDict[ keyword ] = argsDict[ keyword ]
    if len( keyNameList ) == 1:
      keySQLString = "%s"
    else:
      keySQLString = "CONCAT( %s, ' -> ', %s )"
    selectFields = ( keySQLString + ", %s, %s, SUM(%s), SUM(%s)-SUM(%s)",
                     keyNameList + [ 'startTime', 'bucketLength',
                       'TransferOK', 'TransferTotal', 'TransferOK',
                      ]
                   )
    retVal = self._retrieveBucketedData( "DataOperation",
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
    coarsestGranularity = self._getBucketLengthForTime( typeName, startTime )
    for keyField in dataDict:
      dataDict[ keyField ] = self._sumToGranularity( coarsestGranularity, dataDict[ keyField ] )
    return S_OK( ( dataDict, coarsestGranularity ) )

  def _viewQualityBySource( self, startTime, endTime, argsDict, filename ):
    return self.__generateDataOperationQualityPlot(startTime, endTime, [ 'Source' ], argsDict, filename)

  def _viewQualityByDestination( self, startTime, endTime, argsDict, filename ):
    return self.__generateDataOperationQualityPlot(startTime, endTime, [ 'Destination' ], argsDict, filename)

  def _viewQualityByChannel( self, startTime, endTime, argsDict, filename ):
    return self.__generateDataOperationQualityPlot(startTime, endTime, [ 'Source', 'Destination' ], argsDict, filename)


  def __generateDataOperationQualityPlot( self, startTime, endTime, keyNameList, argsDict, filename ):
    retVal = self.__getDataOperationQuality( startTime, endTime, keyNameList, argsDict )
    if not retVal[ 'OK' ]:
      return retVal
    dataDict, granularity = retVal[ 'Value' ]
    self.stripDataField( dataDict, 0 )
    gLogger.info( "Generating plot", "%s with granularity of %s" % ( filename, granularity ) )
    metadata = { 'title' : 'Transfer quality by %s' % " -> ".join( keyNameList ) ,
                 'starttime' : startTime,
                 'endtime' : endTime,
                 'span' : granularity }
    return generateQualityPlot( filename, dataDict, metadata )

  def __getDataOperationQuality( self, startTime, endTime, keyNameList, argsDict ):
    typeName = "DataOperation"
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
                                    'TransferOK', 'TransferTotal'
                                   ]
                   )
    retVal = self._retrieveBucketedData( "DataOperation",
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
    coarsestGranularity = self._getBucketLengthForTime( typeName, startTime )
    for keyField in dataDict:
      dataDict[ keyField ] = self._averageToGranularity( coarsestGranularity, dataDict[ keyField ] )
    return S_OK( ( dataDict, coarsestGranularity ) )


  def _viewTransferedDataBySource( self, startTime, endTime, argsDict, filename ):
    return self.__generateDataOperationTransferedDataPlot(startTime, endTime, [ 'Source' ], argsDict, filename)

  def _viewTransferedDataByDestination( self, startTime, endTime, argsDict, filename ):
    return self.__generateDataOperationTransferedDataPlot(startTime, endTime, [ 'Destination' ], argsDict, filename)

  def _viewTransferedDataByChannel( self, startTime, endTime, argsDict, filename ):
    return self.__generateDataOperationTransferedDataPlot(startTime, endTime, [ 'Source', 'Destination' ], argsDict, filename)


  def __generateDataOperationTransferedDataPlot( self, startTime, endTime, keyNameList, argsDict, filename ):
    retVal = self.__getDataOperationTransferedData( startTime, endTime, keyNameList, argsDict )
    if not retVal[ 'OK' ]:
      return retVal
    dataDict, granularity = retVal[ 'Value' ]
    startTime = startTime - startTime % granularity
    self.stripDataField( dataDict, 0 )
    timeRange = range( startTime, endTime, granularity )
    for key in dataDict:
      for timeEntry in dataDict[ key ]:
        if timeEntry not in timeRange:
          print key, timeEntry, "JARL", Time.fromEpoch( timeEntry )
    gLogger.info( "Generating plot", "%s with granularity of %s" % ( filename, granularity ) )
    metadata = { 'title' : 'Transfered data by %s' % " -> ".join( keyNameList ) ,
                 'starttime' : startTime,
                 'endtime' : endTime,
                 'span' : granularity,
                 'ylabel' : "bytes",
                 'is_cumulative' : False }
    return generateCumulativePlot( filename, dataDict, metadata )

  def __getDataOperationTransferedData( self, startTime, endTime, keyNameList, argsDict ):
    typeName = "DataOperation"
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
                                    'TransferSize'
                                   ]
                   )
    retVal = self._retrieveBucketedData( "DataOperation",
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
    coarsestGranularity = self._getBucketLengthForTime( typeName, startTime )
    for keyField in dataDict:
      dataDict[ keyField ] = self._sumToGranularity( coarsestGranularity, dataDict[ keyField ] )
    return S_OK( ( dataDict, coarsestGranularity ) )
