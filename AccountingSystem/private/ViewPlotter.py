import md5
from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.AccountingSystem.Client.Types.DataOperation import DataOperation
from DIRAC.AccountingSystem.private.DBUtils import DBUtils
from DIRAC.AccountingSystem.private.Plots import generateTimedStackedBar

class ViewPlotter(DBUtils):

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
    filehash = md5.new()
    filehash.update( "%s:%s:%s:%s" % ( viewName, startTime, endTime, argsDict ) )
    #TODO: fix this /tmp
    fileName = "/tmp/%s.png" % filehash.hexdigest()
    try:
      return funcObj( startTime, endTime, argsDict, fileName )
    except Exception, e:
      gLogger.exception( "Exception while generating %s view" % viewName )
      return S_ERROR( "Exception while generating %s view: %s" % ( viewName, str(e) ) )

  def viewsList( self ):
    viewList = []
    for attr in dir( self ):
      if attr.find( "_view" ) == 0:
        viewList.append( attr.replace( "_view", "" ) )
    viewList.sort()
    return viewList

  def _viewBandwidthBySource( self, startTime, endTime, argsDict, filename ):
    return self.__generateDataOperationBandwidthPlot(startTime, endTime, [ 'Source' ], argsDict, filename)

  def _viewBandwidthByDestination( self, startTime, endTime, argsDict, filename ):
    return self.__generateDataOperationBandwidthPlot(startTime, endTime, [ 'Destination' ], argsDict, filename)

  def _viewBandwidthByChannel( self, startTime, endTime, argsDict, filename ):
    return self.__generateDataOperationBandwidthPlot(startTime, endTime, [ 'Source', 'Destination' ], argsDict, filename)

  def __generateDataOperationBandwidthPlot( self, startTime, endTime, keyNameList, argsDict, filename ):
    retVal = self.__getDataOperationBandwidth( startTime, endTime, keyNameList, argsDict )
    if not retVal[ 'OK' ]:
      return retVal
    dataDict, granularity = retVal[ 'Value' ]
    dataDict[ 'Failed' ] = self.stripDataField( dataDict, 0 )[0]
    gLogger.info( "Generating plot", "%s with granularity of %s" % ( filename, granularity ) )
    metadata = { 'title' : 'Transfer bandwidth by %s' % " -> ".join( keyNameList ) ,
                 'ylabel' : 'bytes',
                 'starttime' : startTime,
                 'endtime' : endTime,
                 'span' : granularity }
    return generateTimedStackedBar( filename, dataDict, metadata )

  def __getDataOperationBandwidth( self, startTime, endTime, keyNameList, argsDict ):
    typeName = "DataOperation"
    condDict = {}
    for keyword in keyNameList:
      if keyword in argsDict:
        condDict[ keyword ] = argsDict[ keyword ]
    if len( keyNameList ) == 1:
      keySQLString = "%s"
    else:
      keySQLString = "CONCAT( %s, ' -> ', %s )"
    selectFields = ( keySQLString + ", %s, %s, SUM(%s)*(SUM(%s)/SUM(%s)), SUM(%s)*(1-(SUM(%s)/SUM(%s)))",
                     keyNameList + [ 'startTime', 'bucketLength',
                       'TransferSize', 'TransferOK', 'TransferTotal',
                       'TransferSize', 'TransferOK', 'TransferTotal'
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
      keyData = self._normalizeToGranularity( coarsestGranularity, dataDict[ keyField ] )
      dataDict[ keyField ] = keyData
      dataDict[ keyField ] = self._fillWithZero( coarsestGranularity, startTime, endTime, keyData )
    return S_OK( ( dataDict, coarsestGranularity ) )
