import md5
from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.AccountingSystem.Client.Types.DataOperation import DataOperation
from DIRAC.AccountingSystem.private.DBUtils import DBUtils
from DIRAC.Core.Utilities.Plots.StackedBarPlot import StackedBarPlot

class ViewPlotter(DBUtils):

  requiredParams = ( "PlotSize", )

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
    retVal = self.__getDataOperationBandwidthBySource( startTime, endTime, argsDict )
    if not retVal[ 'OK' ]:
      return retVal
    dataDict, granularity = retVal[ 'Value' ]
    dataDict[ 'Failed' ] = self.stripDataField( dataDict, 0 )[0]
    plot = StackedBarPlot( filename )
    plot.plot( dataDict )

  def __getDataOperationBandwidthBySource( self, startTime, endTime, argsDict ):
    typeName = "DataOperation"
    keyFields = ( 'Source' )
    condDict = {}
    for keyword in keyFields:
      if keyword in argsDict:
        condDict[ keyword ] = argsDict[ keyword ]
    selectFields = ( "%s, %s, %s, SUM(%s)*(SUM(%s)/SUM(%s)), SUM(%s)*(1-(SUM(%s)/SUM(%s)))",
                     ( 'Source', 'startTime', 'bucketLength',
                       'TransferSize', 'TransferOK', 'TransferTotal',
                       'TransferSize', 'TransferOK', 'TransferTotal'
                      )
                   )
    retVal = self._retrieveBucketedData( "DataOperation",
                                          startTime,
                                          endTime,
                                          selectFields,
                                          condDict,
                                          [ 'startTime', "Source" ],
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
