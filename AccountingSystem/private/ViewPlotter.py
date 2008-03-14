from DIRAC import S_OK, S_ERROR
from DIRAC.AccountingSystem.Client.Types.DataOperation import DataOperation
from DIRAC.AccountingSystem.private.DBUtils import DBUtils

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
    return funcObj( startTime, endTime, argsDict )

  def viewsList( self ):
    viewList = []
    for attr in dir( self ):
      if attr.find( "_view" ) == 0:
        viewList.append( attr.replace( "_view", "" ) )
    viewList.sort()
    return viewList

  def _viewQualityBySource( self, startTime, endTime, argsDict ):
    sourceDataDict, granularity = self.__getDataOperationBySource( startTime, endTime, argsDict )
    for keyField in sourceDataDict:
      sourceDataDict[ keyField ] = self._fillWithZero( granularity, startTime, endTime, sourceDataDict[ keyField ] )
    #TODO: I'm here

  def __getDataOperationBySource( self, startTime, endTime, argsDict ):
    typeName = "DataOperation"
    if 'Source' not in argsDict:
      condDict = {}
    else:
      condDict = { "Source" : argsDict[ 'Source' ] }
    selectFields = ( "%s, %s, %s, SUM(%s), SUM(%s)", ( 'Source', 'startTime', 'bucketLength', 'TransferOK', 'TransferTotal' ) )
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
    sourceDataDict = self._groupByField( 0, retVal[ 'Value' ] )
    coarsestGranularity = self._getBucketLengthForTime( typeName, startTime )
    print coarsestGranularity
    for keyField in sourceDataDict:
      sourceDataDict[ keyField ] = self._normalizeToGranularity( coarsestGranularity, sourceDataDict[ keyField ] )
    return( sourceDataDict, coarsestGranularity )
