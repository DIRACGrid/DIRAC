from DIRAC import S_OK, S_ERROR
from DIRAC.AccountingSystem.Client.Types.DataOperation import DataOperation
from DIRAC.AccountingSystem.private.DBUtils import DBUtils

class Summaries(DBUtils):

  def __init__( self, db, setup ):
    DBUtils.__init__( self, db, setup )

  def generate( self, summaryName, startTime, endTime, argsDict ):
    funcName = "_summary%s" % summaryName
    try:
      funcObj = getattr( self, funcName )
    except Exception, e:
      return S_ERROR( "Summary %s is not defined" % summaryName )
    return funcObj( startTime, endTime, argsDict )

  def _summaryDataBySource( self, startTime, endTime, argsDict ):
    """
      argsDict: Source -> Summary only sites in source. If not present summary all.
    """
    if 'Source' not in argsDict:
      condDict = {}
    else:
      condDict = { "Source" : argsDict[ 'Source' ] }
    returnFields = [ ( "Source", "" ) ]
    do = DataOperation()
    for fieldTuple in do.definitionAccountingFields:
      returnFields.append( ( fieldTuple[0], "SUM" ) )
    retVal = self._retrieveBucketedData( "DataOperation",
                                              startTime,
                                              endTime,
                                              returnFields,
                                              condDict,
                                              [ "Source" ],
                                              [ "Source" ] )
    if not retVal[ 'OK' ]:
      return retVal
    return S_OK( ( returnFields, retVal[ 'Value' ] ) )

  def _summaryDataByDestination( self, startTime, endTime, argsDict ):
    """
      argsDict: Source -> Summary only sites in source. If not present summary all.
    """
    if 'Destination' not in argsDict:
      condDict = {}
    else:
      condDict = { "Destination" : argsDict[ 'Destination' ] }
    returnFields = [ ( "Destination", "" ) ]
    do = DataOperation()
    for fieldTuple in do.definitionAccountingFields:
      returnFields.append( ( fieldTuple[0], "SUM" ) )
    retVal = self._retrieveBucketedData( "DataOperation",
                                              startTime,
                                              endTime,
                                              returnFields,
                                              condDict,
                                              [ "Destination" ],
                                              [ "Destination" ] )
    if not retVal[ 'OK' ]:
      return retVal
    return S_OK( ( returnFields, retVal[ 'Value' ] ) )