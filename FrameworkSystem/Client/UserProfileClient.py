
import re
import types
from DIRAC import S_OK, S_ERROR
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.Core.Utilities import DEncode, Time

class UserProfileClient:

  def __init__( self, action, rpcClient = False ):
    self.rpcClient = rpcClient
    self.action = action

  def __getRPCClient(self):
    if self.rpcClient:
      return self.rpcClient
    return RPCClient( "Framework/UserProfileManager" )

  def __generateTypeDest( self, dataObj ):
    cType = type( dataObj )
    if cType == types.BooleanType:
      return "b"
    if cType == types.NoneType:
      return "o"
    if cType in ( types.IntType, types.LongType, types.FloatType ):
      return "n"
    if cType in ( types.StringType, types.UnicodeType ):
      return "s"
    if cType in Time._allTypes:
      return "t"
    if cType in ( types.ListType, types.TupleType ):
      return "l%se" % "".join( [ self.__generateTypeDest( so ) for so in dataObj ] )
    if cType == types.DictType:
      return "d%se" % "".join( [ "%s%s" % ( self.__generateTypeDest( k ),
                                            self.__generateTypeDest( dataObj[k] ) ) for k in dataObj ] )
      return ""

  def checkTypeRe( self, dataObj, typeRE ):
    if typeRE[0] != "^":
      typeRE = "^%s" % typeRE
    if typeRE[-1] != "$":
      typeRE = "%s$" % typeRE
    typeDesc = self.__generateTypeDest( dataObj )
    if not re.match( typeRE, typeDesc ):
      return S_ERROR( "Stored data does not match typeRE: %s vs %s" % ( typeDesc, typeRE ) )
    return S_OK()

  def storeWebData( self, dataKey, data ):
    try:
      stub = DEncode.encode( data )
    except Exception, e:
      return S_ERROR( "Cannot encode data:%s" % str(e) )
    return self.__getRPCClient().storeWebProfileData( self.action, dataKey, stub )

  def retrieveWebData( self, dataKey, dataTypeRE = False ):
    rpcClient = self.__getRPCClient()
    result = rpcClient.retrieveWebProfileData( self.action, dataKey )
    if not result[ 'OK' ]:
      return result
    try:
      dataObj, lenData = DEncode.decode( result[ 'Value' ] )
    except Exception, e:
      return S_ERROR( "Cannot decode data: %s" % str(e) )
    if dataTypeRE:
      result = self.checkTypeRe( dataObj, dataTypeRE )
      if not result[ 'OK' ]:
        return result
    return S_OK( dataObj )

  def deleteWebData( self, dataKey ):
    return self.__getRPCClient().deleteWebProfileData( self.action, dataKey )

  def deleteProfiles( self, userList ):
    return self.__getRPCClient().deleteProfiles( userList )
