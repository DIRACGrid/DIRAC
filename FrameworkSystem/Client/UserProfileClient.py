
import re
import types
from DIRAC import S_OK, S_ERROR
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.Core.Utilities import DEncode, Time

class UserProfileClient:

  def __init__( self, profile, rpcClient = False ):
    self.rpcClient = rpcClient
    self.profile = profile

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

  def storeVar( self, varName, data ):
    try:
      stub = DEncode.encode( data )
    except Exception, e:
      return S_ERROR( "Cannot encode data:%s" % str(e) )
    return self.__getRPCClient().storeProfileVar( self.profile, varName, stub )

  def retrieveVar( self, varName, dataTypeRE = False ):
    rpcClient = self.__getRPCClient()
    result = rpcClient.retrieveProfileVar( self.profile, varName )
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
  
  def retrieveAllVars( self ):
    rpcClient = self.__getRPCClient()
    result = rpcClient.retrieveProfileAllVars( self.profile )
    if not result[ 'OK' ]:
      return result
    try:
      encodedData = result[ 'Value' ]
      dataObj = {}
      for k in encodedData:
        v, lenData = DEncode.decode( encodedData[k] )
        dataObj[ k ] = v
    except Exception, e:
      return S_ERROR( "Cannot decode data: %s" % str(e) )
    return S_OK( dataObj )
  
  def deleteVar( self, varName ):
    return self.__getRPCClient().deleteProfileData( self.profile, varName )

  def deleteProfiles( self, userList ):
    return self.__getRPCClient().deleteProfiles( userList )
  
  def storeHashTag( self, tagName ):
    return self.__getRPCClient().storeHashTag( tagName )

  def retrieveHashTag( self, hashTag ):
    return self.__getRPCClient().retrieveHashTag( hashTag )
  
  def retrieveAllHashTags( self ):
    return self.__getRPCClient().retrieveAllHashTags()