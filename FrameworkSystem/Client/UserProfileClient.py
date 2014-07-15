
import re
import types
from DIRAC import S_OK, S_ERROR
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.Core.Utilities import DEncode, Time

class UserProfileClient:

  def __init__( self, profile, rpcClientFunctor = False ):
    if rpcClientFunctor:
      self.rpcClientFunctor = rpcClientFunctor
    else:
      self.rpcClientFunctor = RPCClient
    self.profile = profile

  def __getRPCClient( self ):
    return self.rpcClientFunctor( "Framework/UserProfileManager" )

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

  def storeVar( self, varName, data, perms = {} ):
    try:
      stub = DEncode.encode( data )
    except Exception, e:
      return S_ERROR( "Cannot encode data:%s" % str( e ) )
    return self.__getRPCClient().storeProfileVar( self.profile, varName, stub, perms )

  def __decodeVar( self, data, dataTypeRE ):
    try:
      dataObj, lenData = DEncode.decode( data )
    except Exception, e:
      return S_ERROR( "Cannot decode data: %s" % str( e ) )
    if dataTypeRE:
      result = self.checkTypeRe( dataObj, dataTypeRE )
      if not result[ 'OK' ]:
        return result
    return S_OK( dataObj )

  def retrieveVar( self, varName, dataTypeRE = False ):
    rpcClient = self.__getRPCClient()
    result = rpcClient.retrieveProfileVar( self.profile, varName )
    if not result[ 'OK' ]:
      return result
    return self.__decodeVar( result[ 'Value' ], dataTypeRE )

  def retrieveVarFromUser( self, ownerName, ownerGroup, varName, dataTypeRE = False ):
    rpcClient = self.__getRPCClient()
    result = rpcClient.retrieveProfileVarFromUser( ownerName, ownerGroup, self.profile, varName )
    if not result[ 'OK' ]:
      return result
    return self.__decodeVar( result[ 'Value' ], dataTypeRE )

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
      return S_ERROR( "Cannot decode data: %s" % str( e ) )
    return S_OK( dataObj )

  def listAvailableVars( self, filterDict = {} ):
    rpcClient = self.__getRPCClient()
    return rpcClient.listAvailableProfileVars( self.profile, filterDict )

  def getUserProfiles( self ):
    rpcClient = self.__getRPCClient()
    return rpcClient.getUserProfiles()

  def setVarPermissions( self, varName, perms ):
    rpcClient = self.__getRPCClient()
    return rpcClient.setProfileVarPermissions( self.profile, varName, perms )

  def getVarPermissions( self, varName ):
    rpcClient = self.__getRPCClient()
    return rpcClient.getProfileVarPermissions( self.profile, varName )

  def deleteVar( self, varName ):
    return self.__getRPCClient().deleteProfileVar( self.profile, varName )

  def deleteProfiles( self, userList ):
    return self.__getRPCClient().deleteProfiles( userList )

  def storeHashTag( self, tagName ):
    return self.__getRPCClient().storeHashTag( tagName )

  def retrieveHashTag( self, hashTag ):
    return self.__getRPCClient().retrieveHashTag( hashTag )

  def retrieveAllHashTags( self ):
    return self.__getRPCClient().retrieveAllHashTags()
  
  def getUserProfileNames( self, permission = dict() ):
    """
    it returns the available profile names by not taking account the permission: ReadAccess and PublishAccess
    """
    return self.__getRPCClient().getUserProfileNames( permission )
