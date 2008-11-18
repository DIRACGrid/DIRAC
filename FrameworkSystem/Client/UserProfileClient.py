
from DIRAC import S_OK, S_ERROR
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.Core.Utilities import DEncode

class UserProfileClient:

  def __init__( self, action, rpcClient = False ):
    self.rpcClient = rpcClient
    self.action = action

  def __getRPCClient(self):
    if self.rpcClient:
      return self.rpcClient
    return RPCClient( "Framework/UserProfileManager" )

  def storeWebData( self, dataKey, data ):
    try:
      stub = DEncode.encode( data )
    except Exception, e:
      return S_ERROR( "Cannot encode data:%s" % str(e) )
    return self.__getRPCClient().storeWebProfileData( self.action, dataKey, stub )

  def retrieveWebData( self, dataKey ):
    result = self.__getRPCClient().retrieveWebProfileData( self.action, dataKey )
    if not result[ 'OK' ]:
      return result
    try:
      dataObj, lenData = DEncode.decode( result[ 'Value' ] )
    except Exception, e:
      return S_ERROR( "Cannot decode data: %s" % str(e) )
    return S_OK( dataObj )

  def deleteProfiles( self, userList ):
    return self.__getRPCClient().deleteProfiles( userList )
