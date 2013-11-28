# $HeadURL$
__RCSID__ = "$Id$"

from DIRAC import gConfig, S_OK, S_ERROR
from DIRAC.Core.Utilities.SOAPFactory import getSOAPClient

def _processListReturn( soapReturn ):
  data = []
  for entry in soapReturn:
    data.append( str( entry ) )
  return data

def _processListDictReturn( soapReturn ):
  data = []
  for entry in soapReturn:
    entryData = {}
    for info in entry:
      entryData[ info[0] ] = str( info[1] )
    data.append( entryData )
  return data

class VOMSService:

  def __init__( self, adminUrl = False, attributesUrl = False ):
    self.__soapClients = {}
    for key, url in ( ( 'Admin', adminUrl ), ( 'Attributes', attributesUrl ) ):
      if not url:
        url = gConfig.getValue( "/Registry/VOMS/URLs/VOMS%s" % key, "" )
      if not url:
        raise Exception( "No URL defined for VOMS%s" % key )
      retries = 3
      while retries:
        retries -= 1
        try:
          client = getSOAPClient( "%s?wsdl" % url )
          client.set_options(headers={"X-VOMS-CSRF-GUARD":"1"})
          self.__soapClients[ key ] = client
          break
        except Exception:
          if retries:
            pass
          else:
            raise

  def admListMembers( self ):
    try:
      result = self.__soapClients[ 'Admin' ].service.listMembers()
    except Exception, e:
      return S_ERROR( "Error in function listMembers: %s" % str( e ) )
    if 'listMembersReturn' in dir( result ):
      return S_OK( _processListDictReturn( result.listMembersReturn ) )
    return S_OK( _processListDictReturn( result ) )

  def admListRoles( self ):
    try:
      result = self.__soapClients[ 'Admin' ].service.listRoles()
    except Exception, e:
      return S_ERROR( "Error in function listRoles: %s" % str( e ) )
    if 'listRolesReturn' in dir( result ):
      return S_OK( _processListReturn( result.listRolesReturn ) )
    return S_OK( _processListReturn( result ) )


  def admListUsersWithRole( self, group, role ):
    try:
      result = self.__soapClients[ 'Admin' ].service.listUsersWithRole( group, role )
    except Exception, e:
      return S_ERROR( "Error in function listUsersWithRole: %s" % str( e ) )
    if 'listUsersWithRoleReturn' in dir( result ):
      return S_OK( _processListDictReturn( result.listUsersWithRoleReturn ) )
    return S_OK( _processListDictReturn( result ) )

  def admGetVOName( self ):
    try:
      result = self.__soapClients[ 'Admin' ].service.getVOName()
    except Exception, e:
      return S_ERROR( "Error in function getVOName: %s" % str( e ) )
    return S_OK( result )

  def attGetUserNickname( self, dn, ca ):
    user = self.__soapClients[ 'Attributes' ].factory.create( 'ns0:User' )
    user.DN = dn
    user.CA = ca
    try:
      result = self.__soapClients[ 'Attributes' ].service.listUserAttributes( user )
    except Exception, e:
      return S_ERROR( "Error in function getUserNickname: %s" % str( e ) )
    if 'listUserAttributesReturn' in dir( result ):
      return S_OK( result.listUserAttributesReturn[0].value )
    return S_OK( result[0].value )
