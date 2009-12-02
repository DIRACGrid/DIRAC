# $HeadURL$
__RCSID__ = "$Id$"

from DIRAC import gConfig, S_OK, S_ERROR
from DIRAC.Core.Utilities.SOAPFactory import getSOAPClient

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
          self.__soapClients[ key ] = getSOAPClient( "%s?wsdl" % url )
          break
        except:
          if retries:
            pass
          else:
            raise

  def __processListReturn( self, soapReturn ):
    data = []
    for entry in soapReturn:
      data.append( str( entry ) )
    return data

  def __processListDictReturn( self, soapReturn ):
    data = []
    for entry in soapReturn:
      entryData = {}
      for info in entry:
        entryData[ info[0] ] = str( info[1] )
      data.append( entryData )
    return data

  def admListMembers( self ):
    try:
      result = self.__soapClients[ 'Admin' ].service.listMembers()
    except Exception, e:
      return S_ERROR( "Error in function listMembers: %s" % str( e ) )
    return S_OK( self.__processListDictReturn( result.listMembersReturn ) )

  def admListRoles( self ):
    try:
      result = self.__soapClients[ 'Admin' ].service.listRoles()
    except Exception, e:
      return S_ERROR( "Error in function listRoles: %s" % str( e ) )
    return S_OK( self.__processListReturn( result.listRolesReturn ) )


  def admListUsersWithRole( self, group, role ):
    try:
      result = self.__soapClients[ 'Admin' ].service.listUsersWithRole( group, role )
    except Exception, e:
      return S_ERROR( "Error in function listUsersWithRole: %s" % str( e ) )
    return S_OK( self.__processListDictReturn( result.listUsersWithRoleReturn ) )

  def admGetVOName( self ):
    try:
      result = self.__soapClients[ 'Admin' ].service.getVOName()
    except Exception, e:
      return S_ERROR( "Error in function getVOName: %s" % str( e ) )
    return S_OK( result )

  def attGetUserNickname( self, DN, CA ):
    user = self.__soapClients[ 'Attributes' ].factory.create( 'ns0:User' )
    user.DN = DN
    user.CA = CA
    try:
      result = self.__soapClients[ 'Attributes' ].service.listUserAttributes( user )
    except Exception, e:
      return S_ERROR( "Error in function getUserNickname: %s" % str( e ) )
    return S_OK( result.listUserAttributesReturn[0].value )
