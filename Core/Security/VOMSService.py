""" VOMSService class encapsulated connection to the VOMS service for a given VO
"""

__RCSID__ = "$Id$"

from DIRAC import gConfig, S_OK, S_ERROR
from DIRAC.Core.Utilities.SOAPFactory import getSOAPClient
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getVOOption
from DIRAC.ConfigurationSystem.Client.Helpers.CSGlobals import getVO

def _processListReturn( soapReturn ):
  data = []
  if soapReturn:
    for entry in soapReturn:
      data.append( str( entry ) )
  return data

def _processListDictReturn( soapReturn ):
  data = []
  if soapReturn:
    for entry in soapReturn:
      entryData = {}
      for info in entry:

        try:
          entryData[ info[0] ] = str( info[1] )
        except:
          pass
      data.append( entryData )
  return data

class VOMSService( object ):

  def __init__( self, vo = None, adminUrl = False, attributesUrl = False, certificatesUrl = False ):

    if vo is None:
      vo = getVO()
    if not vo:
      raise Exception( 'No VO name given' )

    self.vo = vo
    self.vomsVO = getVOOption( vo, "VOMSName" )
    if not self.vomsVO:
      raise Exception( "Can not get VOMS name for VO %s" % vo )
    self.__soapClients = {}
    for key, url in ( ( 'Admin', adminUrl ), ( 'Attributes', attributesUrl ), ( 'Certificates', certificatesUrl ) ):
      urls = []
      if not url:
        url = gConfig.getValue( "/Registry/VO/%s/VOMSServices/VOMS%s" % ( self.vo, key ), "" )
      if not url:
        result = gConfig.getSections( '/Registry/VO/%s/VOMSServers' % self.vo )
        if result['OK']:
          vomsServers = result['Value']
          for server in vomsServers:
            urls.append( 'https://%s:8443/voms/%s/services/VOMS%s' % ( server, self.vomsVO, key ) )
      else:
        urls = [url]

      gotURL = False
      for url in urls:
        retries = 3
        while retries:
          retries -= 1
          try:
            client = getSOAPClient( "%s?wsdl" % url )
            client.set_options(headers={"X-VOMS-CSRF-GUARD":"1"})
            self.__soapClients[ key ] = client
            gotURL = True
            break
          except Exception:
            pass
        if gotURL:
          break
      if not gotURL:
        raise Exception( 'Could not connect to the %s service for VO %s' % ( key, self.vo ) )

  def admListMembers( self ):
    try:
      result = self.__soapClients[ 'Admin' ].service.listMembers()
    except Exception as e:
      return S_ERROR( "Error in function listMembers: %s" % e )
    if 'listMembersReturn' in dir( result ):
      return S_OK( _processListDictReturn( result.listMembersReturn ) )
    return S_OK( _processListDictReturn( result ) )


  def admListCertificates( self, dn, ca ):
    try:
      UserID = self.__soapClients[ 'Certificates' ].service.getUserIdFromDn( dn, ca )
      result = self.__soapClients[ 'Certificates' ].service.getCertificates( UserID )
    except Exception as e:
      return S_ERROR( "Error in function getCertificates: %s" % e )
    if 'listCertificatesReturn' in dir( result ):
      return S_OK( _processListDictReturn( result.listCertificatesReturn ) )
    return S_OK( _processListDictReturn( result ) )

  def admListRoles( self ):
    try:
      result = self.__soapClients[ 'Admin' ].service.listRoles()
    except Exception as e:
      return S_ERROR( "Error in function listRoles: %s" % e )
    if 'listRolesReturn' in dir( result ):
      return S_OK( _processListReturn( result.listRolesReturn ) )
    return S_OK( _processListReturn( result ) )


  def admListUsersWithRole( self, group, role ):
    try:
      result = self.__soapClients[ 'Admin' ].service.listUsersWithRole( group, role )
    except Exception as e:
      return S_ERROR( "Error in function listUsersWithRole: %s" % e )
    if 'listUsersWithRoleReturn' in dir( result ):
      return S_OK( _processListDictReturn( result.listUsersWithRoleReturn ) )
    return S_OK( _processListDictReturn( result ) )

  def admGetVOName( self ):
    try:
      result = self.__soapClients[ 'Admin' ].service.getVOName()
    except Exception as e:
      return S_ERROR( "Error in function getVOName: %s" % e )
    return S_OK( result )

  def attGetUserNickname( self, dn, ca ):
    user = self.__soapClients[ 'Attributes' ].factory.create( 'ns0:User' )
    user.DN = dn
    user.CA = ca
    try:
      result = self.__soapClients[ 'Attributes' ].service.listUserAttributes( user )
    except Exception as e:
      return S_ERROR( "Error in function getUserNickname: %s" % e )

    if result is not None:
      if 'listUserAttributesReturn' in dir( result ):
        return S_OK( result.listUserAttributesReturn[0].value )

      return S_OK( result[0].value )
    else:
      return S_ERROR( result )

  def getUsers( self ):
    """ Get all the users of the VOMS VO with their detailed information

    :return: user dictionary keyed by the user DN
    """

    vomsUsers = {}

    result = self.admListMembers()
    if not result['OK']:
      return result
    members = result['Value']

    result = self.admListRoles()
    if not result['OK']:
      return result
    roles = result['Value']

    roleMembers = {}
    for role in roles:
      result = self.admListUsersWithRole( '/%s' % self.vomsVO, role )
      if not result['OK']:
        return result
      roleMembers[role] = result['Value']

    for member in members:
      member['Roles'] = []
      if "DN" in member:
        DN = member.pop( 'DN' )
        for role in roles:
          for rm in roleMembers[role]:
            if DN == rm['DN']:
              member['Roles'].append( role )

        vomsUsers[ DN ] = member

    return S_OK( vomsUsers )

