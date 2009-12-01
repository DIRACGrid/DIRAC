# $HeadURL$
__RCSID__ = "$Id$"

from DIRAC import gConfig, S_OK, S_ERROR
from DIRAC.Core.Utilities.SOAPFactory import getSOAPClient

class VOMSAdmin:
  
  def __init__( self, url = False ):
    if not url:
      url = gConfig.getValue( "/Registry/VOMSAdminURL", "" )
    if not url:
      raise Exception( "No URL defined for VOMSAdmin" )
    self.__soapClient = getSOAPClient( "%s?wsdl" % url )
    
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
  
  def listMembers(self):
    try:
      result = self.__soapClient.service.listMembers()
    except Exception, e:
      return S_ERROR( "Error in function listMembers: %s" %  str(e) )
    return S_OK( self.__processListDictReturn( result.listMembersReturn ) )
  
  def listRoles( self ):
    try:
      result = self.__soapClient.service.listRoles()
    except Exception, e:
      return S_ERROR( "Error in function listRoles: %s" %  str(e) )
    return S_OK( self.__processListReturn( result.listRolesReturn ) )
    
  
  def listUsersWithRole( self, group, role ):
    try:
      result = self.__soapClient.service.listUsersWithRole( group, role )
    except Exception, e:
      return S_ERROR( "Error in function listUsersWithRole: %s" %  str(e) )
    return S_OK( self.__processListDictReturn( result.listUsersWithRoleReturn ) )
  