# $HeadURL$
__RCSID__ = "$Id$"

import types
from DIRAC import S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client.Config import gConfig
from DIRAC.ConfigurationSystem.Client.Helpers.CSGlobals import getVO

gBaseRegistrySection = "/Registry"

def getUsernameForDN( dn, usersList = False ):
  if not usersList:
    retVal = gConfig.getSections( "%s/Users" % gBaseRegistrySection )
    if not retVal[ 'OK' ]:
      return retVal
    usersList = retVal[ 'Value' ]
  for username in usersList:
    if dn in gConfig.getValue( "%s/Users/%s/DN" % ( gBaseRegistrySection, username ), [] ):
      return S_OK( username )
  return S_ERROR( "No username found for dn %s" % dn )

def getDNForUsername( username ):
  dnList = gConfig.getValue( "%s/Users/%s/DN" % ( gBaseRegistrySection, username ), [] )
  if dnList:
    return S_OK( dnList )
  return S_ERROR( "No DN found for user %s" % username )

def getGroupsForDN( dn ):
  retVal = getUsernameForDN( dn )
  if not retVal[ 'OK' ]:
    return retVal
  return getGroupsForUser( retVal[ 'Value' ] )

def __getGroupsWithAttr( attrName, value ):
  retVal = gConfig.getSections( "%s/Groups" % gBaseRegistrySection )
  if not retVal[ 'OK' ]:
    return retVal
  groupsList = retVal[ 'Value' ]
  groups = []
  for group in groupsList:
    if value in gConfig.getValue( "%s/Groups/%s/%s" % ( gBaseRegistrySection, group, attrName ), [] ):
      groups.append( group )
  if not groups:
    return S_ERROR( "No groups found for %s=%s" % ( attrName,value ) )
  groups.sort()
  return S_OK( groups )

def getGroupsForUser( username ):
  return __getGroupsWithAttr( 'Users', username )

def getGroupsForVO( vo ):
  if getVO():
    return gConfig.getSections( "%s/Groups" % gBaseRegistrySection )
  return __getGroupsWithAttr( 'VO', vo )

def getGroupsWithProperty( propName ):
  return __getGroupsWithAttr( "Properties", propName )

def getHostnameForDN( dn ):
  retVal = gConfig.getSections( "%s/Hosts" % gBaseRegistrySection )
  if not retVal[ 'OK' ]:
    return retVal
  hostList = retVal[ 'Value' ]
  for hostname in hostList:
    if dn in gConfig.getValue( "%s/Hosts/%s/DN" % ( gBaseRegistrySection, hostname ), [] ):
      return S_OK( hostname )
  return S_ERROR( "No hostname found for dn %s" % dn )

def getDefaultUserGroup():
  return gConfig.getValue( "/%s/DefaultGroup" % gBaseRegistrySection, "user" )

def findDefaultGroupForDN( dn ):
  result = getUsernameForDN( dn )
  if not result[ 'OK' ]:
    return result
  return findDefaultGroupForUser( result[ 'Value' ] )

def findDefaultGroupForUser( userName ):
  userDefGroups = getUserOption( userName, "DefaultGroup", [] )
  defGroups = userDefGroups + gConfig.getValue( "%s/DefaultGroup" % gBaseRegistrySection, [ "user" ] )
  result = getGroupsForUser( userName )
  if not result[ 'OK' ]:
    return result
  userGroups = result[ 'Value' ]
  for group in defGroups:
    if group in userGroups:
      return S_OK( group )
  if userGroups:
    return S_OK( userGroups[0] )
  return S_ERROR( "User %s has no groups" % userName )

def getAllUsers():
  retVal = gConfig.getSections( "%s/Users" % gBaseRegistrySection )
  if not retVal[ 'OK' ]:
    return []
  return retVal[ 'Value' ]

def getAllGroups():
  retVal = gConfig.getSections( "%s/Groups" % gBaseRegistrySection )
  if not retVal[ 'OK' ]:
    return []
  return retVal[ 'Value' ]

def getUsersInGroup( groupName, defaultValue = None ):
  if defaultValue == None:
    defaultValue = []
  option = "%s/Groups/%s/Users" % ( gBaseRegistrySection, groupName )
  return gConfig.getValue( option, defaultValue )

def getDNsInGroup( groupName ):
  DNs = []
  for user in getUsersInGroup( groupName ):
    result = getDNForUsername( user )
    if result[ 'OK' ]:
      DNs.extend( result[ 'Value' ] )
  return DNs

def getPropertiesForGroup( groupName, defaultValue = None ):
  if defaultValue == None:
    defaultValue = []
  option = "%s/Groups/%s/Properties" % ( gBaseRegistrySection, groupName )
  return gConfig.getValue( option, defaultValue )

def getPropertiesForHost( hostName, defaultValue = None ):
  if defaultValue == None:
    defaultValue = []
  option = "%s/Hosts/%s/Properties" % ( gBaseRegistrySection, hostName )
  return gConfig.getValue( option, defaultValue )

def getPropertiesForEntity( group, name = "", dn = "", defaultValue = None ):
  if defaultValue == None:
    defaultValue = []
  if group == 'hosts':
    if not name:
      result = getHostnameForDN( dn )
      if not result[ 'OK' ]:
        return defaultValue
      name = result[ 'Value' ]
    return getPropertiesForHost( name, defaultValue )
  else:
    return getPropertiesForGroup( group, defaultValue )

def __matchProps( sProps, rProps ):
  foundProps = []
  for prop in sProps:
    if prop in rProps:
      foundProps.append( prop )
  return foundProps

def groupHasProperties( groupName, propList ):
  if type( propList ) in types.StringTypes:
    propList = [ propList ]
  return __matchProps( propList, getPropertiesForGroup( groupName ) )

def hostHasProperties( hostName, propList ):
  if type( propList ) in types.StringTypes:
    propList = [ propList ]
  return __matchProps( propList, getPropertiesForHost( hostName ) )

def getUserOption( userName, optName, defaultValue = "" ):
  return gConfig.getValue( "%s/Users/%s/%s" % ( gBaseRegistrySection, userName, optName ), defaultValue )

def getGroupOption( groupName, optName, defaultValue = "" ):
  return gConfig.getValue( "%s/Groups/%s/%s" % ( gBaseRegistrySection, groupName, optName ), defaultValue )

def getHostOption( hostName, optName, defaultValue = "" ):
  return gConfig.getValue( "%s/Hosts/%s/%s" % ( gBaseRegistrySection, hostName, optName ), defaultValue )

def getHosts():
  return gConfig.getSections( '%s/Hosts' % gBaseRegistrySection )

def getVOOption( voName, optName, defaultValue = "" ):
  return gConfig.getValue( "%s/VO/%s/%s" % ( gBaseRegistrySection, voName, optName ), defaultValue )


def getBannedIPs():
  return gConfig.getValue( "%s/BannedIPs" % gBaseRegistrySection, [] )

def getVOForGroup( group ):
  voName = getVO()
  if voName:
    return voName
  return gConfig.getValue( "%s/Groups/%s/VO" % ( gBaseRegistrySection, group ), "" )

def getDefaultVOMSAttribute():
  return gConfig.getValue( "%s/DefaultVOMSAttribute" % gBaseRegistrySection, "" )

def getVOMSAttributeForGroup( group ):
  return gConfig.getValue( "%s/Groups/%s/VOMSRole" % ( gBaseRegistrySection, group ), getDefaultVOMSAttribute() )

def getDefaultVOMSVO():
  vomsVO = gConfig.getValue( "%s/DefaultVOMSVO" % gBaseRegistrySection, "" )
  if vomsVO:
    return vomsVO
  return getVO()

def getVOMSVOForGroup( group ):
  vomsVO = gConfig.getValue( "%s/Groups/%s/VOMSVO" % ( gBaseRegistrySection, group ), getDefaultVOMSVO() )
  if not vomsVO:
    vo = getVOForGroup( group )
    vomsVO = getVOOption( vo, 'VOMSName', '' )
  return vomsVO

def getGroupsWithVOMSAttribute( vomsAttr ):
  retVal = gConfig.getSections( "%s/Groups" % ( gBaseRegistrySection ) )
  if not retVal[ 'OK' ]:
    return []
  groups = []
  for group in retVal[ 'Value' ]:
    if vomsAttr == gConfig.getValue( "%s/Groups/%s/VOMSRole" % ( gBaseRegistrySection, group ), "" ):
      groups.append( group )
  return groups

def getVOs():
  """ Get all the configured VOs
  """
  voName = getVO()
  if voName:
    return S_OK([ voName] )
  return gConfig.getSections( '%s/VO' % gBaseRegistrySection )

def getVOMSServerInfo( requestedVO = '' ):
  """ Get information on VOMS servers for the given VO or for all of them
  """
  vomsDict = {}
  # For backward compatibility check the VOMS section first
  result = gConfig.getSections( '%s/VOMS/Servers' % gBaseRegistrySection )
  if result['OK']:
    voNames = result['Value']
    for vo in voNames:
      if requestedVO and vo != requestedVO:
        continue
      vomsDict.setdefault( vo, {} )
      vomsDict[vo]['VOMSName'] = vo
      result = gConfig.getSections( '%s/VOMS/Servers/%s' % (gBaseRegistrySection, vo) )
      if result['OK']:
        serverList = result['Value']
        vomsDict[vo].setdefault( "Servers", {} )
        for server in serverList:
          DN = gConfig.getValue( '%s/VOMS/Servers/%s/%s/DN' % (gBaseRegistrySection, vo, server), '' )
          CA = gConfig.getValue( '%s/VOMS/Servers/%s/%s/CA' % (gBaseRegistrySection, vo, server), '' )
          port = gConfig.getValue( '%s/VOMS/Servers/%s/%s/Port' % (gBaseRegistrySection, vo, server), 0 )
          vomsDict[vo]['Servers'].setdefault( server, {} )
          vomsDict[vo]['Servers'][server]['DN'] = DN
          vomsDict[vo]['Servers'][server]['CA'] = CA
          vomsDict[vo]['Servers'][server]['Port'] = port
  
  result = getVOs()         
  if result['OK']:
    voNames = result['Value']
    for vo in voNames:
      if requestedVO and vo != requestedVO:
        continue
      vomsName = getVOOption( vo, 'VOMSName', '' )
      if not vomsName:
        continue
      vomsDict.setdefault( vo, {} )
      vomsDict[vo]['VOMSName'] = getVOOption( vo, 'VOMSName', '' )
      result = gConfig.getSections( '%s/VO/%s/VOMSServers' % (gBaseRegistrySection, vo) )
      if result['OK']:
        serverList = result['Value']
        vomsDict[vo].setdefault( "Servers", {} )
        for server in serverList:
          vomsDict[vo]['Servers'].setdefault( server, {} )
          DN = gConfig.getValue( '%s/VO/%s/VOMSServers/%s/DN' % (gBaseRegistrySection, vo, server), '' )
          CA = gConfig.getValue( '%s/VO/%s/VOMSServers/%s/CA' % (gBaseRegistrySection, vo, server), '' )
          port = gConfig.getValue( '%s/VO/%s/VOMSServers/%s/Port' % (gBaseRegistrySection, vo, server), 0 )
          vomsDict[vo]['Servers'][server]['DN'] = DN
          vomsDict[vo]['Servers'][server]['CA'] = CA
          vomsDict[vo]['Servers'][server]['Port'] = port   

  return S_OK( vomsDict )     
      