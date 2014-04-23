# $HeadURL$
__RCSID__ = "$Id$"

from DIRAC.ConfigurationSystem.Client.Config                 import gConfig
from DIRAC.ConfigurationSystem.Client.Helpers.Registry       import getDefaultUserGroup
from DIRAC.ConfigurationSystem.Client.Helpers.Registry       import getDefaultVOMSAttribute, getDefaultVOMSVO, findDefaultGroupForDN
from DIRAC.ConfigurationSystem.Client.Helpers.Registry       import getUsernameForDN, getGroupsForDN, getHostnameForDN
from DIRAC.ConfigurationSystem.Client.Helpers.Registry       import getDNForUsername
from DIRAC.ConfigurationSystem.Client.Helpers.Registry       import getAllUsers, getGroupsForUser, getUsersInGroup
from DIRAC.ConfigurationSystem.Client.Helpers.Registry       import getPropertiesForGroup, getPropertiesForHost
from DIRAC.ConfigurationSystem.Client.Helpers.Registry       import getPropertiesForEntity
from DIRAC.ConfigurationSystem.Client.Helpers.Registry       import getVOMSAttributeForGroup, getVOMSVOForGroup
from DIRAC.ConfigurationSystem.Client.Helpers.Registry       import getGroupsWithVOMSAttribute
from DIRAC.ConfigurationSystem.Client.Helpers.Registry       import getBannedIPs

def skipCACheck():
  return gConfig.getValue( "/DIRAC/Security/SkipCAChecks", "false" ).lower() in ( "y", "yes", "true" )

def useServerCertificate():
  return gConfig.getValue( "/DIRAC/Security/UseServerCertificate", "false" ).lower() in ( "y", "yes", "true" )
