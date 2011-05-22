# $HeadURL$
__RCSID__ = "$Id$"

from DIRAC.ConfigurationSystem.Client.Config                 import gConfig
from DIRAC.ConfigurationSystem.Client.Helpers.Registry       import gBaseSecuritySection
from DIRAC.ConfigurationSystem.Client.Helpers.Registry       import getDefaultUserGroup, getDefaultVOMSAttribute, getDefaultVOMSVO
from DIRAC.ConfigurationSystem.Client.Helpers.Registry       import getUsernameForDN, getGroupsForDN, getHostnameForDN, getDNForUsername
from DIRAC.ConfigurationSystem.Client.Helpers.Registry       import getAllUsers, getGroupsForUser, getUsersInGroup
from DIRAC.ConfigurationSystem.Client.Helpers.Registry       import getPropertiesForGroup, getPropertiesForHost, getPropertiesForEntity
from DIRAC.ConfigurationSystem.Client.Helpers.Registry       import getVOMSAttributeForGroup, getVOMSVOForGroup, getGroupsWithVOMSAttribute
from DIRAC.ConfigurationSystem.Client.Helpers.Registry       import getBannedIPs

def skipCACheck():
  return gConfig.getValue( "/DIRAC/Security/SkipCAChecks", "false" ).lower() in ( "y", "yes", "true" )

def useServerCertificate():
  return gConfig.getValue( "/DIRAC/Security/UseServerCertificate", "false" ).lower() in ( "y", "yes", "true" )
