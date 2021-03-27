""" A set of utilities for getting configuration information for the WMS components
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.FrameworkSystem.Client.ProxyManagerClient import gProxyManager
from DIRAC.ConfigurationSystem.Client.Helpers import Registry, Operations


def findGenericPilotCredentials(vo=False, group=False, pilotDN='', pilotGroup='', pilotUser=None):
  """ Looks into the Operations/<>/Pilot section of CS to find the pilot credentials.
      Then check if the user has a registered proxy in ProxyManager.

      if pilotDN or pilotGroup are specified, use them

      :param str vo: VO name
      :param str group: group name
      :param str pilotDN: pilot DN
      :param str pilotGroup: pilot group
      :param str pilotUser: pilot user

      :return: S_OK(tuple)/S_ERROR()
  """
  if not group and not vo:
    return S_ERROR("Need a group or a VO to determine the Generic pilot credentials")
  vo = vo or Registry.getVOForGroup(group)
  if not vo:
    return S_ERROR("Group %s does not have a VO associated" % group)
  opsHelper = Operations.Operations(vo=vo)
  pilotDN = pilotDN or opsHelper.getValue("Pilot/GenericPilotDN", "")
  pilotUser = pilotUser or opsHelper.getValue("Pilot/GenericPilotUser", "")
  pilotGroup = pilotGroup or opsHelper.getValue("Pilot/GenericPilotGroup", "")
  if not pilotGroup:
    return S_ERROR("%s does not have group" % pilotDN or pilotUser)
  if pilotUser and not pilotDN:
    result = Registry.getDNForUsernameInGroup(pilotUser, pilotGroup)
    if not result['OK']:
      return result
    pilotDN = result['Value']
  if pilotDN and not pilotUser:
    result = Registry.getUsernameForDN(pilotDN)
    if not result['OK']:
      return result
    pilotUser = result['Value']

  gLogger.verbose("Pilot credentials: %s@%s (%s)" % (pilotUser, pilotGroup, pilotDN))
  result = gProxyManager.userHasProxy(pilotUser, pilotGroup, 86400)
  if not result['OK']:
    return S_ERROR("%s@%s has no proxy in ProxyManager" % (pilotUser, pilotGroup))
  return S_OK((pilotUser, pilotGroup, pilotDN))
