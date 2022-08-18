""" A set of utilities for getting configuration information for the WMS components
"""
from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.FrameworkSystem.Client.ProxyManagerClient import gProxyManager
from DIRAC.ConfigurationSystem.Client.Helpers import Registry, Operations


def findGenericPilotCredentials(vo=False, group=False, pilotDN="", pilotGroup=""):
    """Looks into the Operations/<>/Pilot section of CS to find the pilot credentials.
    Then check if the user has a registered proxy in ProxyManager.

    if pilotDN or pilotGroup are specified, use them

    :param str vo: VO name
    :param str group: group name
    :param str pilotDN: pilot DN
    :param str pilotGroup: pilot group

    :return: S_OK(tuple)/S_ERROR()
    """
    if not group and not vo:
        return S_ERROR("Need a group or a VO to determine the Generic pilot credentials")
    if not vo:
        vo = Registry.getVOForGroup(group)
        if not vo:
            return S_ERROR("Group %s does not have a VO associated" % group)
    opsHelper = Operations.Operations(vo=vo)
    if not pilotGroup:
        pilotGroup = opsHelper.getValue("Pilot/GenericPilotGroup", "")
    if not pilotDN:
        pilotDN = opsHelper.getValue("Pilot/GenericPilotDN", "")
    if not pilotDN:
        pilotUser = opsHelper.getValue("Pilot/GenericPilotUser", "")
        if pilotUser:
            result = Registry.getDNForUsername(pilotUser)
            if result["OK"]:
                pilotDN = result["Value"][0]
    if pilotDN and pilotGroup:
        gLogger.verbose(f"Pilot credentials: {pilotDN}@{pilotGroup}")
        result = gProxyManager.userHasProxy(pilotDN, pilotGroup, 86400)
        if not result["OK"]:
            return S_ERROR("%s@%s has no proxy in ProxyManager")
        return S_OK((pilotDN, pilotGroup))

    if pilotDN:
        return S_ERROR(f"DN {pilotDN} does not have group {pilotGroup}")
    return S_ERROR("No generic proxy in the Proxy Manager with groups %s" % pilotGroup)
