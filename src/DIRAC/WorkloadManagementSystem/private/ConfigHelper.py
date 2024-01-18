""" A set of utilities for getting configuration information for the WMS components
"""
from DIRAC import S_ERROR, S_OK, gLogger
from DIRAC.ConfigurationSystem.Client.Helpers import Operations, Registry
from DIRAC.FrameworkSystem.Client.ProxyManagerClient import gProxyManager


def findGenericPilotCredentials(vo=False, pilotDN=""):
    """Looks into the Operations/<>/Pilot section of CS to find the pilot credentials.
    Then check if the user has a registered proxy in ProxyManager.

    if pilotDN are specified, use them

    :param str vo: VO name
    :param str pilotDN: pilot DN

    :return: S_OK(tuple)/S_ERROR()
    """
    if not vo:
        return S_ERROR("Need a VO to determine the Generic pilot credentials")
    opsHelper = Operations.Operations(vo=vo)
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
            return S_ERROR(f"{pilotDN}@{pilotGroup} has no proxy in ProxyManager")
        return S_OK((pilotDN, pilotGroup))

    if pilotDN:
        return S_ERROR(f"DN {pilotDN} does not have group {pilotGroup}")
    return S_ERROR(f"No generic proxy in the Proxy Manager with groups {pilotGroup}")
