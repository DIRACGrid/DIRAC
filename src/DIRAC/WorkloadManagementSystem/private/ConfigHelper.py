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
    if not pilotDN:
        pilotUser = opsHelper.getValue("Pilot/GenericPilotUser", "")
        if pilotUser:
            result = Registry.getDNForUsername(pilotUser)
            if result["OK"]:
                pilotDN = result["Value"][0]
    if pilotDN:
        gLogger.verbose(f"Pilot credentials: {pilotDN}@")
        result = gProxyManager.userHasProxy(pilotDN, 86400)
        if not result["OK"]:
            return S_ERROR(f"{pilotDN} has no proxy in ProxyManager")
        return S_OK((pilotDN,))

    return S_ERROR(f"No pilot DN")
