"""  The SiteSEMapping module performs the necessary CS gymnastics to
     resolve site and SE combinations.  These manipulations are necessary
     in several components.

     Assumes CS structure of: /Resources/Sites/<GRIDNAME>/<SITENAME>
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function


__RCSID__ = "$Id$"

from DIRAC import gLogger, S_OK
from DIRAC.DataManagementSystem.Utilities.DMSHelpers import DMSHelpers, siteGridName


def getSEParameters(seName):
  """ get all the SE parameters in a list

      :param str seName: name of the Storage Element

      :return: S_OK() with list of dict with parameters
  """
  # This import is here to avoid circular imports
  from DIRAC.Resources.Storage.StorageElement import StorageElement
  se = StorageElement(seName, hideExceptions=True)

  protocolsSet = set(se.localAccessProtocolList) | set(se.localWriteProtocolList)

  seParametersList = []
  for protocol in protocolsSet:
    seParameters = se.getStorageParameters(protocol=protocol)
    if seParameters['OK']:
      seParametersList.append(seParameters['Value'])
    else:
      gLogger.verbose("No SE parameters obtained", "for SE %s and protocol %s" % (seName, protocol))

  return S_OK(seParametersList)


def getSEHosts(seName):
  """ Get StorageElement host names (can be more than one depending on the protocol)

      :param str seName: name of the storage element

      :return: S_OK() with list of hosts or S_ERROR
  """

  seParameters = getSEParameters(seName)
  if not seParameters['OK']:
    gLogger.warn("Could not get SE parameters", "SE: %s" % seName)
    return seParameters

  return S_OK([parameters['Host'] for parameters in seParameters['Value']])


def getStorageElementsHosts(seNames=None):
  """ Get StorageElement host names

      :param list seNames: possible list of storage element names (if not provided, will use all)
      :param list plugins: if provided, restrict to a certain list of plugins

      :return: S_OK() with list of hosts or S_ERROR
  """

  seHosts = []

  if seNames is None:
    seNames = DMSHelpers().getStorageElements()

  for seName in seNames:

    try:
      seHost = getSEHosts(seName)
      if not seHost['OK']:
        gLogger.warn("Could not get SE Host", "SE: %s" % seName)
        continue
      if seHost['Value']:
        seHosts.extend(seHost['Value'])
    except Exception as excp:  # pylint: disable=broad-except
      gLogger.error("Failed to get SE %s information (SE skipped) " % seName)
      gLogger.exception("Operation finished  with exception: ", lException=excp)
  return S_OK(list(set(seHosts)))


#############################################################################
def getSiteSEMapping(gridName='', withSiteLocalSEMapping=False):
  """ Returns a dictionary of all sites and their localSEs as a list, e.g.
      {'LCG.CERN.ch':['CERN-RAW','CERN-RDST',...]}
      If gridName is specified, result is restricted to that Grid type.
  """
  result = DMSHelpers().getSiteSEMapping()
  if not result['OK']:
    return result
  if withSiteLocalSEMapping:
    mapping = result['Value'][2]
  else:
    mapping = result['Value'][1]
  if gridName:
    mapping = dict((site, mapping[site]) for site in mapping if siteGridName(site) == gridName)
  return S_OK(mapping)


#############################################################################
def getSESiteMapping(gridName='', withSiteLocalSEMapping=False):
  """ Returns a dictionary of all SEs and their associated site(s), e.g.
      {'CERN-RAW':'LCG.CERN.ch','CERN-RDST':'LCG.CERN.ch',...]}
      Although normally one site exists for a given SE, it is possible over all
      Grid types to have multiple entries.
      If gridName is specified, result is restricted to that Grid type.
      Assumes CS structure of: /Resources/Sites/<GRIDNAME>/<SITENAME>
  """
  storageElements = DMSHelpers().getStorageElements()
  return S_OK(dict((se,
                    getSitesForSE(se, gridName=gridName,
                                  withSiteLocalSEMapping=withSiteLocalSEMapping).get('Value', []))
                   for se in storageElements))

#############################################################################


def getSitesForSE(storageElement, gridName='', withSiteLocalSEMapping=False):
  """ Given a DIRAC SE name this method returns a list of corresponding sites.
      Optionally restrict to Grid specified by name.
  """

  result = DMSHelpers().getSitesForSE(storageElement,
                                      connectionLevel='DOWNLOAD' if withSiteLocalSEMapping else 'LOCAL')
  if not result['OK'] or not gridName:
    return result

  return S_OK([site for site in result['Value'] if siteGridName(site) == gridName])


#############################################################################
def getSEsForSite(siteName, withSiteLocalSEMapping=False):
  """ Given a DIRAC site name this method returns a list of corresponding SEs.
  """
  result = DMSHelpers().getSEsForSite(siteName, connectionLevel='DOWNLOAD' if withSiteLocalSEMapping else 'LOCAL')
  if not result['OK']:
    return S_OK([])
  return result

#############################################################################


def isSameSiteSE(se1, se2):
  """ Check if the 2 SEs are at the same site
  """
  dmsHelper = DMSHelpers()
  site1 = dmsHelper.getLocalSiteForSE(se1).get('Value')
  site2 = dmsHelper.getLocalSiteForSE(se2).get('Value')
  return site1 and site2 and site1 == site2

#############################################################################


def getSEsForCountry(country):
  """ Determines the associated SEs from the country code
  """
  return DMSHelpers().getSEsAtCountry(country)
