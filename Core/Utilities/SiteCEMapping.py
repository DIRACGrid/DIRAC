"""  The SiteCEMapping module performs the necessary CS gymnastics to
     resolve site and CE combinations.  These manipulations are necessary
     in several components.

     Assumes CS structure of: /Resources/Sites/<GRIDNAME>/<SITENAME>
"""

__RCSID__ = "$Id$"

from DIRAC import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client.Helpers.Resources import getCESiteMapping


def getQueueInfo(ceUniqueID, diracSiteName=''):
  """
    Extract information from full CE Name including associate DIRAC Site
  """
  try:
    subClusterUniqueID = ceUniqueID.split('/')[0].split(':')[0]
    queueID = ceUniqueID.split('/')[1]
  except BaseException:
    return S_ERROR('Wrong full queue Name')

  if not diracSiteName:
    gLogger.debug("SiteName not given, looking in /LocaSite/Site")
    diracSiteName = gConfig.getValue('/LocalSite/Site', '')

    if not diracSiteName:
      gLogger.debug("Can't find LocalSite name, looking in CS")
      result = getCESiteMapping(subClusterUniqueID)
      if not result['OK']:
        return result
      diracSiteName = result['Value'][subClusterUniqueID]

      if not diracSiteName:
        gLogger.error('Can not find corresponding Site in CS')
        return S_ERROR('Can not find corresponding Site in CS')

  gridType = diracSiteName.split('.')[0]

  siteCSSEction = '/Resources/Sites/%s/%s/CEs/%s' % (gridType, diracSiteName, subClusterUniqueID)
  queueCSSection = '%s/Queues/%s' % (siteCSSEction, queueID)

  resultDict = {'SubClusterUniqueID': subClusterUniqueID,
                'QueueID': queueID,
                'SiteName': diracSiteName,
                'Grid': gridType,
                'SiteCSSEction': siteCSSEction,
                'QueueCSSection': queueCSSection}

  return S_OK(resultDict)
