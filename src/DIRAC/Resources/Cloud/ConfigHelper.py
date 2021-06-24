####################################################################
#
# Configuration related utilities
#
####################################################################

from __future__ import print_function
from __future__ import division
from __future__ import absolute_import

import six

from DIRAC import S_OK, S_ERROR, gLogger, gConfig
from DIRAC.ConfigurationSystem.Client.Helpers import Registry, Operations
from DIRAC.FrameworkSystem.Client.ProxyManagerClient import gProxyManager
from DIRAC.Core.Utilities.List import fromChar

__RCSID__ = "$Id$"


def findGenericCloudCredentials(vo=False, group=False):
  if not group and not vo:
    return S_ERROR("Need a group or a VO to determine the Generic cloud credentials")
  if not vo:
    vo = Registry.getVOForGroup(group)
    if not vo:
      return S_ERROR("Group %s does not have a VO associated" % group)
  opsHelper = Operations.Operations(vo=vo)
  cloudGroup = opsHelper.getValue("Cloud/GenericCloudGroup", "")
  cloudDN = opsHelper.getValue("Cloud/GenericCloudDN", "")
  if not cloudDN:
    cloudUser = opsHelper.getValue("Cloud/GenericCloudUser", "")
    if cloudUser:
      result = Registry.getDNForUsername(cloudUser)
      if result['OK']:
        cloudDN = result['Value'][0]
  if cloudDN and cloudGroup:
    gLogger.verbose("Cloud credentials from CS: %s@%s" % (cloudDN, cloudGroup))
    result = gProxyManager.userHasProxy(cloudDN, cloudGroup, 86400)
    if not result['OK']:
      return S_ERROR("%s@%s has no proxy in ProxyManager")
    return S_OK((cloudDN, cloudGroup))
  return S_ERROR("Cloud credentials not found")


def getVMTypes(siteList=None, ceList=None, vmTypeList=None, vo=None):
  """ Get CE/vmType options according to the specified selection
  """

  result = gConfig.getSections('/Resources/Sites')
  if not result['OK']:
    return result

  resultDict = {}

  grids = result['Value']
  for grid in grids:
    result = gConfig.getSections('/Resources/Sites/%s' % grid)
    if not result['OK']:
      continue
    sites = result['Value']
    for site in sites:
      if siteList is not None and site not in siteList:
        continue
      if vo:
        voList = gConfig.getValue('/Resources/Sites/%s/%s/VO' % (grid, site), [])
        if voList and vo not in voList:
          continue
      result = gConfig.getSections('/Resources/Sites/%s/%s/Cloud' % (grid, site))
      if not result['OK']:
        continue
      ces = result['Value']
      for ce in ces:
        if ceList is not None and ce not in ceList:
          continue
        if vo:
          voList = gConfig.getValue('/Resources/Sites/%s/%s/Cloud/%s/VO' % (grid, site, ce), [])
          if voList and vo not in voList:
            continue
        result = gConfig.getOptionsDict('/Resources/Sites/%s/%s/Cloud/%s' % (grid, site, ce))
        if not result['OK']:
          continue
        ceOptionsDict = result['Value']
        result = gConfig.getSections('/Resources/Sites/%s/%s/Cloud/%s/VMTypes' % (grid, site, ce))
        if not result['OK']:
          result = gConfig.getSections('/Resources/Sites/%s/%s/Cloud/%s/Images' % (grid, site, ce))
          if not result['OK']:
            return result
        vmTypes = result['Value']
        for vmType in vmTypes:
          if vmTypeList is not None and vmType not in vmTypeList:
            continue
          if vo:
            voList = gConfig.getValue('/Resources/Sites/%s/%s/Cloud/%s/VMTypes/%s/VO' % (grid, site, ce, vmType), [])
            if not voList:
              voList = gConfig.getValue('/Resources/Sites/%s/%s/Cloud/%s/Images/%s/VO' % (grid, site, ce, vmType), [])
            if voList and vo not in voList:
              continue
          resultDict.setdefault(site, {})
          resultDict[site].setdefault(ce, ceOptionsDict)
          resultDict[site][ce].setdefault('VMTypes', {})
          result = gConfig.getOptionsDict('/Resources/Sites/%s/%s/Cloud/%s/VMTypes/%s' % (grid, site, ce, vmType))
          if not result['OK']:
            result = gConfig.getOptionsDict('/Resources/Sites/%s/%s/Cloud/%s/Images/%s' % (grid, site, ce, vmType))
            if not result['OK']:
              continue
          vmTypeOptionsDict = result['Value']
          resultDict[site][ce]['VMTypes'][vmType] = vmTypeOptionsDict

  return S_OK(resultDict)


def getVMTypeConfig(site, ce='', vmtype=''):
  """ Get parameters of the specified queue
  """
  Tags = []
  grid = site.split('.')[0]
  if not ce:
    result = gConfig.getSections('/Resources/Sites/%s/%s/Cloud' % (grid, site))
    if not result['OK']:
      return result
    ceList = result['Value']
    if len(ceList) == 1:
      ce = ceList[0]
    else:
      return S_ERROR('No cloud endpoint specified')

  result = gConfig.getOptionsDict('/Resources/Sites/%s/%s/Cloud/%s' % (grid, site, ce))
  if not result['OK']:
    return result
  resultDict = result['Value']
  ceTags = resultDict.get('Tag')
  if ceTags:
    Tags = fromChar(ceTags)
  resultDict['CEName'] = ce

  if vmtype:
    result = gConfig.getOptionsDict('/Resources/Sites/%s/%s/Cloud/%s/VMTypes/%s' % (grid, site, ce, vmtype))
    if not result['OK']:
      return result
    resultDict.update(result['Value'])
    queueTags = resultDict.get('Tag')
    if queueTags:
      queueTags = fromChar(queueTags)
      Tags = list(set(Tags + queueTags))

  if Tags:
    resultDict['Tag'] = Tags
  resultDict['VMType'] = vmtype
  resultDict['Site'] = site
  return S_OK(resultDict)


def getPilotBootstrapParameters(vo='', runningPod=''):

  op = Operations.Operations(vo=vo)
  result = op.getOptionsDict('Cloud')
  opParameters = {}
  if result['OK']:
    opParameters = result['Value']
  opParameters['VO'] = vo
  opParameters['ReleaseProject'] = op.getValue('Cloud/ReleaseProject', 'DIRAC')
  opParameters['ReleaseVersion'] = op.getValue('Cloud/ReleaseVersion')
  opParameters['Setup'] = gConfig.getValue('/DIRAC/Setup', 'unknown')
  opParameters['SubmitPool'] = op.getValue('Cloud/SubmitPool')
  opParameters['CloudPilotCert'] = op.getValue('Cloud/CloudPilotCert')
  opParameters['CloudPilotKey'] = op.getValue('Cloud/CloudPilotKey')
  opParameters['pilotFileServer'] = op.getValue('Pilot/pilotFileServer')
  result = op.getOptionsDict('Cloud/%s' % runningPod)
  if result['OK']:
    opParameters.update(result['Value'])

  # Get standard pilot version now
  if 'Version' in opParameters:
    gLogger.warn("Cloud bootstrap version now uses standard Pilot/Version setting. "
                 "Please remove all obsolete (Cloud/Version) setting(s).")
  pilotVersions = op.getValue('Pilot/Version')
  if isinstance(pilotVersions, six.string_types):
    pilotVersions = [pilotVersions]
  if not pilotVersions:
    S_ERROR("Failed to get pilot version.")
  opParameters['Version'] = pilotVersions[0].strip()

  return S_OK(opParameters)
