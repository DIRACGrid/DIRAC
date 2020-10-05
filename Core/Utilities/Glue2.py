"""Module collecting functions dealing with the GLUE2 information schema

:author: A.Sailer

Known problems:

 * ARC CEs do not seem to publish wall or CPU time per queue anywhere
 * There is no consistency between which memory information is provided where,
   execution environment vs. information for a share
 * Some execution environment IDs are used more than once

Print outs with "SCHEMA PROBLEM" point -- in my opinion -- to errors in the
published information, like a foreign key pointing to non-existent entry.

"""

from pprint import pformat

from DIRAC import gLogger
from DIRAC.Core.Utilities.ReturnValues import S_OK, S_ERROR

__RCSID__ = "$Id$"


def __ldapsearchBDII(*args, **kwargs):
  """ wrap `DIRAC.Core.Utilities.Grid.ldapsearchBDII` to avoid circular import """
  from DIRAC.Core.Utilities.Grid import ldapsearchBDII
  return ldapsearchBDII(*args, **kwargs)


def getGlue2CEInfo(vo, host):
  """ call ldap for GLUE2 and get information

  :param str vo: Virtual Organisation
  :param str host: host to query for information
  :returns: result structure with result['Value'][siteID]['CEs'][ceID]['Queues'][queueName]. For
               each siteID, ceID, queueName all the GLUE2 parameters are retrieved
  """

  # get all Policies allowing given VO
  filt = "(&(objectClass=GLUE2Policy)(|(GLUE2PolicyRule=VO:%s)(GLUE2PolicyRule=vo:%s)))" % (vo, vo)
  polRes = __ldapsearchBDII(filt=filt, attr=None, host=host, base="o=glue", selectionString="GLUE2")

  if not polRes['OK']:
    return S_ERROR("Failed to get policies for this VO")
  polRes = polRes['Value']

  gLogger.notice("Found %s policies for this VO %s" % (len(polRes), vo))
  # get all shares for this policy
  # create an or'ed list of all the shares and then call the search
  listOfSitesWithPolicies = set()
  shareFilter = ''
  for policyValues in polRes:
    if 'GLUE2DomainID' not in policyValues['attr']['dn']:
      continue
    shareID = policyValues['attr'].get('GLUE2MappingPolicyShareForeignKey', None)
    policyID = policyValues['attr']['GLUE2PolicyID']
    siteName = policyValues['attr']['dn'].split('GLUE2DomainID=')[1].split(',', 1)[0]
    listOfSitesWithPolicies.add(siteName)
    if shareID is None:  # policy not pointing to ComputingInformation
      gLogger.debug("Policy %s does not point to computing information" % (policyID,))
      continue
    gLogger.verbose("%s policy %s pointing to %s " % (siteName, policyID, shareID))
    gLogger.debug("Policy values:\n%s" % pformat(policyValues))
    shareFilter += '(GLUE2ShareID=%s)' % shareID

  filt = '(&(objectClass=GLUE2Share)(|%s))' % shareFilter
  shareRes = __ldapsearchBDII(filt=filt, attr=None, host=host, base="o=glue", selectionString="GLUE2")
  if not shareRes['OK']:
    gLogger.error("Could not get share information", shareRes['Message'])
    return shareRes
  shareInfoLists = {}
  for shareInfo in shareRes['Value']:
    if 'GLUE2DomainID' not in shareInfo['attr']['dn']:
      continue
    if 'GLUE2ComputingShare' not in shareInfo['objectClass']:
      gLogger.debug('Share %r is not a ComputingShare: \n%s' % (shareID, pformat(shareInfo)))
      continue
    gLogger.debug("Found computing share:\n%s" % pformat(shareInfo))
    siteName = shareInfo['attr']['dn'].split('GLUE2DomainID=')[1].split(',', 1)[0]
    shareInfoLists.setdefault(siteName, []).append(shareInfo['attr'])

  siteInfo = __getGlue2ShareInfo(host, shareInfoLists)
  if not siteInfo['OK']:
    gLogger.error("Could not get CE info for %s:" % shareID, siteInfo['Message'])
    return siteInfo
  siteDict = siteInfo['Value']
  gLogger.debug("Found Sites:\n%s" % pformat(siteDict))
  sitesWithoutShares = set(siteDict) - listOfSitesWithPolicies
  if sitesWithoutShares:
    gLogger.error("Found some sites without any shares", pformat(sitesWithoutShares))
  else:
    gLogger.notice("All good")
  return S_OK(siteDict)


def __getGlue2ShareInfo(host, shareInfoLists):
  """ get information from endpoints, which are the CE at a Site

  :param str host: BDII host to query
  :param dict shareInfoDict: dictionary of GLUE2 parameters belonging to the ComputingShare
  :returns: result structure S_OK/S_ERROR
  """
  executionEnvironments = []
  for _siteName, shareInfoDicts in shareInfoLists.items():
    for shareInfoDict in shareInfoDicts:
      executionEnvironment = shareInfoDict['GLUE2ComputingShareExecutionEnvironmentForeignKey']
      if isinstance(executionEnvironment, basestring):
        executionEnvironment = [executionEnvironment]
      executionEnvironments.extend(executionEnvironment)
  resExeInfo = __getGlue2ExecutionEnvironmentInfo(host, executionEnvironments)
  if not resExeInfo['OK']:
    gLogger.error("SCHEMA PROBLEM: Cannot get execution environment info for %r" % str(executionEnvironments)[:100],
                  resExeInfo['Message'])
    return resExeInfo
  exeInfos = resExeInfo['Value']

  siteDict = {}
  for siteName, shareInfoDicts in shareInfoLists.items():
    siteDict[siteName] = {'CEs': {}}
    for shareInfoDict in shareInfoDicts:
      ceInfo = {}
      ceInfo['MaxWaitingJobs'] = shareInfoDict.get('GLUE2ComputingShareMaxWaitingJobs', '-1')  # This is not used
      ceInfo['Queues'] = {}
      queueInfo = {}
      queueInfo['GlueCEStateStatus'] = shareInfoDict['GLUE2ComputingShareServingState']
      queueInfo['GlueCEPolicyMaxCPUTime'] = str(int(shareInfoDict.get('GLUE2ComputingShareMaxCPUTime', 86400)) / 60)
      queueInfo['GlueCEPolicyMaxWallClockTime'] = str(int(shareInfoDict
                                                          .get('GLUE2ComputingShareMaxWallTime', 86400)) / 60)
      queueInfo['GlueCEInfoTotalCPUs'] = shareInfoDict.get('GLUE2ComputingShareMaxRunningJobs', '10000')
      queueInfo['GlueCECapability'] = ['CPUScalingReferenceSI00=2552']
      executionEnvironment = shareInfoDict['GLUE2ComputingShareExecutionEnvironmentForeignKey']
      if isinstance(executionEnvironment, basestring):
        executionEnvironment = [executionEnvironment]
      resExeInfo = __getGlue2ExecutionEnvironmentInfoForSite(siteName, executionEnvironment, exeInfos)
      if not resExeInfo['OK']:
        continue

      exeInfo = resExeInfo.get('Value')
      if not exeInfo:
        gLogger.warn('Did not find information for execution environment %s, using dummy values' % siteName)
        exeInfo = {'GlueHostMainMemoryRAMSize': '1999',  # intentionally identifiably dummy value
                   'GlueHostOperatingSystemVersion': '',
                   'GlueHostOperatingSystemName': '',
                   'GlueHostOperatingSystemRelease': '',
                   'GlueHostArchitecturePlatformType': 'x86_64',
                   'GlueHostBenchmarkSI00': '2500',  # needed for the queue to be used by the sitedirector
                   'MANAGER': '',
                   }

      # sometimes the time is still in hours
      maxCPUTime = int(queueInfo['GlueCEPolicyMaxCPUTime'])
      if maxCPUTime in [12, 24, 36, 48, 168]:
        queueInfo['GlueCEPolicyMaxCPUTime'] = str(maxCPUTime * 60)
        queueInfo['GlueCEPolicyMaxWallClockTime'] = str(int(queueInfo['GlueCEPolicyMaxWallClockTime']) * 60)

      ceInfo.update(exeInfo)
      shareEndpoints = shareInfoDict.get('GLUE2ShareEndpointForeignKey', [])
      if isinstance(shareEndpoints, basestring):
        shareEndpoints = [shareEndpoints]
      cesDict = {}
      for endpoint in shareEndpoints:
        ceType = endpoint.rsplit('.', 1)[1]
        # get queue Name, in CREAM this is behind GLUE2entityOtherInfo...
        if ceType == 'CREAM':
          for otherInfo in shareInfoDict['GLUE2EntityOtherInfo']:
            if otherInfo.startswith('CREAMCEId'):
              queueName = otherInfo.split('/', 1)[1]

        # cern HTCondorCE
        elif ceType.endswith('HTCondorCE'):
          ceType = 'HTCondorCE'
          queueName = 'condor'

        queueInfo['GlueCEImplementationName'] = ceType
        ceName = endpoint.split('_', 1)[0]
        cesDict.setdefault(ceName, {})
        existingQueues = dict(cesDict[ceName].get('Queues', {}))
        existingQueues[queueName] = queueInfo
        ceInfo['Queues'] = existingQueues
        cesDict[ceName].update(ceInfo)

      # ARC CEs do not have endpoints, we have to try something else to get the information about the queue etc.
      if not shareEndpoints and shareInfoDict['GLUE2ShareID'].startswith('urn:ogf'):
        exeInfo = dict(exeInfo)  # silence pylint about tuples
        queueInfo['GlueCEImplementationName'] = 'ARC'
        managerName = exeInfo.pop('MANAGER', '').split(' ', 1)[0].rsplit(':', 1)[1]
        managerName = managerName.capitalize() if managerName == 'condor' else managerName
        queueName = 'nordugrid-%s-%s' % (managerName, shareInfoDict['GLUE2ComputingShareMappingQueue'])
        ceName = shareInfoDict['GLUE2ShareID'].split('ComputingShare:')[1].split(':')[0]
        cesDict.setdefault(ceName, {})
        existingQueues = dict(cesDict[ceName].get('Queues', {}))
        existingQueues[queueName] = queueInfo
        ceInfo['Queues'] = existingQueues
        cesDict[ceName].update(ceInfo)

      siteDict[siteName]['CEs'].update(cesDict)

  return S_OK(siteDict)


def __getGlue2ExecutionEnvironmentInfo(host, executionEnvironments):
  """ get the information about OS version, architecture, memory from GLUE2 ExecutionEnvironment

  :param str host: BDII host to query
  :param list executionEnvironments: list of the execution environments to get some information from
  :returns: result structure with information in Glue1 schema to be consumed later elsewhere
  """
  exeFilter = ''
  for execEnv in executionEnvironments:
    exeFilter += '(GLUE2ResourceID=%s)' % execEnv
  filt = "(&(objectClass=GLUE2ExecutionEnvironment)(|%s))" % exeFilter
  response = __ldapsearchBDII(filt=filt, attr=None, host=host, base="o=glue", selectionString="GLUE2")
  if not response['OK']:
    return response
  if not response['Value']:
    return S_ERROR("No information found for %s" % executionEnvironments)
  return response

def __getGlue2ExecutionEnvironmentInfoForSite(sitename, foreignKeys, exeInfos):
  """Get the information about the execution environment for a specific site or ce or something."""
  # filter those that we want
  exeInfos = [exeInfo for exeInfo in exeInfos if exeInfo['attr']['GLUE2ResourceID'] in foreignKeys]
  # take the CE with the lowest MainMemory
  exeInfo = sorted(exeInfos, key=lambda k: int(k['attr']['GLUE2ExecutionEnvironmentMainMemorySize']))
  if not exeInfo:
    gLogger.error('Did not find execution info for', sitename)
    return S_OK()
  gLogger.debug("Found ExecutionEnvironments", pformat(exeInfo[0]))
  exeInfo = exeInfo[0]['attr']  # pylint: disable=unsubscriptable-object
  maxRam = exeInfo.get('GLUE2ExecutionEnvironmentMainMemorySize', '')
  architecture = exeInfo.get('GLUE2ExecutionEnvironmentPlatform', '')
  architecture = 'x86_64' if architecture == 'amd64' else architecture
  architecture = 'x86_64' if architecture == 'UNDEFINEDVALUE' else architecture
  architecture = 'x86_64' if "Intel(R) Xeon(R)" in architecture else architecture
  osFamily = exeInfo.get('GLUE2ExecutionEnvironmentOSFamily', '')  # e.g. linux
  osName = exeInfo.get('GLUE2ExecutionEnvironmentOSName', '')
  osVersion = exeInfo.get('GLUE2ExecutionEnvironmentOSVersion', '')
  manager = exeInfo.get('GLUE2ExecutionEnvironmentComputingManagerForeignKey', 'manager:unknownBatchSystem')
  # translate to Glue1 like keys, because that is used later on
  infoDict = {'GlueHostMainMemoryRAMSize': maxRam,
              'GlueHostOperatingSystemVersion': osName,
              'GlueHostOperatingSystemName': osFamily,
              'GlueHostOperatingSystemRelease': osVersion,
              'GlueHostArchitecturePlatformType': architecture.lower(),
              'GlueHostBenchmarkSI00': '2500',  # needed for the queue to be used by the sitedirector
              'MANAGER': manager,  # to create the ARC QueueName mostly
              }

  return S_OK(infoDict)
