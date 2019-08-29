"""Utilities to help Computing Element Queues manipulation
"""

__RCSID__ = '$Id$'

from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Utilities.List import fromChar
from DIRAC.Core.Utilities.ClassAd.ClassAdLight import ClassAd
from DIRAC.ConfigurationSystem.Client.Helpers.Resources import getDIRACPlatform
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.Core.Security.ProxyInfo import getProxyInfo

def getQueuesResolved(siteDict):
  """
  Get the list of queue descriptions merging site/ce/queue parameters and adding some
  derived parameters.

  :param dict siteDict: dictionary with configuration data as returned by Resources.getQueues() method

  :return: S_OK/S_ERROR, Value dictionary per queue with configuration data updated, e.g. for SiteDirector
  """

  queueDict = {}

  for site in siteDict:
    for ce in siteDict[site]:
      ceDict = siteDict[site][ce]
      qDict = ceDict.pop('Queues')
      for queue in qDict:
        queueName = '%s_%s' % (ce, queue)
        queueDict[queueName] = qDict[queue]
        queueDict[queueName] = qDict[queue]
        queueDict[queueName]['Queue'] = queue
        queueDict[queueName]['Site'] = site
        # Evaluate the CPU limit of the queue according to the Glue convention
        # To Do: should be a utility
        if "maxCPUTime" in queueDict[queueName] and \
           "SI00" in queueDict[queueName]:
          maxCPUTime = float(queueDict[queueName]['maxCPUTime'])
          # For some sites there are crazy values in the CS
          maxCPUTime = max(maxCPUTime, 0)
          maxCPUTime = min(maxCPUTime, 86400 * 12.5)
          si00 = float(queueDict[queueName]['SI00'])
          queueCPUTime = 60. / 250. * maxCPUTime * si00
          queueDict[queueName]['CPUTime'] = int(queueCPUTime)

        # Tags & RequiredTags defined on the Queue level and on the CE level are concatenated
        # This also converts them from a string to a list if required.
        for tagFieldName in ('Tag', 'RequiredTag'):
          ceTags = ceDict.get(tagFieldName, [])
          if isinstance(ceTags, basestring):
            ceTags = fromChar(ceTags)
          queueTags = queueDict[queueName].get(tagFieldName)
          if queueTags and isinstance(queueTags, basestring):
            queueTags = fromChar(queueTags)
            queueDict[queueName][tagFieldName] = queueTags
          if ceTags:
            if queueTags:
              allTags = list(set(ceTags + queueTags))
              queueDict[queueName][tagFieldName] = allTags
            else:
              queueDict[queueName][tagFieldName] = ceTags

        # Some parameters can be defined on the CE level and are inherited by all Queues
        for parameter in ['MaxRAM', 'NumberOfProcessors', 'WholeNode']:
          queueParameter = queueDict[queueName].get(parameter)
          ceParameter = ceDict.get(parameter)
          if ceParameter or queueParameter:
            queueDict[queueName][parameter] = ceParameter if not queueParameter \
                else queueParameter

        # If we have a multi-core queue add MultiProcessor tag
        if queueDict[queueName].get('NumberOfProcessors',1) > 1:
          queueDict[queueName].setdefault('Tag', []).append('MultiProcessor')

        queueDict[queueName]['CEName'] = ce
        queueDict[queueName]['GridCE'] = ce
        queueDict[queueName]['CEType'] = ceDict['CEType']
        queueDict[queueName]['GridMiddleware'] = ceDict['CEType']
        queueDict[queueName]['QueueName'] = queue

        platform = ''
        if "Platform" in queueDict[queueName]:
          platform = queueDict[queueName]['Platform']
        elif "Platform" in ceDict:
          platform = ceDict['Platform']
        elif "OS" in ceDict:
          architecture = ceDict.get('architecture', 'x86_64')
          platform = '_'.join([architecture, ceDict['OS']])

        queueDict[queueName]['Platform'] = platform
        if "Platform" not in queueDict[queueName] and platform:
          result = getDIRACPlatform(platform)
          if result['OK']:
            queueDict[queueName]['Platform'] = result['Value'][0]

  return S_OK(queueDict)

def matchQueue(jobJDL, queueDict, fullMatch = False):
  """
  Match the job description to the queue definition

  :param str job: JDL job description
  :param bool fullMatch: test matching on all the criteria
  :param dict queueDict: queue parameters dictionary

  :return: S_OK/S_ERROR, Value - result of matching, S_OK if matched or
           S_ERROR with the reason for no match
  """

  # Check the job description validity
  job = ClassAd(jobJDL)
  if not job.isOK():
    return S_ERROR('Invalid job description')

  noMatchReasons = []

  # Check job requirements to resource
  # 1. CPUTime
  cpuTime = job.getAttributeInt('CPUTime')
  if not cpuTime:
    cpuTime = 84600
  if cpuTime and cpuTime > queueDict.get('CPUTime', 0.):
    noMatchReasons.append('Job CPUTime requirement not satisfied')
    if not fullMatch:
      return S_OK({'Match': False, 'Reason': noMatchReasons[0]})

  # 2. Multi-value match requirements
  for parameter in ['Site', 'GridCE', 'Platform', 'GridMiddleware',
                    'PilotType', 'SubmitPool', 'JobType']:
    if parameter in queueDict:
      valueSet = set(job.getListFromExpression(parameter))
      if not valueSet:
        valueSet = set(job.getListFromExpression('%ss' % parameter))
      queueSet = set(fromChar(queueDict[parameter]))
      if valueSet and queueSet and not valueSet.intersection(queueSet):
        valueToPrint = ','.join(valueSet)
        if len(valueToPrint) > 20:
          valueToPrint = "%s..." % valueToPrint[:20]
        noMatchReasons.append('Job %s %s requirement not satisfied' % (parameter, valueToPrint))
        if not fullMatch:
          return S_OK({'Match': False, 'Reason': noMatchReasons[0]})

  # 3. Banned multi-value match requirements
  for par in ['Site', 'GridCE', 'Platform', 'GridMiddleware',
                    'PilotType', 'SubmitPool', 'JobType']:
    parameter = "Banned%s" % par
    if par in queueDict:
      valueSet = set(job.getListFromExpression(parameter))
      if not valueSet:
        valueSet = set(job.getListFromExpression('%ss' % parameter))
      queueSet = set(fromChar(queueDict[par]))
      if valueSet and queueSet and valueSet.issubset(queueSet):
        valueToPrint = ','.join(valueSet)
        if len(valueToPrint) > 20:
          valueToPrint = "%s..." % valueToPrint[:20]
        noMatchReasons.append('Job %s %s requirement not satisfied' % (parameter, valueToPrint))
        if not fullMatch:
          return S_OK({'Match': False, 'Reason': noMatchReasons[0]})

  # 4. Tags
  tags = set(job.getListFromExpression('Tag'))
  nProc = job.getAttributeInt('NumberOfProcessors')
  if nProc and nProc > 1:
    tags.add('MultiProcessor')
  wholeNode = job.getAttributeString('WholeNode')
  if wholeNode:
    tags.add('WholeNode')
  queueTags = set(queueDict.get('Tags',[]))
  if not tags.issubset(queueTags):
    noMatchReasons.append('Job Tag %s not satisfied' % ','.join(tags))
    if not fullMatch:
      return S_OK({'Match': False, 'Reason': noMatchReasons[0]})

  # 4. MultiProcessor requirements
  if nProc and nProc > int(queueDict.get('NumberOfProcessors', 1)):
    noMatchReasons.append('Job NumberOfProcessors %d requirement not satisfied' % nProc)
    if not fullMatch:
      return S_OK({'Match': False, 'Reason': noMatchReasons[0]})

  # 5. RAM
  ram = job.getAttributeInt('RAM')
  if ram and ram > int(queueDict['MaxRAM']):
    noMatchReasons.append('Job RAM %d requirement not satisfied' % ram)
    if not fullMatch:
      return S_OK({'Match': False, 'Reason': noMatchReasons[0]})

  # Check resource requirements to job
  # 1. OwnerGroup - rare case but still
  if "OwnerGroup" in queueDict:
    result = getProxyInfo(disableVOMS=True)
    if not result['OK']:
      return S_ERROR('No valid proxy available')
    ownerGroup = result['Value']['group']
    if ownerGroup != queueDict['OwnerGroup']:
      noMatchReasons.append('Resource OwnerGroup %s requirement not satisfied' % queueDict['OwnerGroup'])
      if not fullMatch:
        return S_OK({'Match': False, 'Reason': noMatchReasons[0]})

  # 2. Required tags
  requiredTags = set(queueDict.get('RequiredTags',[]))
  if not requiredTags.issubset(tags):
    noMatchReasons.append('Resource RequiredTags %s not satisfied' % ','.join(requiredTags))
    if not fullMatch:
      return S_OK({'Match': False, 'Reason': noMatchReasons[0]})

  # 3. RunningLimit
  site = queueDict['Site']
  opsHelper = Operations()
  result = opsHelper.getSections('JobScheduling/RunningLimit')
  if result['OK'] and site in result['Value']:
    result = opsHelper.getSections('JobScheduling/RunningLimit/%s' % site)
    if result['OK']:
      for parameter in result['Value']:
        value = job.getAttributeString(parameter)
        if value and opsHelper.getValue('JobScheduling/RunningLimit/%s/%s/%s' % (site, parameter, value), 1) == 0:
          noMatchReasons.append('Resource operational %s requirement not satisfied' % parameter)
          if not fullMatch:
            return S_OK({'Match': False, 'Reason': noMatchReasons[0]})

  if noMatchReasons:
    return S_OK({'Match': False, 'Reason': noMatchReasons})
  return S_OK({'Match': True, 'Reason': noMatchReasons})