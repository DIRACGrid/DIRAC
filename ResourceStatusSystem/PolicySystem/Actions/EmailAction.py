''' EmailAction

  This action writes all the necessary data to a cache file ( cache.db ) that
  will be used later by the EmailAgent in order to send the emails for each site.

'''

__RCSID__ = '$Id$'

from DIRAC import S_ERROR
from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient import ResourceStatusClient
from DIRAC.ResourceStatusSystem.PolicySystem.Actions.BaseAction import BaseAction
from DIRAC.Core.Utilities.SiteSEMapping import getSitesForSE
from DIRAC.Core.Utilities.SiteCEMapping import getSiteForCE


class EmailAction(BaseAction):

  def __init__(self, name, decisionParams, enforcementResult, singlePolicyResults,
               clients=None):

    super(EmailAction, self).__init__(name, decisionParams, enforcementResult,
                                      singlePolicyResults, clients)

    if clients is not None and 'ResourceStatusClient' in clients:
      self.rsClient = clients['ResourceStatusClient']
    else:
      self.rsClient = ResourceStatusClient()

  def run(self):
    ''' Checks it has the parameters it needs and writes the date to a cache file.
    '''
    # Minor security checks

    element = self.decisionParams['element']
    if element is None:
      return S_ERROR('element should not be None')

    name = self.decisionParams['name']
    if name is None:
      return S_ERROR('name should not be None')

    statusType = self.decisionParams['statusType']
    if statusType is None:
      return S_ERROR('statusType should not be None')

    previousStatus = self.decisionParams['status']
    if previousStatus is None:
      return S_ERROR('status should not be None')

    status = self.enforcementResult['Status']
    if status is None:
      return S_ERROR('status should not be None')

    reason = self.enforcementResult['Reason']
    if reason is None:
      return S_ERROR('reason should not be None')

    if self.decisionParams['element'] == 'Site':
      siteName = self.decisionParams['name']
    else:
      elementType = self.decisionParams['elementType']

      if elementType == 'StorageElement':
        siteName = getSitesForSE(name)
      elif elementType == 'ComputingElement':
        siteName = getSiteForCE(name)
      else:
        siteName = {'OK': True, 'Value': 'Unassigned'}

      if not siteName['OK']:
        self.log.error('Resource %s does not exist at any site: %s' % (name, siteName['Message']))
        siteName = "Unassigned Resources"
      elif not siteName['Value']:
        siteName = "Unassigned Resources"
      else:
        siteName = siteName['Value'] if isinstance(siteName['Value'], basestring) else siteName['Value'][0]

    # create record for insertion
    recordDict = {}
    recordDict['SiteName'] = siteName
    recordDict['ResourceName'] = name
    recordDict['Status'] = status
    recordDict['PreviousStatus'] = previousStatus
    recordDict['StatusType'] = statusType

    return self.rsClient.insert('ResourceStatusCache', recordDict)
