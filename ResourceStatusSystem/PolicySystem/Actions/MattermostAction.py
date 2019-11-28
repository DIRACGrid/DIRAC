""" MattermostAction

  This action posts the status change to a Mattermost channel
"""

__RCSID__ = '$Id$'

import json
import requests
from DIRAC import S_ERROR, S_OK
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.ResourceStatusSystem.PolicySystem.Actions.BaseAction import BaseAction
from DIRAC.Core.Utilities.SiteSEMapping import getSitesForSE
from DIRAC.Core.Utilities.SiteCEMapping import getSiteForCE


class MattermostAction(BaseAction):
  '''
    Action that sends a brief Mattermost Message.
  '''

  def __init__(self, name, decisionParams, enforcementResult, singlePolicyResults,
               clients=None, url=None):

    super(MattermostAction, self).__init__(name, decisionParams, enforcementResult,
                                           singlePolicyResults, clients)
    if url is not None:
      self.url = url
    else:
      self.url = Operations().getValue('ResourceStatus/Config/Mattermost')

  def run(self):
    '''
      Checks it has the parameters it needs and tries to send an sms to the users
      that apply.
    '''

    if self.url is None:
      return S_ERROR('Mattermost URL not set')

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

    message = "*{name}* _{statusType}_ --> _{status}_ \n{reason}".format(name=name,
                                                                         statusType=statusType,
                                                                         status=status,
                                                                         reason=reason)
    return self.sendMattermostMessage(message)

  def sendMattermostMessage(self, message):
    """
    Sends a Mattermost message to self.url

    :param str message: text message to send
    """

    payload = {'text': message}
    response = requests.post(self.url, data=json.dumps(payload), headers={'Content-Type': 'application/json'})
    response.raise_for_status()
    return S_OK()
