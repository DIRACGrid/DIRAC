'''
PropagationCommand module
This command gets all the elements that exist under a given site and applies the following logic:
if even one element is 'Active' for the given site then it marks the site as 'Active', if all elements
are set in a different status (like banned or error) then it marks the site as 'Banned'
'''
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = '$Id$'

from DIRAC import S_OK, S_ERROR
from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient import ResourceStatusClient
from DIRAC.ResourceStatusSystem.Command.Command import Command
from DIRAC.ResourceStatusSystem.Utilities import CSHelpers


class PropagationCommand(Command):

  def __init__(self, args=None, clients=None):

    self.rssClient = ResourceStatusClient()
    super(PropagationCommand, self).__init__(args, clients)

  def doNew(self, masterParams=None):
    return S_OK()

  def doCache(self):

    if not self.args['site']:
      return S_ERROR('site was not found in args')

    site = self.args['site']

    elements = CSHelpers.getSiteElements(site)

    statusList = []

    if elements['OK']:
      for element in elements['Value']:
        status = self.rssClient.selectStatusElement("Resource", "Status", element, meta={'columns': ['Status']})
        if not status['OK']:
          return status

        if status['Value']:
          statusList.append(status['Value'][0][0])
        else:  # forcing in the case the resource has no status (yet)
          statusList.append('Active')

      if 'Active' in statusList:
        return S_OK({'Status': 'Active', 'Reason': 'An element that belongs to the site is Active'})

      if 'Degraded' in statusList:
        return S_OK({'Status': 'Degraded', 'Reason': 'An element that belongs to the site is Degraded'})

    return S_OK({'Status': 'Banned', 'Reason': 'There is no Active element in the site'})

  def doMaster(self):
    return S_OK()
