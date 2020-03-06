''' EmailAction

  This action writes all the necessary data to a cache file ( cache.db ) that
  will be used later by the EmailAgent in order to send the emails for each site.

'''

__RCSID__ = '$Id$'

import six
import os
import sqlite3
from DIRAC import S_ERROR, S_OK
from DIRAC.ConfigurationSystem.Client.Helpers.Resources import getCESiteMapping
from DIRAC.Core.Utilities.SiteSEMapping import getSitesForSE
from DIRAC.ResourceStatusSystem.PolicySystem.Actions.BaseAction import BaseAction


class EmailAction(BaseAction):

  def __init__(self, name, decisionParams, enforcementResult, singlePolicyResults,
               clients=None):

    super(EmailAction, self).__init__(name, decisionParams, enforcementResult,
                                      singlePolicyResults, clients)

    if 'DIRAC' in os.environ:
      self.cacheFile = os.path.join(
          os.getenv('DIRAC'), 'work/ResourceStatus/cache.db')
    else:
      self.cacheFile = os.path.realpath('cache.db')

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
	res = getCESiteMapping(name)
	if not res['OK']:
	  return res
	siteName = S_OK(res['Value'][name])
      else:
        siteName = {'OK': True, 'Value': 'Unassigned'}

      if not siteName['OK']:
        self.log.error('Resource %s does not exist at any site: %s' % (name, siteName['Message']))
        siteName = "Unassigned Resources"
      elif not siteName['Value']:
        siteName = "Unassigned Resources"
      else:
        siteName = siteName['Value'] if isinstance(siteName['Value'], six.string_types) else siteName['Value'][0]

    with sqlite3.connect(self.cacheFile) as conn:

      try:
        conn.execute('''CREATE TABLE IF NOT EXISTS ResourceStatusCache(
                      SiteName VARCHAR(64) NOT NULL,
                      ResourceName VARCHAR(64) NOT NULL,
                      Status VARCHAR(8) NOT NULL DEFAULT "",
                      PreviousStatus VARCHAR(8) NOT NULL DEFAULT "",
                      StatusType VARCHAR(128) NOT NULL DEFAULT "all",
                      Time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                     );''')

        insertQuery = "INSERT INTO ResourceStatusCache (SiteName, ResourceName, Status, PreviousStatus, StatusType)"
        insertQuery += " VALUES ('%s', '%s', '%s', '%s', '%s' ); " % (siteName, name, status,
                                                                      previousStatus, statusType)
        conn.execute(insertQuery)

        conn.commit()

      except sqlite3.OperationalError:
        self.log.error('Email cache database is locked')

    return S_OK()
