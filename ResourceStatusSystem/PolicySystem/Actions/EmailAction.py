''' Action that bulk writes data that will be sent by email later using EmailAgent

'''

import os
import json
from datetime import datetime
from DIRAC                                                      import gConfig, S_ERROR, S_OK
from DIRAC.Interfaces.API.DiracAdmin                            import DiracAdmin
from DIRAC.ResourceStatusSystem.PolicySystem.Actions.BaseAction import BaseAction
from DIRAC.Core.Utilities.SiteSEMapping                         import getSitesForSE

__RCSID__ = '$Id:  $'

class EmailAction( BaseAction ):
  ''' Action that sends an email with the information concerning the status and the policies run.
  '''

  def __init__( self, name, decisionParams, enforcementResult, singlePolicyResults,
                clients = None ):

    super( EmailAction, self ).__init__( name, decisionParams, enforcementResult,
                                         singlePolicyResults, clients )
    self.diracAdmin = DiracAdmin()

    self.default_value = '/opt/dirac/pro/work/ResourceStatus/'
    self.dirac_path = os.getenv('DIRAC', self.default_value)
    self.cacheFile = self.dirac_path + 'work/ResourceStatus/' + 'cache.json'

  def run( self ):
    ''' Checks it has the parameters it needs and tries to send an email to the users that apply.
    '''
    # Minor security checks

    element = self.decisionParams[ 'element' ]
    if element is None:
      return S_ERROR( 'element should not be None' )

    name = self.decisionParams[ 'name' ]
    if name is None:
      return S_ERROR( 'name should not be None' )

    statusType = self.decisionParams[ 'statusType' ]
    if statusType is None:
      return S_ERROR( 'statusType should not be None' )

    previousStatus = self.decisionParams[ 'status' ]
    if previousStatus is None:
      return S_ERROR( 'status should not be None' )

    status = self.enforcementResult[ 'Status' ]
    if status is None:
      return S_ERROR( 'status should not be None' )

    reason = self.enforcementResult[ 'Reason' ]
    if reason is None:
      return S_ERROR( 'reason should not be None' )

    siteName = getSitesForSE(name)['Value'][0]
    time     = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    dict    = { 'name': name, 'statusType': statusType, 'status': status, 'time': time, 'previousStatus': previousStatus }

    actionResult = self._addtoJSON(self.cacheFile, siteName, dict)

    #returns S_OK() if the record was added successfully using addtoJSON
    return actionResult


  def _addtoJSON(self, cache_file, siteName, record):
    ''' Adds a record of a banned element to a local JSON file grouped by site name.
    '''

    try:

      if (not os.path.isfile(cache_file)) or (os.stat(cache_file).st_size == 0):
        #if the file is empty or it does not exist create it and write the first element of the group
        with open(cache_file, 'w') as f:
          json.dump({ siteName: [record] }, f)

      else:
        #otherwise load the file
        with open(cache_file) as f:
          new_dict = json.load(f)

        #if the site's name is in there just append the group
        if siteName in new_dict:
          new_dict[siteName].append(record)
        else:
          #if it is not there, create a new group
          new_dict.update( { siteName: [record] } )

        #write the file again with the modified contents
        with open(cache_file, 'w') as f:
          json.dump(new_dict, f)

      return S_OK()

    except ValueError:
      return S_ERROR("Could not add site to cache file, " + ValueError)

  def _deleteCacheFile(self, cache_file):
    ''' Deletes the cache file
    '''

    try:
      os.remove(cache_file)
      return S_OK()
    except OSError:
      return S_ERROR("Could not delete the cache file")

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
