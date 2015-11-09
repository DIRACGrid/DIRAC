# $HeadURL:  $
''' FTSstorageUnbanAction

'''

import json
import fts3.rest.client.easy as fts3

from DIRAC                                                      import S_ERROR, S_OK
from DIRAC.ResourceStatusSystem.PolicySystem.Actions.BaseAction import BaseAction
from DIRAC.Core.Security.ProxyInfo                              import getProxyInfo
from DIRAC.ConfigurationSystem.Client.Helpers.Resources         import getFTS3Servers

__RCSID__ = '$Id:  $'

class FTSStorageUnbanAction( BaseAction ):
  '''
    Action that sends to the FTS server a unbanning request for a given list of sites.
  '''

  def __init__( self, name, decisionParams, enforcementResult, singlePolicyResults, clients ):

    super( FTSStorageUnbanAction, self ).__init__( name, decisionParams, enforcementResult,
                                         singlePolicyResults, clients )


    # enforcementResult supposed to look like:
    # {
    #   'Status'        : <str>,
    #   'Reason'        : <str>,
    #   'PolicyActions' : <list>,
    #   [ 'EndDate' : <str> ]
    # }

    # decisionParams supposed to look like:
    # {
    #   'element'     : None,
    #   'name'        : None,
    #   'elementType' : None,
    #   'statusType'  : None,
    #   'status'      : None,
    #   'reason'      : None,
    #   'tokenOwner'  : None
    # }

  def run( self ):
    '''
      Checks it has the parameters it needs and tries to unban the site.
    '''
    storageElement = self.decisionParams[ 'name' ]
    elementType = self.decisionParams[ 'elementType' ]
    if elementType != 'StorageElement':
      return S_ERROR( "'elementType' should be 'StorageElement'" )

    return self._unbanStorageElement( storageElement )


  def _unbanStorageElement( self, storageElement ):

    endpoints = getFTS3Servers()[ 'Value' ]

    blacklist = {}
    for endpoint in endpoints:
      # endpoint = 'https://fts3-pilot.cern.ch:8446'

      # TODO: maybe proxyPath is not needed since it is picked from the environment by the REST API
      proxyPath = getProxyInfo()
      if not proxyPath['OK']:
        return proxyPath

      try:
        proxyPath = proxyPath['Value']['path']
      except Exception as e:
        return S_ERROR( e.message )

      context = fts3.Context( endpoint, proxyPath )

      fts3.unban_se( context, storageElement )

      blacklist[endpoint] = json.loads( context.get( "ban/se" ) )

    return S_OK( blacklist )


################################################################################
# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
