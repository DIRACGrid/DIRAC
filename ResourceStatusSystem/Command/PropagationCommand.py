''' PropagationCommand module
'''

from DIRAC                                                      import S_OK, S_ERROR
from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient     import ResourceStatusClient
from DIRAC.ResourceStatusSystem.Command.Command                 import Command
from DIRAC.ResourceStatusSystem.Utilities                       import CSHelpers

__RCSID__ = '$Id:  $'

class PropagationCommand( Command ):

  def __init__( self, args = None, clients = None ):

    self.rssClient = ResourceStatusClient()
    super( PropagationCommand, self ).__init__( args, clients )

  def doNew( self, masterParams = None ):
     return S_OK()

  def doCache( self ):

    if not self.args['site']:
      return S_ERROR('site was not found in args')

    site = self.args['site']

    storageElements = CSHelpers.getSiteStorageElements(site)
    computingElements = CSHelpers.getSiteComputingElements(site)

    for element in storageElements:
      status = self.rssClient.selectStatusElement("Resource", "Status", element, meta = { 'columns' : [ 'Status' ] })
      if not status[ 'OK' ]:
        return status

      if status['Value'] == 'Active':
        return S_OK({ 'Status': 'Active', 'Reason': 'An element that belongs to the site is Active' })

    for element in computingElements:
      status = self.rssClient.selectStatusElement("Resource", "Status", element, meta = { 'columns' : [ 'Status' ] })
      if not status[ 'OK' ]:
        return status

      if status['Value'] == 'Active':
        return S_OK({ 'Status': 'Active', 'Reason': 'An element that belongs to the site is Active' })

    return S_OK({ 'Status': 'Banned', 'Reason': 'There is no Active element in the site' })

  def doMaster( self ):
    return S_OK()

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
