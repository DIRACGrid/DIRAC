# $HeadURL:  $
''' FTSUnbanAction

'''

from DIRAC                                                      import gConfig, gLogger, S_ERROR, S_OK
from DIRAC.Interfaces.API.DiracAdmin                            import DiracAdmin
from DIRAC.ResourceStatusSystem.PolicySystem.Actions.BaseAction import BaseAction
from DIRAC.ResourceStatusSystem.Utilities                       import RssConfiguration
#from DIRAC.ResourceStatusSystem.Utilities.InfoGetter            import InfoGetter
from DIRAC.Core.Security.ProxyInfo                              import getProxyInfo
from DIRAC.ConfigurationSystem.Client.Helpers.Resources         import getFTSServers
import fts3.rest.client.easy as fts3
import json

from DIRAC import gLogger

__RCSID__  = '$Id:  $'

class FTSUnbanAction( BaseAction ):
	'''
		Action that sends to the FTS server a unbanning request for a given list of sites.
	'''
	
	def __init__( self, name, decissionParams, enforcementResult, singlePolicyResults, clients ):
		
		super( FTSUnbanAction, self ).__init__( name, decissionParams, enforcementResult, 
                                         singlePolicyResults, clients )
		self.diracAdmin = DiracAdmin()
		
		# enforcementResult supposed to look like:
		# { 
		#	 'Status'				: <str>,
		#	 'Reason'				: <str>, 
		#	 'PolicyActions' : <list>,
		#	 [ 'EndDate' : <str> ]
		# } 

		# decissionParams supposed to look like:
		# {
		#	 'element'		 : None,
		#	 'name'				: None,
		#	 'elementType' : None,
		#	 'statusType'	: None,
		#	 'status'			: None,
		#	 'reason'			: None,
		#	 'tokenOwner'	: None
		# }

	def run( self ):
		'''
			Checks it has the parameters it needs and tries to unban the site.
		'''		
		storageElement = self.decissionParams[ 'name' ]
		elementType = self.decissionParams[ 'elementType' ]
		if elementType != 'StorageElement':
			return S_ERROR( "'elementType' should be 'StorageElement'" )
					
		return self._unbanSite( storageElement )		

	def _unbanSite( self, storageElement ):

#		from DIRAC.Interfaces.API.DiracAdmin import DiracAdmin
#		diracAdmin = DiracAdmin()
#		address = InfoGetter().getNotificationsThatApply( self.decissionParams, self.actionName )
		
		#endpoint = 'https://fts3-pilot.cern.ch:8446'
		endpoint = getFTSServers("FTS3")[ 'Value' ][0]
		proxyPath = getProxyInfo().get('Value').get('path')
		context = fts3.Context(endpoint, proxyPath)
		#site = 'gsiftp://example.com'
		output = fts3.unban_se(context, storageElement)
			
		return S_OK( json.loads(context.get("ban/se")) )

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF