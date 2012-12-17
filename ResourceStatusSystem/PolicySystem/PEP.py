# $HeadURL:  $
''' PEP

  Module used for enforcing policies. Its class is used for:
    1. invoke a PDP and collects results
    2. enforcing results by:
       a. saving result on a DB
       b. raising alarms
       c. other....
'''

from DIRAC                                                       import gLogger, S_OK
from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient      import ResourceStatusClient
from DIRAC.ResourceStatusSystem.Client.ResourceManagementClient  import ResourceManagementClient
from DIRAC.ResourceStatusSystem.PolicySystem.PDP                 import PDP
from DIRAC.ResourceStatusSystem.Utilities                        import Utils

__RCSID__  = '$Id:  $'

class PEP:

  def __init__( self, clients = None ):
   
    if clients is None:
      clients = {}
   
    if 'ResourceStatusClient' in clients:           
      self.rsClient = clients[ 'ResourceStatusClient' ]
    else:
      self.rsClient = ResourceStatusClient()
    if 'ResourceManagementClient' in clients:             
      self.rmClient = clients[ 'ResourceManagementClient' ]
    else: 
      self.rmClient = ResourceManagementClient()

    self.clients = clients
    self.pdp     = PDP( clients )   

  def enforce( self, decissionParams ):
    
    '''
      Enforce policies for given set of keyworkds. To be better explained.
    '''
   
    ## policy decision point setup #############################################  
    
    self.pdp.setup( decissionParams )

    ## policy decision #########################################################

    resDecisions = self.pdp.takeDecision()
    if not resDecisions[ 'OK' ]:
      gLogger.error( 'PEP: Something went wrong, not enforcing policies for %s' % decissionParams )
      return resDecisions
    resDecisions = resDecisions[ 'Value' ]
    
    # We take from PDP the decision parameters used to find the policies
    decissionParams      = resDecisions[ 'decissionParams' ]
    policyCombinedResult = resDecisions[ 'policyCombinedResult' ]
    singlePolicyResults  = resDecisions[ 'singlePolicyResults' ]
    
    for policyAction in policyCombinedResult[ 'PolicyAction' ]:
      
      try:
        actionMod = Utils.voimport( 'DIRAC.ResourceStatusSystem.PolicySystem.Actions.%s' % policyAction )
      except ImportError:
        gLogger.error( 'Error importing %s action' % policyAction )
        
      if not hasattr( actionMod, policyAction ):
        gLogger.error( 'Error importing %s action class' % policyAction )
      
      action = getattr( actionMod, policyAction )( decissionParams, 
                                                   policyCombinedResult,
                                                   singlePolicyResults, 
                                                   self.clients )
      
      gLogger.debug( policyAction )
      
      actionResult = action.run()
      if not actionResult[ 'OK' ]:
        gLogger.error( actionResult[ 'Message' ] ) 
        
    return S_OK( resDecisions )

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF