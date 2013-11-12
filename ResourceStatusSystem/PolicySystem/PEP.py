# $HeadURL:  $
""" PEP

  PEP ( Policy Enforcement Point ) is the front-end of the whole Policy System.
  Any interaction with it must go through the PEP to ensure a smooth flow.
  
  Firstly, it loads the PDP ( Policy Decision Point ) which actually is the
  module doing all dirty work ( finding policies, running them, merging their
  results, etc... ). Indeed, the PEP takes the output of the PDP for a given set
  of parameters ( decissionParams ) and enforces the actions that apply ( also
  determined by the PDP output ).
  
"""

from DIRAC                                                      import gLogger, S_OK, S_ERROR
from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient     import ResourceStatusClient
from DIRAC.ResourceStatusSystem.Client.ResourceManagementClient import ResourceManagementClient
from DIRAC.ResourceStatusSystem.PolicySystem.PDP                import PDP
from DIRAC.ResourceStatusSystem.Utilities                       import Utils

__RCSID__  = '$Id: $'

class PEP:
  """ PEP ( Policy Enforcement Point )
  """

  def __init__( self, clients = None ):
    """ Constructor
    
    examples:
      >>> pep = PEP()
      >>> pep1 = PEP( { 'ResourceStatusClient' : ResourceStatusClient() } )
      >>> pep2 = PEP( { 'ResourceStatusClient' : ResourceStatusClient(), 'ClientY' : None } )
    
    :Parameters:
      **clients** - [ None, `dict` ]
        dictionary with clients to be used in the commands issued by the policies.
        If not defined, the commands will import them. It is a measure to avoid
        opening the same connection every time a policy is evaluated.
        
    """
   
    if clients is None:
      clients = {}
    
    # PEP uses internally two of the clients: ResourceStatusClient and ResouceManagementClient   
    if 'ResourceStatusClient' in clients:           
      self.rsClient = clients[ 'ResourceStatusClient' ]
    else:
      self.rsClient = ResourceStatusClient()
    if 'ResourceManagementClient' in clients:             
      self.rmClient = clients[ 'ResourceManagementClient' ]
    else: 
      self.rmClient = ResourceManagementClient()

    self.clients = clients
    # Pass to the PDP the clients that are going to be used on the Commands
    self.pdp     = PDP( clients )   


  def enforce( self, decisionParams ):
    """ Given a dictionary with decisionParams, it is passed to the PDP, which
    will return ( in case there is a/are positive match/es ) a dictionary containing
    three key-pair values: the original decisionParams ( `decisionParams` ), all
    the policies evaluated ( `singlePolicyResults` ) and the computed final result
    ( `policyCombinedResult` ).
    
    To know more about decisionParams, please read PDP.setup where the decisionParams
    are sanitized.
    
    examples:
       >>> pep.enforce( { 'element' : 'Site', 'name' : 'MySite' } )
       >>> pep.enforce( { 'element' : 'Resource', 'name' : 'myce.domain.ch' } )
    
    :Parameters:
      **decisionParams** - `dict`
        dictionary with the parameters that will be used to match policies.
    
    """ 
    
    # Setup PDP with new parameters dictionary
    self.pdp.setup( decisionParams )

    # Run policies, get decision, get actions to apply
    resDecisions = self.pdp.takeDecision()
    if not resDecisions[ 'OK' ]:
      gLogger.error( 'PEP: Something went wrong, not enforcing policies for %s' % decisionParams )
      return resDecisions
    resDecisions = resDecisions[ 'Value' ]
    
    # We take from PDP the decision parameters used to find the policies
    decisionParams       = resDecisions[ 'decissionParams' ]
    policyCombinedResult = resDecisions[ 'policyCombinedResult' ]
    singlePolicyResults  = resDecisions[ 'singlePolicyResults' ]

    # We have run the actions and at this point, we are about to execute the actions.
    # One more final check before proceeding
    isNotUpdated = self.__isNotUpdated( decisionParams )
    if not isNotUpdated[ 'OK' ]:
      return isNotUpdated
                
    for policyActionName, policyActionType in policyCombinedResult[ 'PolicyAction' ]:
      
      try:
        actionMod = Utils.voimport( 'DIRAC.ResourceStatusSystem.PolicySystem.Actions.%s' % policyActionType )
      except ImportError:
        gLogger.error( 'Error importing %s action' % policyActionType )
        continue
      
      try:
        action = getattr( actionMod, policyActionType )
      except AttributeError:
        gLogger.error( 'Error importing %s action class' % policyActionType )
        continue  
              
      actionObj = action( policyActionName, decisionParams, policyCombinedResult,
                          singlePolicyResults, self.clients )
      
      gLogger.debug( ( policyActionName, policyActionType ) )
      
      actionResult = actionObj.run()
      if not actionResult[ 'OK' ]:
        gLogger.error( actionResult[ 'Message' ] ) 
        
    return S_OK( resDecisions )


  def __isNotUpdated( self, decisionParams ):
    """ Checks for the existence of the element as it was passed to the PEP. It may
    happen that while being the element processed by the PEP an user through the 
    web interface or the CLI has updated the status for this particular element. As
    a result, the PEP would overwrite whatever the user had set. This check is not
    perfect, as still an user action can happen while executing the actions, but
    the probability is close to 0. However, if there is an action that takes seconds
    to be executed, this must be re-evaluated. !
    
    :Parameters:
      **decisionParams** - `dict`
        dictionary with the parameters that will be used to match policies
        
    :return: S_OK / S_ERROR
    
    """
    
    # Copy original dictionary and get rid of one key we cannot pass as kwarg
    selectParams = decisionParams.copy()
    del selectParams[ 'element' ]
    del selectParams[ 'active' ]
    
    # We expect to have an exact match. If not, then something has changed and
    # we cannot proceed with the actions.    
    unchangedRow = self.rsClient.selectStatusElement( decisionParams[ 'element' ], 
                                                      'Status', **selectParams )
    if not unchangedRow[ 'OK' ]:
      return unchangedRow
    
    if not unchangedRow[ 'Value' ]:
      msg = '%(name)s  ( %(status)s / %(statusType)s ) has been updated after PEP started running'
      return S_ERROR( msg % selectParams )
    
    return S_OK()

#...............................................................................
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF