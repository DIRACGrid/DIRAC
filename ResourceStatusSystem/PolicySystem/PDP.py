# $HeadURL $
''' PDP

  PolicyDecissionPoint

'''

from DIRAC                                                import gLogger, S_OK, S_ERROR 
from DIRAC.ResourceStatusSystem.PolicySystem.PolicyCaller import PolicyCaller
from DIRAC.ResourceStatusSystem.PolicySystem.StateMachine import RSSMachine
from DIRAC.ResourceStatusSystem.Utilities                 import RssConfiguration
from DIRAC.ResourceStatusSystem.Utilities.InfoGetter      import InfoGetter

__RCSID__  = '$Id: $'

class PDP:
  """
    The PDP (Policy Decision Point) module is used to:
    1. Decides which policies have to be applied.
    2. Invokes an evaluation of the policies, and returns the result (to a PEP)
  """

  def __init__( self, clients ):
    '''
      Constructor. Defines members that will be used later on.
    '''
    
    self.pCaller         = PolicyCaller( clients = clients )
    self.iGetter         = InfoGetter()

    self.decissionParams = {}  
    self.rssMachine      = RSSMachine( 'Unknown' )

  def setup( self, decissionParams = None ):

    standardParamsDict = {
                          'element'     : None,
                          'name'        : None,
                          'elementType' : None,
                          'statusType'  : None,
                          'status'      : None,
                          'reason'      : None,
                          'tokenOwner'  : None,
                          # Last parameter allows policies to be deactivated
                          'active'      : 'Active'
                          }

    if decissionParams is not None:
      standardParamsDict.update( decissionParams )
      
    self.decissionParams = standardParamsDict  
        
################################################################################

  def takeDecision( self ):#, policyIn = None, argsIn = None, knownInfo = None ):
    """ PDP MAIN FUNCTION

        decides policies that have to be applied, based on

        __granularity,

        __name,

        __status,

        __formerStatus

        __reason

        If more than one policy is evaluated, results are combined.

        Logic for combination: a conservative approach is followed
        (i.e. if a site should be banned for at least one policy, that's what is returned)

        returns:

          { 'PolicyType': a policyType (in a string),
            'Action': True|False,
            'Status': 'Active'|'Probing'|'Banned',
            'Reason': a reason
            #'EndDate: datetime.datetime (in a string)}
    """

    policiesThatApply = self.iGetter.getPoliciesThatApply( self.decissionParams )
    if not policiesThatApply[ 'OK' ]:
      return policiesThatApply
    policiesThatApply = policiesThatApply[ 'Value' ]
    
    singlePolicyResults   = self._runPolicies( policiesThatApply )
    if not singlePolicyResults[ 'OK' ]:
      return singlePolicyResults
    singlePolicyResults = singlePolicyResults[ 'Value' ]    
        
    policyCombinedResults = self._combineSinglePolicyResults( singlePolicyResults )
    if not policyCombinedResults[ 'OK' ]:
      return policyCombinedResults
    policyCombinedResults = policyCombinedResults[ 'Value' ]

    policyActionsThatApply = self.iGetter.getPolicyActionsThatApply( self.decissionParams,
                                                                     singlePolicyResults,
                                                                     policyCombinedResults )
    if not policyActionsThatApply[ 'OK' ]:
      return policyActionsThatApply
    policyActionsThatApply = policyActionsThatApply[ 'Value' ]
           
    policyCombinedResults[ 'PolicyAction' ] = policyActionsThatApply

    return S_OK( 
                { 
                 'singlePolicyResults'  : singlePolicyResults,
                 'policyCombinedResult' : policyCombinedResults,
                 'decissionParams'      : self.decissionParams 
                 }
                )

################################################################################

  def _runPolicies( self, policies, decissionParams = None ):
    
    if decissionParams is None:
      decissionParams = self.decissionParams
    
    validStatus = RssConfiguration.getValidStatus()
    if not validStatus[ 'OK' ]:
      return validStatus
    validStatus = validStatus[ 'Value' ]
       
    policyInvocationResults = []
    
    for policyDict in policies:
      
      policyInvocationResult = self.pCaller.policyInvocation( decissionParams,
                                                              policyDict ) 
      if not policyInvocationResult[ 'OK' ]:
        # We should never enter this line ! Just in case there are policies
        # missconfigured !
        _msg = 'runPolicies no OK: %s' % policyInvocationResult
        gLogger.error( _msg )
        return S_ERROR( _msg )
       
      policyInvocationResult = policyInvocationResult[ 'Value' ]
      
      if not 'Status' in policyInvocationResult:
        _msg = 'runPolicies (no Status): %s' % policyInvocationResult
        gLogger.error( _msg )
        return S_ERROR( _msg )
        
      if not policyInvocationResult[ 'Status' ] in validStatus:
        _msg = 'runPolicies ( not valid status ) %s' % policyInvocationResult[ 'Status' ]
        gLogger.error( _msg )
        return S_ERROR( _msg )

      if not 'Reason' in policyInvocationResult:
        _msg = 'runPolicies (no Reason): %s' % policyInvocationResult
        gLogger.error( _msg )
        return S_ERROR( _msg )
       
      policyInvocationResults.append( policyInvocationResult )
      
    return S_OK( policyInvocationResults )   
    
################################################################################

  def _combineSinglePolicyResults( self, singlePolicyRes ):
    '''
      singlePolicyRes = [ { 'State' : X, 'Reason' : Y, ... }, ... ]
      
      If there are no policyResults, returns Unknown as there are no policies to
      apply.
      
      Order elements in list by state, being the lowest the most restrictive
      one in the hierarchy.
   
    '''

    # Dictionary to be returned
    policyCombined = { 
                       'Status'       : None,
                       'Reason'       : ''
                      }

    # If there are no policyResults, we return Unknown    
    if not singlePolicyRes:
      
      _msgTuple = ( self.decissionParams[ 'element' ], self.decissionParams[ 'name' ],
                    self.decissionParams[ 'elementType' ] )
      
      policyCombined[ 'Status' ] = 'Unknown'
      policyCombined[ 'Reason' ] = 'No policy applies to %s, %s, %s' % _msgTuple 
      
      return S_OK( policyCombined )

    # We set the rssMachine on the current state
    machineStatus = self.rssMachine.setState( self.decissionParams[ 'status' ] )
    if not machineStatus[ 'OK' ]:
      return machineStatus
    
    # Order statuses by most restrictive ( lower level first )
    self.rssMachine.orderPolicyResults( singlePolicyRes )
    #policyResults = self.rssMachine.orderPolicyResults( singlePolicyRes )
        
    # Get according to the RssMachine the next state, given a candidate    
    candidateState = singlePolicyRes[ 0 ][ 'Status' ]
    nextState      = self.rssMachine.getNextState( candidateState )
    
    if not nextState[ 'OK' ]:
      return nextState
    nextState = nextState[ 'Value' ]
    
    # If the RssMachine does not accept the candidate, return forcing message
    if candidateState != nextState:
                
      policyCombined[ 'Status' ] = nextState
      policyCombined[ 'Reason' ] = 'RssMachine forced status %s to %s' % ( candidateState, nextState )
      return S_OK( policyCombined )
    
    # If the RssMachine accepts the candidate, just concatenate the reasons
    for policyRes in singlePolicyRes:
      
      if policyRes[ 'Status' ] == nextState:
        policyCombined[ 'Reason' ] += '%s ###' % policyRes[ 'Reason' ]  
        
    policyCombined[ 'Status' ] = nextState
    
    return S_OK( policyCombined )                             

################################################################################

#  def __useOldPolicyRes( self, name, policyName ):
#    '''
#     Use the RSS Service to get an old policy result.
#     If such result is older than 2 hours, it returns {'Status':'Unknown'}
#    '''
#    res = self.clients[ 'ResourceManagementClient' ].getPolicyResult( name = name, policyName = policyName )
#    
#    if not res[ 'OK' ]:
#      return { 'Status' : 'Unknown' }
#    
#    res = res[ 'Value' ]
#
#    if res == []:
#      return { 'Status' : 'Unknown' }
#
#    res = res[ 0 ]
#
#    oldStatus     = res[ 5 ]
#    oldReason     = res[ 6 ]
#    lastCheckTime = res[ 8 ]
#
#    if ( lastCheckTime + datetime.timedelta(hours = 2) ) < datetime.datetime.utcnow():
#      return { 'Status' : 'Unknown' }
#
#    result = {}
#
#    result[ 'Status' ]     = oldStatus
#    result[ 'Reason' ]     = oldReason
#    result[ 'OLD' ]        = True
#    result[ 'PolicyName' ] = policyName
#
#    return result

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF