# $HeadURL: $
""" InfoGetter

  Module used to match the decision parameters with the CS. In general terms, the
  decision parameters dictionary looks like ( but does not need to contain all
  key-value pairs ):
  
  {
    'element'     : str | None | list( str ),
    'name'        : ..same..
    'elementType' : ..same..
    'statusType'  : ..same..
    'status'      : ..same..
    'reason'      : ..same..
    'tokenOwner'  : ..same..
    'active'      : 'Active' / something else
  }

"""

import copy

from DIRAC                                import S_OK
from DIRAC.ResourceStatusSystem.Utilities import RssConfiguration, Utils

__RCSID__ = '$Id: $'


class InfoGetter:
  """ InfoGetter
  """

  def __init__( self ):
    """ Constructor. Imports the policy configurations containing the command
    information, among other things. 
    
    examples:
      >>> iGetter = InfoGetter()
    
    """
        
    configModule  = Utils.voimport( 'DIRAC.ResourceStatusSystem.Policy.Configurations' )
    self.policies = copy.deepcopy( configModule.POLICIESMETA )  


  def getPoliciesThatApply( self, decisionParams ):
    """ Given a dictionary, it matches it against all the policies configuration
    dictionaries as they are on the CS. Returns the policy dictionaries that 
    produced a positive match plus their configuration in <self.policies>. 
    
    examples:
      >>> # This matches all policies !
      >>> iGetter.getPoliciesThatApply( {} )
          [ { 
             'name'        : 'AlwaysActiveForResource',
             'type'        : 'AlwaysActive',
             'module'      : 'AlwaysActivePolicy',
             'description' : 'This is the AlwaysActive policy'
             'command'     : None,
             'args'        : {}
            },... ]
      >>> # There is no policy that matches BlahSite      
      >>> iGetter.getPoliciesThatApply( { 'name' : 'BlahSite' } )
          []
    
    :Parameters:
      **decisionParams** - `dict`
        dictionary with the parameters to match policies.
        
    :return: S_OK() / S_ERROR
    
    """
    
    policiesToBeLoaded = []
    
    # Get policies configuration metadata from CS.
    policiesConfig = RssConfiguration.getPolicies()
    if not policiesConfig[ 'OK' ]:
      return policiesConfig
    policiesConfig = policiesConfig[ 'Value' ]
    
    # Get policies that match the given decissionParameters
    for policyName, policySetup in policiesConfig.iteritems():
          
      # The section matchParams is not mandatory, so we set {} as default.
      policyMatchParams = policySetup.get( 'matchParams',  {} )   
      if not Utils.configMatch( decisionParams, policyMatchParams ):
        continue
        
      # the policyName replaces the policyTipe if not present. This allows us to
      # define several policies of the same type on the CS. We just need to
      # give them different names and assign them the same policyType. 
      try:
        policyType = policySetup[ 'policyType' ][ 0 ]
      except KeyError:
        policyType = policyName
      
      policyDict = {
                     'name' : policyName, 
                     'type' : policyType,
                     'args' : {}
                   }
      
      # Get policy static configuration        
      try:
        policyDict.update( self.policies[ policyType ] )
      except KeyError:
        continue  
      
      policiesToBeLoaded.append( policyDict )
       
    return S_OK( policiesToBeLoaded )    
    

  def getPolicyActionsThatApply( self, decisionParams, singlePolicyResults, policyCombinedResults ):
    """ Method that returns the actions to be triggered based on the original 
    decision parameters ( decisionParams ), which also can apply to the method
    `getPoliciesThatApply`, each one of the policy results ( singlePolicyResults )
    and the combined policy results ( policyCombinedResults ) as computed on the `PDP`.
    
    examples:
      >>> iGetter.getPolicyActionsThatApply( { 'name' : 'SiteA' },[],{} )[ 'Value' ]
          [ ( 'BanSiteA', 'BanSite' ) ]
      >>> iGetter.getPolicyActionsThatApply( { 'name' : 'SiteA' },[],
                                             { 'Status' : 'Active', 'Reason' : 'Blah' } )[ 'Value' ]
          [ ( 'BanSiteA', 'BanSite' ), ( 'EmailActive2Banned', 'EmailAction' ) ]    
      
    :Parameters:
      **decisionParams** - `dict`  
        dictionary with the parameters to match policies ( and actions in this case )
      **singlePolicyResults** - `list( dict )`
        list containing the dictionaries returned by the policies evaluated
      **policyCombinedResults** - `dict`
        dictionary containing the combined result of the policies evaluation
    
    :return: S_OK( list ) / S_ERROR
        
    """
    
    policyActionsThatApply = []
    
    # Get policies configuration metadata from CS.
    policyActionsConfig = RssConfiguration.getPolicyActions()
    if not policyActionsConfig[ 'OK' ]:
      return policyActionsConfig
    policyActionsConfig = policyActionsConfig[ 'Value' ]
    
    # Let's create a dictionary to use it with configMatch
    policyResults = self._getPolicyResults( singlePolicyResults )
    
    # Get policies that match the given decissionParameters
    for policyActionName, policyActionConfig in policyActionsConfig.iteritems():
      
      # The parameter policyType is mandatory. If not present, we pick policyActionName
      try:
        policyActionType = policyActionConfig[ 'actionType' ][ 0 ]
      except KeyError:
        policyActionType = policyActionName
      
      # We get matchParams to be compared against decissionParams
      policyActionMatchParams = policyActionConfig.get( 'matchParams', {} )
      if not Utils.configMatch( decisionParams, policyActionMatchParams ):
        continue
    
      # Let's check single policy results
      # Assumed structure:
      # ...
      # policyResults
      # <PolicyName> = <PolicyResult1>,<PolicyResult2>...
      policyActionPolicyResults = policyActionConfig.get( 'policyResults', {} )
      if not Utils.configMatch( policyResults, policyActionPolicyResults ):
        continue
      
      # combinedResult
      # \Status = X,Y
      # \Reason = asdasd,asdsa
      policyActionCombinedResult = policyActionConfig.get( 'combinedResult', {} )
      if not Utils.configMatch( policyCombinedResults, policyActionCombinedResult ):
        continue
            
      # They may not be necessarily the same
      policyActionsThatApply.append( ( policyActionName, policyActionType ) )    
      
    return S_OK( policyActionsThatApply )


  @staticmethod
  def _getPolicyResults( singlePolicyResults ):
    """ Method that transforms a list of dictionaries into a dictionary containing
    the policy names as keys and the status they propose as value.
    
    examples:
      >>> iGetter._getPolicyResults( [ { 'Status' : 'A',
                                         'Policy' : {
                                                      'name' : 'policyName',
                                                      ...                                          
                                                    }
                                       }, ... ] )
          { 'policyName' : 'A' }                             
    
    :Parameters:
      **singlePolicyResults** - `list( dict )`
        list of dictionaries containing the policies returned values
    
    :return: dict
    
    """
    
    # Let's create a dictionary to use it with configMatch
    policyResults = {}
    
    for policyResult in singlePolicyResults:
      try:
        policyResults[ policyResult[ 'Policy' ][ 'name' ] ] = policyResult[ 'Status' ]
      except KeyError:
        continue
      
    return policyResults  


#...............................................................................
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF