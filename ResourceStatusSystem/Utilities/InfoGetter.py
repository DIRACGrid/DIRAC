# $HeadURL: $
''' InfoGetter

  Module used to map the policies with the CS.

'''

import copy

from DIRAC                                import S_OK, S_ERROR
from DIRAC.ResourceStatusSystem.Utilities import RssConfiguration, Utils

__RCSID__ = '$Id: $'

class InfoGetter:
  """ 
    Class InfoGetter is in charge of getting information from the RSS Configurations
  """

  def __init__( self ):
    
    configModule = Utils.voimport( 'DIRAC.ResourceStatusSystem.Policy.Configurations' )
    self.policies = copy.deepcopy( configModule.POLICIESMETA )

  @staticmethod
  def sanitizeDecissionParams( decissionParams ):
    '''
      Method that filters the input parameters. If the input parameter keys
      are no present on the "params" tuple, are not taken into account.
    '''
    
    # active is a hook to disable the policy / action if needed
    params = ( 'element', 'name', 'elementType', 'statusType', 'status', 
               'reason', 'tokenOwner', 'active' )
    
    sanitizedParams = {} 
    
    for key in params:
      if key in decissionParams:
        # In CS names are with upper case, capitalize them here
        sanitizedParams[ key[0].upper() + key[1:] ] = decissionParams[ key ]
            
    return sanitizedParams

  def getPoliciesThatApply( self, decissionParams ):
    '''
      Method that sanitizes the input parameters and returns the policies that
      match them.
    '''

    decissionParams = self.sanitizeDecissionParams( decissionParams )    
    
    return self.__getPoliciesThatApply(decissionParams)

#  def getPolicyActionsThatApply( self, decissionParams ):
#    '''
#      Method that sanitizes the input parameters and returns the policies actions
#      that match them.
#    '''
#
#    decissionParams = self.sanitizeDecissionParams( decissionParams )    
#    
#    return self.__getPolicyActionsThatApply( decissionParams )

  def getPolicyActionsThatApply( self, decissionParams, singlePolicyResults,
                                  policyCombinedResults ):
    '''
      Method that sanitizes the input parameters and returns the policies actions
      that match them.
    '''

    decissionParams = self.sanitizeDecissionParams( decissionParams )    
    
    return self.__getPolicyActionsThatApply2( decissionParams, singlePolicyResults,
                                              policyCombinedResults )

  def getNotificationsThatApply( self, decissionParams, notificationAction ):
    '''
      Method that sanitizes the input parameters and returns the users that will
      be notified.
    '''

    decissionParams = self.sanitizeDecissionParams( decissionParams )

    return self.__getNotificationsThatApply( decissionParams, notificationAction )

  ## Private methods ###########################################################

#  def __getPoliciesThatApply( self, decissionParams ):
#    '''
#      Method that matches the input dictionary with the policies configuration in
#      the CS. It returns a list of policy dictionaries that matched.
#    '''
#    
#    policiesThatApply = []
#    
#    # Get policies configuration metadata from CS.
#    policiesConfig = RssConfiguration.getPolicies()
#    if not policiesConfig[ 'OK' ]:
#      return policiesConfig
#    policiesConfig = policiesConfig[ 'Value' ]
#    
#    # Get policies that match the given decissionParameters
#    for policyName, policyConfig in policiesConfig.items():
#      policyMatch = Utils.configMatch( decissionParams, policyConfig )   
#      if policyMatch:
#        policiesThatApply.append( policyName )
#        
#    policiesToBeLoaded = []    
#    
#    # Gets policies parameters from code.    
#    for policyName in policiesThatApply:
#      
#      if not policyName in self.policies:
#        continue
#      
#      policyDict = { 'name' : policyName }
#      policyDict.update( self.policies[ policyName ] ) 
#      
#      policiesToBeLoaded.append( policyDict )
#       
#    return S_OK( policiesToBeLoaded )

  def __getPoliciesThatApply( self, decissionParams ):
    '''
      Method that matches the input dictionary with the policies configuration in
      the CS. It returns a list of policy dictionaries that matched.
    '''
    
    policiesThatApply = []
    
    # Get policies configuration metadata from CS.
    policiesConfig = RssConfiguration.getPolicies()
    if not policiesConfig[ 'OK' ]:
      return policiesConfig
    policiesConfig = policiesConfig[ 'Value' ]
    
    # Each policy, has the following format
    # <policyName>
    # \
    #  policyType = <policyType>
    #  matchParams
    #  \
    #   ...        
    #  configParams
    #  \
    #   ...
    
    # Get policies that match the given decissionParameters
    for policyName, policySetup in policiesConfig.items():
      
      # The parameter policyType replaces policyName, so if it is not present,
      # we pick policyName
      try:
        policyType = policySetup[ 'policyType' ][ 0 ]
      except KeyError:
        policyType = policyName
        #continue
      
      # The section matchParams is not mandatory, so we set {} as default.
      policyMatchParams  = policySetup.get( 'matchParams',  {} )
      
      # FIXME: make sure the values in the policyConfigParams dictionary are typed !!
      policyConfigParams = {}
      #policyConfigParams = policySetup.get( 'configParams', {} )
      
      policyMatch = Utils.configMatch( decissionParams, policyMatchParams )   
      if policyMatch:
        policiesThatApply.append( ( policyName, policyType, policyConfigParams ) )
        
    policiesToBeLoaded = []    
    
    # Gets policies parameters from code.    
    for policyName, policyType, _policyConfigParams in policiesThatApply:
      
      try:
        policyMeta = self.policies[ policyType ]
      except KeyError:
        continue  
      
      # We are not going to use name / type anymore, but we keep them for debugging
      # and future usage.
      policyDict = { 
                     'name' : policyName, 
                     'type' : policyType,
                     'args' : {}
                   }
      
      # args is one of the parameters we are going to use on the policies. We copy
      # the defaults and then we update if with whatever comes from the CS.
      policyDict.update( policyMeta )
      # FIXME: watch out, args can be None !
      #policyDict[ 'args' ].update( policyConfigParams )
      
      policiesToBeLoaded.append( policyDict )
       
    return S_OK( policiesToBeLoaded )

  @staticmethod
  def __getPolicyActionsThatApply2( decissionParams, singlePolicyResults,
                                    policyCombinedResults ):
    '''
      Method that matches the input dictionary with the policy actions 
      configuration in the CS. It returns a list of policy actions names that 
      matched.
    '''
    
    policyActionsThatApply = []
    
    # Get policies configuration metadata from CS.
    policyActionsConfig = RssConfiguration.getPolicyActions()
    if not policyActionsConfig[ 'OK' ]:
      return policyActionsConfig
    policyActionsConfig = policyActionsConfig[ 'Value' ]
    
    # Let's create a dictionary to use it with configMatch
    policyResults = {}
    for policyResult in singlePolicyResults:
      try:
        policyResults[ policyResult[ 'Policy' ][ 'name' ] ] = policyResult[ 'Status' ]
      except KeyError:
        continue
    
    # Get policies that match the given decissionParameters
    for policyActionName, policyActionConfig in policyActionsConfig.items():
      
      # The parameter policyType is mandatory. If not present, we pick policyActionName
      try:
        policyActionType = policyActionConfig[ 'actionType' ][ 0 ]
      except KeyError:
        policyActionType = policyActionName
        #continue
      
      # We get matchParams to be compared against decissionParams
      policyActionMatchParams = policyActionConfig.get( 'matchParams', {} )
      policyMatch = Utils.configMatch( decissionParams, policyActionMatchParams )   
      #policyMatch = Utils.configMatch( decissionParams, policyActionConfig )
      if not policyMatch:
        continue
    
      # Let's check single policy results
      # Assumed structure:
      # ...
      # policyResults
      # <PolicyName> = <PolicyResult1>,<PolicyResult2>...
      policyActionPolicyResults = policyActionConfig.get( 'policyResults', {} )
      policyResultsMatch = Utils.configMatch( policyResults, policyActionPolicyResults )
      if not policyResultsMatch:
        continue
      
      # combinedResult
      # \Status = X,Y
      # \Reason = asdasd,asdsa
      policyActionCombinedResult = policyActionConfig.get( 'combinedResult', {} )
      policyCombinedMatch = Utils.configMatch( policyCombinedResults, policyActionCombinedResult )
      if not policyCombinedMatch:
        continue
            
      #policyActionsThatApply.append( policyActionName )
      # They may not be necessarily the same
      policyActionsThatApply.append( ( policyActionName, policyActionType ) )    
      
    return S_OK( policyActionsThatApply )

  @staticmethod
  def __getPolicyActionsThatApply( decissionParams ):
    '''
      Method that matches the input dictionary with the policy actions 
      configuration in the CS. It returns a list of policy actions names that 
      matched.
    '''
    
    policyActionsThatApply = []
    
    # Get policies configuration metadata from CS.
    policyActionsConfig = RssConfiguration.getPolicyActions()
    if not policyActionsConfig[ 'OK' ]:
      return policyActionsConfig
    policyActionsConfig = policyActionsConfig[ 'Value' ]
    
    # Get policies that match the given decissionParameters
    for policyActionName, policyActionConfig in policyActionsConfig.items():
      policyMatch = Utils.configMatch( decissionParams, policyActionConfig )   
      if policyMatch:
        policyActionsThatApply.append( policyActionName )
               
    return S_OK( policyActionsThatApply )

  @staticmethod
  def __getNotificationsThatApply( decissionParams, notificationAction ):
    '''
      Method that matches the input dictionary with the notifications 
      configuration in the CS. It returns a list of notification dictionaries that 
      matched.
    '''

    notificationsThatApply = []
    
    # Get notifications configuration metadata from CS.
    notificationsConfig = RssConfiguration.getNotifications()
    if not notificationsConfig[ 'OK' ]:
      return notificationsConfig
    notificationsConfig = notificationsConfig[ 'Value' ]
    
    if not notificationAction in notificationsConfig:
      return S_ERROR( '"%s" not in notifications configuration' % notificationAction )
    
    notificationsConfig = notificationsConfig[ notificationAction ]
    
    # Get notifications that match the given decissionParameters
    for notificationName, notificationConfig in notificationsConfig.items():
      notificationMatch = Utils.configMatch( decissionParams, notificationConfig )   
      if notificationMatch:
        
        notificationConfig[ 'name' ] = notificationName
        notificationsThatApply.append( notificationConfig )
               
    return S_OK( notificationsThatApply )    
    
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF

#  def getInfoToApply( self, args, granularity, statusType = None, status = None,
#                      formerStatus = None, siteType = None, serviceType = None,
#                      resourceType = None, useNewRes = False ):
#    """ Main method. Use internal methods to parse information regarding:
#        policies to be applied, policy types, panel and view info.
#
#        :params:
#          :attr:`args`: a tuple. Can contain: 'policy', 'policyType', 'panel_info', 'view_info'
#
#          :attr:`granularity`: a ValidElement
#
#          :attr:`status`: a ValidStatus
#
#          :attr:`formerStatus`: a ValidStatus
#
#          :attr:`siteType`: a ValidSiteType
#
#          :attr:`serviceType`: a ValidServiceType
#
#          :attr:`resourceType`: a ValidSiteType
#    """
#
#    EVAL = {}
#
#    if 'policy' in args:
#      EVAL['Policies'] = self.__getPolToEval( granularity = granularity, statusType = statusType,
#                                              status = status, formerStatus = formerStatus,
#                                              siteType = siteType, serviceType = serviceType,
#                                              resourceType = resourceType, useNewRes = useNewRes)
#
#    if 'policyType' in args:
#      EVAL['PolicyType'] = self.__getPolTypes( granularity = granularity, statusType = statusType,
#                                               status = status, formerStatus = formerStatus,
#                                               siteType = siteType, serviceType = serviceType,
#                                               resourceType = resourceType)
#
#    if 'panel_info' in args:
#      if granularity in ('Site', 'Sites'):
#        info = 'Site_Panel'
#      elif granularity in ('Service', 'Services'):
#        if serviceType == 'Storage':
#          info = 'Service_Storage_Panel'
#        elif serviceType == 'Computing':
#          info = 'Service_Computing_Panel'
#        elif serviceType == 'VO-BOX':
#          info = 'Service_VO-BOX_Panel'
#        elif serviceType == 'VOMS':
#          info = 'Service_VOMS_Panel'
#      elif granularity in ('Resource', 'Resources'):
#        info = 'Resource_Panel'
#      elif granularity in ('StorageElementRead', 'StorageElementsRead'):
#        info = 'SE_Panel'
#      elif granularity in ('StorageElementWrite', 'StorageElementsWrite'):
#        info = 'SE_Panel'
#      EVAL['Info'] = self.__getPanelsInfo( granularity = granularity, statusType = statusType,
#                                           status = status, formerStatus = formerStatus,
#                                           siteType = siteType, serviceType = serviceType,
#                                           resourceType = resourceType, panel_name = info,
#                                           useNewRes = useNewRes )
#
#    if 'view_info' in args:
#      panels_info_dict = {}
#
#      if granularity in ('Site', 'Sites'):
#        granularity = None
#
#      view_panels = self.__getViewPanels(granularity)
#      for panel in view_panels:
#        panel_info = self.__getPanelsInfo( granularity = granularity, statusType = statusType,
#                                           status = status, formerStatus = formerStatus,
#                                           siteType = siteType, serviceType = serviceType,
#                                           resourceType = resourceType, panel_name = panel,
#                                           useNewRes = useNewRes )
#        panels_info_dict[panel] = panel_info
#
#      EVAL['Panels'] = panels_info_dict
#
#    return EVAL

#################################################################################
#
#  def __getPolToEval( self, granularity, statusType = None, status=None,
#                      formerStatus=None, siteType=None, serviceType=None,
#                      resourceType=None, useNewRes=False ):
#    """Returns a possibly empty list of dicts, each dict containing
#    enough information to evaluate a given policy"""
#
#    # This dict is constructed to be used with function dictMatch that
#    # helps selecting policies. **kwargs are not used due to the fact
#    # that it's too dangerous here.
#    argsdict = { 'Granularity'  : granularity,
#                 'StatusType'   : statusType,
#                 'Status'       : status,
#                 'FormerStatus' : formerStatus,
#                 'SiteType'     : siteType,
#                 'ServiceType'  : serviceType,
#                 'ResourceType' : resourceType }
#
#    pConfig = getTypedDictRootedAtOperations("Policies")
#    pol_to_eval = (p for p in pConfig if Utils.dictMatch(argsdict, pConfig[p]))
#    polToEval_Args = []
#
#    for p in pol_to_eval:
#      try:
#        moduleName = self.C_Policies[p]['module']
#      except KeyError:
#        moduleName = None
#      try:
#        ConfirmationPolicy = self.C_Policies[p]['ConfirmationPolicy']
#      except KeyError:
#        ConfirmationPolicy = None
#
#      if useNewRes:
#        try:
#          commandIn = self.C_Policies[p]['commandInNewRes']
#        except KeyError:
#          commandIn = self.C_Policies[p]['commandIn']
#        try:
#          args = self.C_Policies[p]['argsNewRes']
#        except KeyError:
#          args = self.C_Policies[p]['args']
#      else:
#        commandIn = self.C_Policies[p]['commandIn']
#        args = self.C_Policies[p]['args']
#
#      polToEval_Args.append({'Name' : p, 'Module' : moduleName, 'args' : args,
#                             'ConfirmationPolicy' : ConfirmationPolicy,
#                             'commandIn' : commandIn})
#
#    return polToEval_Args
#
#################################################################################
#
#  def __getPolTypes( self, granularity, statusType=None, status=None,
#                     formerStatus=None, newStatus=None, siteType=None,
#                     serviceType=None, resourceType=None ):
#    """Get Policy Types from config that match the given keyword
#    arguments. Always returns a generator object, possibly empty."""
#
#    # This dict is constructed to be used with function dictMatch that
#    # helps selecting policies. **kwargs are not used due to the fact
#    # that it's too dangerous here.
#    argsdict = {'Granularity'  : granularity,
#                'StatusType'   : statusType,
#                'Status'       : status,
#                'FormerStatus' : formerStatus,
#                'NewStatus'    : newStatus,
#                'SiteType'     : siteType,
#                'ServiceType'  : serviceType,
#                'ResourceType' : resourceType }
#
#    pTconfig = RssConfiguration.getValidPolicyTypes()
#    return (pt for pt in pTconfig if Utils.dictMatch(argsdict, pTconfig[pt]))
#
#  def getNewPolicyType(self, granularity, newStatus):
#    return self.__getPolTypes(granularity = granularity, newStatus = newStatus)
#
#################################################################################
#
#  def __getPanelsInfo( self, granularity, statusType = None, status = None,
#                       formerStatus = None, siteType = None, serviceType = None,
#                       resourceType = None, panel_name = None, useNewRes = False ):
#
#    info = []
#
#    # First, select only policies we want.
#    argsdict = {'Granularity'  : granularity,
#                'StatusType'   : statusType,
#                'Status'       : status,
#                'FormerStatus' : formerStatus,
#                'SiteType'     : siteType,
#                'ServiceType'  : serviceType,
#                'ResourceType' : resourceType}
#
#
#    all_policies = getTypedDictRootedAtOperations("Policies")
#    selected_policies = []
#    for p in all_policies:
#      if Utils.dictMatch(argsdict, all_policies[p]):
#        selected_policies.append(p)
#
#    for p in selected_policies:                   # For selected policies
#      if panel_name in self.C_Policies[p].keys(): # For selected panel_name (arguments)
#
#        toAppend = copy.deepcopy(self.C_Policies[p][panel_name]) # type(toAppend) = list
#
#        # Put CommandIn and args to correct values according to useNewRes
#        if useNewRes:
#          for panel in toAppend:
#            for info_type in panel.keys():
#
#              if type(panel[info_type]) == dict:
#                try:
#                  panel[info_type]['CommandIn'] = panel[info_type]['CommandInNewRes']
#                  del panel[info_type]['CommandInNewRes']
#                except KeyError:
#                  pass
#                try:
#                  panel[info_type]['args'] = panel[info_type]['argsNewRes']
#                  del panel[info_type]['argsNewRes']
#                except KeyError:
#                  pass
#        else:
#          for panel in toAppend:
#            for info_type in panel.keys():
#              try:
#                del panel[info_type]['CommandInNewRes']
#              except KeyError:
#                pass
#              try:
#                del panel[info_type]['argsNewRes']
#              except KeyError:
#                pass
#
#        info.append({p:toAppend})
#
#    return info
#
#################################################################################
#
#  def __getViewPanels( self, granularity ):
#    if granularity is None:
#      granularity = 'Site'
#    return RssConfiguration.views_panels[ granularity ]

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
