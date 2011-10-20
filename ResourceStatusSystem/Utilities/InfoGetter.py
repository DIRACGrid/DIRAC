################################################################################
# $HeadURL:  $
################################################################################
__RCSID__ = "$Id:  $"

import copy

from DIRAC.ResourceStatusSystem.Utilities.CS import getTypedDictRootedAt
from DIRAC.ResourceStatusSystem.Utilities    import Utils
from DIRAC.ResourceStatusSystem              import views_panels

class InfoGetter:
  """ Class InfoGetter is in charge of getting information from the RSS Configurations
  """
  
  def __init__(self, VOExtension):
    """
    Standard constructor

    :params:
      :attr:`VOExtension`: string - VO extension (e.g. 'LHCb')
    """

    configModule    = Utils.voimport("DIRAC.ResourceStatusSystem.Policy.Configurations", VOExtension)
    self.C_Policies = copy.deepcopy(configModule.Policies)

################################################################################

  def getInfoToApply( self, args, granularity, statusType = None, status = None, 
                      formerStatus = None, siteType = None, serviceType = None, 
                      resourceType = None, useNewRes = False ):
    """ Main method. Use internal methods to parse information regarding:
        policies to be applied, policy types, panel and view info.

        :params:
          :attr:`args`: a tuple. Can contain: 'policy', 'policyType', 'panel_info', 'view_info'

          :attr:`granularity`: a ValidRes

          :attr:`status`: a ValidStatus

          :attr:`formerStatus`: a ValidStatus

          :attr:`siteType`: a ValidSiteType

          :attr:`serviceType`: a ValidServiceType

          :attr:`resourceType`: a ValidSiteType
    """

    EVAL = {}

    if 'policy' in args:
      EVAL['Policies'] = self.__getPolToEval( granularity = granularity, statusType = statusType,
                                              status = status, formerStatus = formerStatus, 
                                              siteType = siteType, serviceType = serviceType, 
                                              resourceType = resourceType, useNewRes = useNewRes)

    if 'policyType' in args:
      EVAL['PolicyType'] = self.__getPolTypes( granularity = granularity, statusType = statusType,
                                               status = status, formerStatus = formerStatus, 
                                               siteType = siteType, serviceType = serviceType, 
                                               resourceType = resourceType)

    if 'panel_info' in args:
      if granularity in ('Site', 'Sites'):
        info = 'Site_Panel'
      elif granularity in ('Service', 'Services'):
        if serviceType == 'Storage':
          info = 'Service_Storage_Panel'
        elif serviceType == 'Computing':
          info = 'Service_Computing_Panel'
        elif serviceType == 'VO-BOX':
          info = 'Service_VO-BOX_Panel'
        elif serviceType == 'VOMS':
          info = 'Service_VOMS_Panel'
      elif granularity in ('Resource', 'Resources'):
        info = 'Resource_Panel'
      elif granularity in ('StorageElementRead', 'StorageElementsRead'):
        info = 'SE_Panel'
      elif granularity in ('StorageElementWrite', 'StorageElementsWrite'):
        info = 'SE_Panel'
      EVAL['Info'] = self.__getPanelsInfo( granularity = granularity, statusType = statusType,
                                           status = status, formerStatus = formerStatus, 
                                           siteType = siteType, serviceType = serviceType, 
                                           resourceType = resourceType, panel_name = info, 
                                           useNewRes = useNewRes )

    if 'view_info' in args:
      panels_info_dict = {}

      if granularity in ('Site', 'Sites'):
        granularity = None

      view_panels = self.__getViewPanels(granularity)
      for panel in view_panels:
        panel_info = self.__getPanelsInfo( granularity = granularity, statusType = statusType, 
                                           status = status, formerStatus = formerStatus, 
                                           siteType = siteType, serviceType = serviceType, 
                                           resourceType = resourceType, panel_name = panel, 
                                           useNewRes = useNewRes )
        panels_info_dict[panel] = panel_info

      EVAL['Panels'] = panels_info_dict

    return [EVAL]

################################################################################

  def getNewPolicyType(self, granularity, newStatus):
    return self.__getPolTypes(granularity = granularity, newStatus = newStatus)

################################################################################

  def __getPolToEval( self, granularity, statusType = None, status=None, 
                      formerStatus=None, siteType=None, serviceType=None, 
                      resourceType=None, useNewRes=False ):

    # This dict is constructed to be used with function dictMatch that
    # helps selecting policies. **kwargs are not used due to the fact
    # that it's too dangerous here.
    argsdict = { 'Granularity'  : granularity,
                 'Status'       : status,
                 'FormerStatus' : formerStatus,
                 'SiteType'     : siteType,
                 'ServiceType'  : serviceType,
                 'ResourceType' : resourceType }

    pConfig = getTypedDictRootedAt("Policies")
    pol_to_eval = []

    for p in pConfig:
      if Utils.dictMatch(argsdict, pConfig[p]):
        pol_to_eval.append(p)

    polToEval_Args = []

    for p in pol_to_eval:
      try:
        moduleName = self.C_Policies[p]['module']
      except KeyError:
        moduleName = None
      try:
        ConfirmationPolicy = self.C_Policies[p]['ConfirmationPolicy']
      except KeyError:
        ConfirmationPolicy = None

      if useNewRes:
        try:
          commandIn = self.C_Policies[p]['commandInNewRes']
        except KeyError:
          commandIn = self.C_Policies[p]['commandIn']
        try:
          args = self.C_Policies[p]['argsNewRes']
        except KeyError:
          args = self.C_Policies[p]['args']
      else:
        commandIn = self.C_Policies[p]['commandIn']
        args = self.C_Policies[p]['args']

      polToEval_Args.append({'Name' : p, 'Module' : moduleName, 'args' : args,
                             'ConfirmationPolicy' : ConfirmationPolicy,
                             'commandIn' : commandIn})

    return polToEval_Args

################################################################################

  def __getPolTypes( self, granularity, statusType = None, status=None, 
                     formerStatus=None, newStatus=None, siteType=None, 
                     serviceType=None, resourceType=None ):
    """Get Policy Types from config that match the given keyword
    arguments"""

    # This dict is constructed to be used with function dictMatch that
    # helps selecting policies. **kwargs are not used due to the fact
    # that it's too dangerous here.
    argsdict = {'Granularity'  : granularity,
                'Status'       : status,
                'FormerStatus' : formerStatus,
                'NewStatus'    : newStatus,
                'SiteType'     : siteType,
                'ServiceType'  : serviceType,
                'ResourceType' : resourceType }

    pTconfig = getTypedDictRootedAt("PolicyTypes")

    pTypes = []

    for pt in pTconfig:
      if Utils.dictMatch(argsdict, pTconfig[pt]):
        pTypes.append(pt)

    for pt_name in pTypes:
      if 'Alarm_PolType' in pt_name:
        pTypes.remove(pt_name)
        pTypes.append('Alarm_PolType')

    return pTypes

################################################################################

  def __getPanelsInfo( self, granularity, statusType = None, status = None, 
                       formerStatus = None, siteType = None, serviceType = None, 
                       resourceType = None, panel_name = None, useNewRes = False ):

    info = []

    # First, select only policies we want.
    argsdict = {'Granularity': granularity,
                'Status': status,
                'FormerStatus': formerStatus,
                'SiteType': siteType,
                'ServiceType': serviceType,
                'ResourceType': resourceType}

    all_policies = getTypedDictRootedAt("Policies")
    selected_policies = []
    for p in all_policies:
      if Utils.dictMatch(argsdict, all_policies[p]):
        selected_policies.append(p)

    for p in selected_policies:                   # For selected policies
      if panel_name in self.C_Policies[p].keys(): # For selected panel_name (arguments)

        toAppend = copy.deepcopy(self.C_Policies[p][panel_name]) # type(toAppend) = list

        # Put CommandIn and args to correct values according to useNewRes
        if useNewRes:
          for panel in toAppend:
            for info_type in panel.keys():

              if type(panel[info_type]) == dict:
                try:
                  panel[info_type]['CommandIn'] = panel[info_type]['CommandInNewRes']
                  del panel[info_type]['CommandInNewRes']
                except KeyError:
                  pass
                try:
                  panel[info_type]['args'] = panel[info_type]['argsNewRes']
                  del panel[info_type]['argsNewRes']
                except KeyError:
                  pass
        else:
          for panel in toAppend:
            for info_type in panel.keys():
              try:
                del panel[info_type]['CommandInNewRes']
              except KeyError:
                pass
              try:
                del panel[info_type]['argsNewRes']
              except KeyError:
                pass

        info.append({p:toAppend})

    return info

################################################################################

  def __getViewPanels( self, granularity ):
    if granularity is None:
      granularity = 'Site'
    return views_panels[granularity]

################################################################################
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #  
################################################################################

'''
  HOW DOES THIS WORK.
    
    will come soon...
'''
            
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
