########################################################################
# $HeadURL:  $
########################################################################

from DIRAC.ResourceStatusSystem.Utilities.Utils import *
from DIRAC.ResourceStatusSystem.Policy import Configurations

__RCSID__ = "$Id:  $"


class InfoGetter:
  """ Class InfoGetter is in charge of getting information from the RSS Configurations
  """

#############################################################################

  def __init__(self):
    """ Standard constructor
    """
    pass
  
#############################################################################

  def getInfoToApply(self, args, granularity, status = None, formerStatus = None, 
                      siteType = None, serviceType = None, resourceType = None, info = None ):
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
          
          :attr:`info`: info only valid for the panels (panel name)
    """
    
    EVAL = {}
    
    if 'policy' in args:
      EVAL['Policies'] = self.__getPolToEval(granularity = granularity, status = status, 
                                             formerStatus = formerStatus, siteType = siteType, 
                                             serviceType = serviceType, resourceType = resourceType)
    
    if 'policyType' in args:
      EVAL['PolicyType'] = self.__getPolTypes(granularity = granularity, status = status, 
                                              formerStatus = formerStatus, siteType = siteType,
                                              serviceType = serviceType, resourceType = resourceType)
      
    if 'panel_info' in args:
      EVAL['Info'] = self.__getPanelsInfo(granularity = granularity, status = status,
                                          formerStatus = formerStatus, siteType = siteType,
                                          serviceType = serviceType, resourceType = resourceType,
                                          panel_name = info)
    
    if 'view_info' in args:
      panels_info_dict = {}
      
      view_panels = self.__getViewPanels(info)
      
      for panel in view_panels:
        panel_info = self.__getPanelsInfo(granularity = granularity, status = status,
                                          formerStatus = formerStatus, siteType = siteType,
                                          serviceType = serviceType, resourceType = resourceType,
                                          panel_name = panel)
        panels_info_dict[panel] = panel_info
      
      EVAL['Panels'] = panels_info_dict
    
    return [EVAL]
  
      
#############################################################################
  
  def getNewPolicyType(self, granularity, newStatus):
    return self.__getPolTypes(granularity = granularity, newStatus = newStatus)
    
#############################################################################

  def __getPolToEval(self, granularity, status = None, formerStatus = None, 
                       siteType = None, serviceType = None, resourceType = None ):
  
    pol_to_eval = []
    
    for p in Configurations.Policies.keys():
      if granularity in Configurations.Policies[p]['Granularity']:
        pol_to_eval.append(p)
        
        if status is not None:
          if status not in Configurations.Policies[p]['Status']:
            pol_to_eval.remove(p)
        
        if formerStatus is not None:
          if formerStatus not in Configurations.Policies[p]['FormerStatus']:
            try:
              pol_to_eval.remove(p)
            except Exception:
              continue
            
        if siteType is not None:
          if siteType not in Configurations.Policies[p]['SiteType']:
            try:
              pol_to_eval.remove(p)
            except Exception:
              continue
            
        if serviceType is not None:
          if serviceType not in Configurations.Policies[p]['ServiceType']:
            try:
              pol_to_eval.remove(p)
            except Exception:
              continue
            
        if resourceType is not None:
          if resourceType not in Configurations.Policies[p]['ResourceType']:
            try:
              pol_to_eval.remove(p)
            except Exception:
              continue
    
    polToEval_Args = []
    
    for p in pol_to_eval:
      args = Configurations.Policies[p]['args']
      commandIn = Configurations.Policies[p]['commandIn']
      polToEval_Args.append({'Name' : p, 'args' : args, 'commandIn' : commandIn})
    
    return polToEval_Args
  
  
#############################################################################

  def __getPolTypes(self, granularity, status = None, formerStatus = None, newStatus = None,  
                    siteType = None, serviceType = None, resourceType = None ):
          
    pol_types = [] 
     
    for pt in Configurations.Policy_Types.keys():
      if granularity in Configurations.Policy_Types[pt]['Granularity']:
        pol_types.append(pt)  
    
        if status is not None:
          if status not in Configurations.Policy_Types[pt]['Status']:
            pol_types.remove(pt)
        
        if formerStatus is not None:
          if formerStatus not in Configurations.Policy_Types[pt]['FormerStatus']:
            try:
              pol_types.remove(pt)
            except Exception:
              continue
            
        if newStatus is not None:
          if newStatus not in Configurations.Policy_Types[pt]['NewStatus']:
            try:
              pol_types.remove(pt)
            except Exception:
              continue
            
        if siteType is not None:
          if siteType not in Configurations.Policy_Types[pt]['SiteType']:
            try:
              pol_types.remove(pt)
            except Exception:
              continue
            
        if serviceType is not None:
          if serviceType not in Configurations.Policy_Types[pt]['ServiceType']:
            try:
              pol_types.remove(pt)
            except Exception:
              continue
            
        if resourceType is not None:
          if resourceType not in Configurations.Policy_Types[pt]['ResourceType']:
            try:
              pol_types.remove(pt)
            except Exception:
              continue
              
    for pt_name in pol_types:
      if 'Alarm_PolType' in pt_name:
        pol_types.remove(pt_name)
        pol_types.append('Alarm_PolType')

    
    return pol_types
 
#############################################################################

  def __getPanelsInfo(self, granularity, status = None, formerStatus = None, siteType = None,
                      serviceType = None, resourceType = None, panel_name = None ):
  
  
    info = []
    
    for p in Configurations.Policies.keys():
      if panel_name in Configurations.Policies[p].keys():
        info.append({p: Configurations.Policies[p][panel_name]})
        
        if granularity is not None:
          if granularity not in Configurations.Policies[p]['Granularity']:
            try:
              info.remove({p: Configurations.Policies[p][panel_name]})
            except Exception:
              continue
        
        if status is not None:
          if status not in Configurations.Policies[p]['Status']:
            try:
              info.remove({p: Configurations.Policies[p][panel_name]})
            except Exception:
              continue
        
        if formerStatus is not None:
          if formerStatus not in Configurations.Policies[p]['FormerStatus']:
            try:
              info.remove({p: Configurations.Policies[p][panel_name]})
            except Exception:
              continue
            
        if siteType is not None:
          if siteType not in Configurations.Policies[p]['SiteType']:
            try:
              info.remove({p: Configurations.Policies[p][panel_name]})
            except Exception:
              continue
            
        if serviceType is not None:
          if serviceType not in Configurations.Policies[p]['ServiceType']:
            try:
              info.remove({p: Configurations.Policies[p][panel_name]})
            except Exception:
              continue
            
        if resourceType is not None:
          if resourceType not in Configurations.Policies[p]['ResourceType']:
            try:
              info.remove({p: Configurations.Policies[p][panel_name]})
            except Exception:
              continue
    
    return info
  
#############################################################################

  def __getViewPanels(self, view_name):
    return Configurations.views_panels[view_name]

#############################################################################
  