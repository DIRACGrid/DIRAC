########################################################################
# $HeadURL:  $
########################################################################

from DIRAC.ResourceStatusSystem.Utilities.Utils import *
from DIRAC.ResourceStatusSystem.Policy.Configurations import Policies, Policy_Types, views_panels
import copy

__RCSID__ = "$Id:  $"


class InfoGetter:
  """ Class InfoGetter is in charge of getting information from the RSS Configurations
  """

#############################################################################

  def __init__(self):
    """ Standard constructor
    """
  
#############################################################################

  def getInfoToApply(self, args, granularity, status = None, formerStatus = None, 
                     siteType = None, serviceType = None, resourceType = None, useNewRes = False):
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
      EVAL['Policies'] = self.__getPolToEval(granularity = granularity, status = status, 
                                                    formerStatus = formerStatus, siteType = siteType, 
                                                    serviceType = serviceType, resourceType = resourceType, 
                                                    useNewRes = useNewRes)
    
    if 'policyType' in args:
      EVAL['PolicyType'] = self.__getPolTypes(granularity = granularity, status = status, 
                                              formerStatus = formerStatus, siteType = siteType,
                                              serviceType = serviceType, resourceType = resourceType)
      
    if 'panel_info' in args:
      if granularity in ('Site', 'Sites'):
        info = 'Site_Panel'
      elif granularity in ('Service', 'Services'):
        if serviceType == 'Storage':
          info = 'Service_Storage_Panel'
        elif serviceType == 'Computing':
          info = 'Service_Computing_Panel'
        elif serviceType == 'Others':
          info = 'Service_Others_Panel'
      elif granularity in ('Resource', 'Resources'):
        info = 'Resource_Panel'
      elif granularity in ('StorageElement', 'StorageElements'):
        info = 'SE_Panel'
      EVAL['Info'] = self.__getPanelsInfo(granularity = granularity, status = status,
                                          formerStatus = formerStatus, siteType = siteType,
                                          serviceType = serviceType, resourceType = resourceType,
                                          panel_name = info, useNewRes = useNewRes)
    
    if 'view_info' in args:
      panels_info_dict = {}

      view_panels = self.__getViewPanels(granularity)
      print view_panels
      for panel in view_panels:
        panel_info = self.__getPanelsInfo(granularity = granularity, status = status,
                                          formerStatus = formerStatus, siteType = siteType,
                                          serviceType = serviceType, resourceType = resourceType,
                                          panel_name = panel, useNewRes = useNewRes)
        print panel_info
        panels_info_dict[panel] = panel_info
      
      EVAL['Panels'] = panels_info_dict
    
    return [EVAL]
  
      
#############################################################################
  
  def getNewPolicyType(self, granularity, newStatus):
    return self.__getPolTypes(granularity = granularity, newStatus = newStatus)
    
#############################################################################

  def __getPolToEval(self, granularity, status = None, formerStatus = None, 
                     siteType = None, serviceType = None, resourceType = None,
                     useNewRes = False):
  
    C_Policies = copy.deepcopy(Policies)

    pol_to_eval = []
    
    for p in C_Policies.keys():
      if granularity in C_Policies[p]['Granularity']:
        pol_to_eval.append(p)
        
        if status is not None:
          if status not in C_Policies[p]['Status']:
            pol_to_eval.remove(p)
        
        if formerStatus is not None:
          if formerStatus not in C_Policies[p]['FormerStatus']:
            try:
              pol_to_eval.remove(p)
            except Exception:
              continue
            
        if siteType is not None:
          if siteType not in C_Policies[p]['SiteType']:
            try:
              pol_to_eval.remove(p)
            except Exception:
              continue
            
        if serviceType is not None:
          if serviceType not in C_Policies[p]['ServiceType']:
            try:
              pol_to_eval.remove(p)
            except Exception:
              continue
            
        if resourceType is not None:
          if resourceType not in C_Policies[p]['ResourceType']:
            try:
              pol_to_eval.remove(p)
            except Exception:
              continue
    
    polToEval_Args = []
    
    for p in pol_to_eval:
      args = C_Policies[p]['args']
      if useNewRes:
        try:
          commandIn = C_Policies[p]['commandInNewRes']
        except:
          commandIn = C_Policies[p]['commandIn']
      else:
        commandIn = C_Policies[p]['commandIn']
      polToEval_Args.append({'Name' : p, 'args' : args, 'commandIn' : commandIn})
    
    return polToEval_Args
  
  
#############################################################################

  def __getPolTypes(self, granularity, status = None, formerStatus = None, newStatus = None,  
                    siteType = None, serviceType = None, resourceType = None):
          
    C_Policy_Types = copy.deepcopy(Policy_Types)

    pTypes = [] 
     
    for pt in C_Policy_Types.keys():
      if granularity in C_Policy_Types[pt]['Granularity']:
        pTypes.append(pt)  
    
        if status is not None:
          if status not in C_Policy_Types[pt]['Status']:
            pTypes.remove(pt)
        
        if formerStatus is not None:
          if formerStatus not in C_Policy_Types[pt]['FormerStatus']:
            try:
              pTypes.remove(pt)
            except Exception:
              continue
            
        if newStatus is not None:
          if newStatus not in C_Policy_Types[pt]['NewStatus']:
            try:
              pTypes.remove(pt)
            except Exception:
              continue
            
        if siteType is not None:
          if siteType not in C_Policy_Types[pt]['SiteType']:
            try:
              pTypes.remove(pt)
            except Exception:
              continue
            
        if serviceType is not None:
          if serviceType not in C_Policy_Types[pt]['ServiceType']:
            try:
              pTypes.remove(pt)
            except Exception:
              continue
            
        if resourceType is not None:
          if resourceType not in C_Policy_Types[pt]['ResourceType']:
            try:
              pTypes.remove(pt)
            except Exception:
              continue
              
    for pt_name in pTypes:
      if 'Alarm_PolType' in pt_name:
        pTypes.remove(pt_name)
        pTypes.append('Alarm_PolType')

    
    return pTypes
 
#############################################################################

  def __getPanelsInfo(self, granularity, status = None, formerStatus = None, siteType = None,
                      serviceType = None, resourceType = None, panel_name = None, useNewRes = False):
  
  
    C_Policies = copy.deepcopy(Policies)
    
    info = []
    
    for p in C_Policies.keys():
      if panel_name in C_Policies[p].keys():
        info.append({p:C_Policies[p][panel_name]})
        c = len(info) - 1
        if isinstance(info[c][p], list):
          
          if useNewRes == False:
            
#            print "sono in useNewRes == False:"
            
            for i in info[c][p]:
              for k in i.keys():
                if 'CommandNew' in i[k].keys():
#                  del i[k]['CommandNew']
                  pass
          elif useNewRes == True:

#            print "sono in useNewRes == True:"
            
            for i in info[c][p]:
#              print i
              for k in i.keys():
                if 'CommandNew' in i[k].keys():
                  i[k]['Command'] = i[k]['CommandNew']
#                  del i[k]['CommandNew']
        
        if granularity is not None:
          if granularity not in C_Policies[p]['Granularity']:
            try:
              info.remove({p: C_Policies[p][panel_name]})
            except Exception:
              continue
        
        if status is not None:
          if status not in C_Policies[p]['Status']:
            try:
              info.remove({p: C_Policies[p][panel_name]})
            except Exception:
              continue
        
        if formerStatus is not None:
          if formerStatus not in C_Policies[p]['FormerStatus']:
            try:
              info.remove({p: C_Policies[p][panel_name]})
            except Exception:
              continue
            
        if siteType is not None:
          if siteType not in C_Policies[p]['SiteType']:
            try:
              info.remove({p: C_Policies[p][panel_name]})
            except Exception:
              continue
            
        if serviceType is not None:
          if serviceType not in C_Policies[p]['ServiceType']:
            try:
              info.remove({p: C_Policies[p][panel_name]})
            except Exception:
              continue
            
        if resourceType is not None:
          if resourceType not in C_Policies[p]['ResourceType']:
            try:
              info.remove({p: C_Policies[p][panel_name]})
            except Exception:
              continue
    
    return info
  
#############################################################################

#  def __getViewPanels(self, view_name):
  def __getViewPanels(self, granularity):
    if granularity is None:
      granularity = 'Site'
    return copy.deepcopy(views_panels[granularity])

#############################################################################
  