########################################################################
# $HeadURL:  $
########################################################################

__RCSID__ = "$Id:  $"

import copy

from DIRAC.ResourceStatusSystem.Utilities.CS    import getTypedDict
from DIRAC.ResourceStatusSystem.Utilities.Utils import dictMatch

class InfoGetter:
  """ Class InfoGetter is in charge of getting information from the RSS Configurations
  """

#############################################################################

  def __init__(self, VOExtension):
    """
    Standard constructor

    :params:
      :attr:`VOExtension`: string - VO extension (e.g. 'LHCb')
    """
    
    module = "DIRAC.ResourceStatusSystem."
    
    try:
      submodule    = 'Policy.Configurations'
      configModule = __import__( VOExtension + module + submodule , globals(), locals(), ['*'])
    except ImportError:
      submodule    = 'PolicySystem.Configurations'
      configModule = __import__( module + submodule , globals(), locals(), ['*'])

    self.C_Policies = copy.deepcopy(configModule.Policies)
    self.C_views_panels = copy.deepcopy(configModule.views_panels)

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
      EVAL['Policies'] = self.__getPolToEval( granularity = granularity, status = status,
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
        elif serviceType == 'VO-BOX':
          info = 'Service_VO-BOX_Panel'
        elif serviceType == 'VOMS':
          info = 'Service_VOMS_Panel'
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

      if granularity in ('Site', 'Sites'):
        granularity = None

      view_panels = self.__getViewPanels(granularity)
      for panel in view_panels:
        panel_info = self.__getPanelsInfo(granularity = granularity, status = status,
                                          formerStatus = formerStatus, siteType = siteType,
                                          serviceType = serviceType, resourceType = resourceType,
                                          panel_name = panel, useNewRes = useNewRes)
        panels_info_dict[panel] = panel_info

      EVAL['Panels'] = panels_info_dict

    return [EVAL]


#############################################################################

  def getNewPolicyType(self, granularity, newStatus):
    return self.__getPolTypes(granularity = granularity, newStatus = newStatus)

#############################################################################

  def __getPolToEval(self, useNewRes = False, **kwargs):

    pConfig = getTypedDict("Policies")
    pol_to_eval = []

    for p in pConfig:
      if dictMatch(kwargs, pConfig[p]):
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
#      args = self.C_Policies[p]['args']
      if useNewRes:
        try:
          commandIn = self.C_Policies[p]['commandInNewRes']
        except:
          commandIn = self.C_Policies[p]['commandIn']
      else:
        commandIn = self.C_Policies[p]['commandIn']

      if useNewRes:
        try:
          args = self.C_Policies[p]['argsNewRes']
        except:
          args = self.C_Policies[p]['args']
      else:
        args = self.C_Policies[p]['args']

      polToEval_Args.append({'Name' : p, 'Module' : moduleName, 'args' : args,
                             'ConfirmationPolicy' : ConfirmationPolicy,
                             'commandIn' : commandIn})

    return polToEval_Args


#############################################################################

  def __getPolTypes(self, **kwargs):
    """Get Policy Types from config that match the given keyword
    arguments"""
    pTconfig = getTypedDict("PolicyTypes")

    pTypes = []

    for pt in pTconfig:
      if dictMatch(kwargs, pTconfig[pt]):
        pTypes.append(pt)

    for pt_name in pTypes:
      if 'Alarm_PolType' in pt_name:
        pTypes.remove(pt_name)
        pTypes.append('Alarm_PolType')

    return pTypes

#############################################################################

  def __getPanelsInfo(self, granularity, status = None, formerStatus = None, siteType = None,
                      serviceType = None, resourceType = None, panel_name = None, useNewRes = False):

    info = []

    for p in self.C_Policies.keys():
      if panel_name in self.C_Policies[p].keys():

        toAppend = copy.deepcopy(self.C_Policies[p][panel_name])

        if not useNewRes:
          for i in range(len(toAppend)):
            for info_type in toAppend[i].keys():

              try:
                command = toAppend[i][info_type]['CommandIn']
                toAppend[i][info_type]['CommandIn'] = command
                del toAppend[i][info_type]['CommandInNewRes']
              except:
                pass
              try:
                args = toAppend[i][info_type]['args']
                toAppend[i][info_type]['args'] = args
                del toAppend[i][info_type]['argsNewRes']
              except:
                pass

        else:
          for i in range(len(toAppend)):
            for info_type in toAppend[i].keys():

              if isinstance(toAppend[i][info_type], dict):
                try:
                  command = toAppend[i][info_type]['CommandInNewRes']
                  del toAppend[i][info_type]['CommandInNewRes']
                except:
                  command = toAppend[i][info_type]['CommandIn']
                toAppend[i][info_type]['CommandIn'] = command


                try:
                  args = toAppend[i][info_type]['argsNewRes']
                  del toAppend[i][info_type]['argsNewRes']
                except:
                  args = toAppend[i][info_type]['args']
                toAppend[i][info_type]['args'] = args

        info.append({p:toAppend})

        if granularity is not None:
          if granularity not in self.C_Policies[p]['Granularity']:
            try:
              for x in info:
                if p in x.keys():
                  toRemove = x
              info.remove(toRemove)
            except Exception:
              continue

        if status is not None:
          if status not in self.C_Policies[p]['Status']:
            try:
              for x in info:
                if p in x.keys():
                  toRemove = x
              info.remove(toRemove)
            except Exception:
              continue

        if formerStatus is not None:
          if formerStatus not in self.C_Policies[p]['FormerStatus']:
            try:
              for x in info:
                if p in x.keys():
                  toRemove = x
              info.remove(toRemove)
            except Exception:
              continue

        if siteType is not None:
          if siteType not in self.C_Policies[p]['SiteType']:
            try:
              for x in info:
                if p in x.keys():
                  toRemove = x
              info.remove(toRemove)
            except Exception:
              continue

        if serviceType is not None:
          if serviceType not in self.C_Policies[p]['ServiceType']:
            try:
              for x in info:
                if p in x.keys():
                  toRemove = x
              info.remove(toRemove)
            except Exception:
              continue

        if resourceType is not None:
          if resourceType not in self.C_Policies[p]['ResourceType']:
            try:
              for x in info:
                if p in x.keys():
                  toRemove = x
              info.remove(toRemove)
            except Exception:
              continue

    return info

#############################################################################

  def __getViewPanels(self, granularity):
    if granularity is None:
      granularity = 'Site'
    return self.C_views_panels[granularity]

#############################################################################
