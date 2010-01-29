""" The OnServicePropagation_Policy module is a policy module used to update the status of
    the service, based on statistics of nodes for that service, and of it site
"""

from DIRAC.ResourceStatusSystem.Policy.PolicyBase import PolicyBase
from DIRAC.ResourceStatusSystem.Client.Command.MacroCommand import MacroCommand
from DIRAC.ResourceStatusSystem.Utilities.Exceptions import *
from DIRAC.ResourceStatusSystem.Utilities.Utils import *

class OnServicePropagation_Policy(PolicyBase):
  
  def evaluate(self, args, commandIn=None, knownInfo=None):
    """ Evaluate policy on Service Status, using args (tuple).
        Get Resources stats and Site status. 
        By standard, service status is active. If all the resources are banned, service status is banned.
        Otherwise, if there are no active resources but not all are banned, the status is probing.
        The site status is simply propagated.
        
        :params:
          :attr:`args`: a tuple
            `args[0]` should be a ValidRes (just 'Service' - is ignored!)
           
            `args[1]` should be the name of the service

            `args[2]` should be the present status
          
          :attr:`commandIn`: optional command object
          
          :attr:`knownInfo`: optional information dictionary
        
        :returns:
            { 
              `SAT`:True|False, 
              `Status`:Active|Probing|Banned, 
              `Reason`:'Site Active/Probing/Banned. Nodes: A:X/P:Y/B:Z'
            }
    """ 

    if not isinstance(args, tuple):
      raise TypeError, where(self, self.evaluate)
    
    if args[2] not in ValidStatus:
      raise InvalidStatus, where(self, self.evaluate)

    #get resources stats
    if knownInfo is not None and 'ResourceStats' in knownInfo.keys() and 'MonitoredStatus' in knownInfo.keys():
      resourceStats = knownInfo['ResourceStats']
      siteStatus = knownInfo['MonitoredStatus']
    else:
      if commandIn is not None:
        commandsList = commandIn
      else:
        # use standard Commands 
        from DIRAC.ResourceStatusSystem.Client.Command.RS_Command import ResourceStats_Command
        rs_c = ResourceStats_Command()
        
        from DIRAC.ResourceStatusSystem.Client.Command.RS_Command import MonitoredStatus_Command
        ms_c = MonitoredStatus_Command()
        
        commandsList = [rs_c, ms_c]
      
      #make a MacroCommand
      command = MacroCommand(commandsList)
      res = command.doCommand((args[0], args[1], 'Site'))
      
      resourceStats = res[0]['ResourceStats']
      siteStatus = res[1]['MonitoredStatus']
      
      if resourceStats is None or siteStatus is None:
        return {'SAT':None}
      
    resourcesStatus = 'Active'
    
    if resourceStats['Total'] != 0:
      if resourceStats['Active'] == 0:
        if resourceStats['Probing'] == 0:
          resourcesStatus = 'Banned'
        else:
          resourcesStatus = 'Probing'
    
    #get site status
#    if knownInfo is not None and 'SiteStatus' in knownInfo.keys():
#      siteStatus = knownInfo['SiteStatus']
#    else:
#      if commandIn_2 is not None:
#        command = commandIn_2
#        clientsInvoker = ClientsInvoker()
#        clientsInvoker.setCommand(command)
#        siteStatus = clientsInvoker.doCommand((args[0], args[1]))
#      else:
#        # use standard way to get Site status
#        from DIRAC.ResourceStatusSystem.DB.ResourceStatusDB import ResourceStatusDB
#        rsDB = ResourceStatusDB()
#        siteName = rsDB.getGeneralName(args[1], 'Service', 'Site')
#        siteStatus = rsDB.getMonitoredsStatusWeb('Site', 
#                                                 {'SiteName':siteName}, [], 0, 1)['Records'][0][4]
    
    result = {}
    
    if args[2] == 'Active':
      if siteStatus == 'Active' and resourcesStatus == 'Active':
        result['SAT'] = False
        result['Status'] = 'Active'
        result['Reason'] =  'Site "%s". Nodes: A:%d/P:%d/B:%d' %(siteStatus,
                                                                 resourceStats['Active'], 
                                                                 resourceStats['Probing'], 
                                                                 resourceStats['Banned'])
      if siteStatus == 'Probing' or resourcesStatus == 'Probing':
        result['SAT'] = True
        result['Status'] = 'Probing'
        result['Reason'] =  'Site "%s". Nodes: A:%d/P:%d/B:%d' %(siteStatus,
                                                                 resourceStats['Active'], 
                                                                 resourceStats['Probing'], 
                                                                 resourceStats['Banned'])
      if siteStatus == 'Banned' or resourcesStatus == 'Banned':
        result['SAT'] = True
        result['Status'] = 'Banned'
        result['Reason'] =  'Site "%s". Nodes: A:%d/P:%d/B:%d' %(siteStatus,
                                                                 resourceStats['Active'], 
                                                                 resourceStats['Probing'], 
                                                                 resourceStats['Banned'])
        
    elif args[2] == 'Probing':
      if siteStatus == 'Active' and resourcesStatus == 'Active':
        result['SAT'] = True
        result['Status'] = 'Active'
        result['Reason'] =  'Site "%s". Nodes: A:%d/P:%d/B:%d' %(siteStatus,
                                                                 resourceStats['Active'], 
                                                                 resourceStats['Probing'], 
                                                                 resourceStats['Banned'])
      if siteStatus == 'Probing' or resourcesStatus == 'Probing':
        result['SAT'] = False
        result['Status'] = 'Probing'
        result['Reason'] =  'Site "%s". Nodes: A:%d/P:%d/B:%d' %(siteStatus,
                                                                 resourceStats['Active'], 
                                                                 resourceStats['Probing'], 
                                                                 resourceStats['Banned'])
      if siteStatus == 'Banned' or resourcesStatus == 'Banned':
        result['SAT'] = True
        result['Status'] = 'Banned'
        result['Reason'] =  'Site "%s". Nodes: A:%d/P:%d/B:%d' %(siteStatus,
                                                                 resourceStats['Active'], 
                                                                 resourceStats['Probing'], 
                                                                 resourceStats['Banned'])
        
    elif args[2] == 'Banned':
      if siteStatus == 'Active' and resourcesStatus == 'Active':
        result['SAT'] = True
        result['Status'] = 'Active'
        result['Reason'] =  'Site "%s". Nodes: A:%d/P:%d/B:%d' %(siteStatus,
                                                                 resourceStats['Active'], 
                                                                 resourceStats['Probing'], 
                                                                 resourceStats['Banned'])
      if siteStatus == 'Probing' or resourcesStatus == 'Probing':
        result['SAT'] = True
        result['Status'] = 'Probing'
        result['Reason'] =  'Site "%s". Nodes: A:%d/P:%d/B:%d' %(siteStatus,
                                                                 resourceStats['Active'], 
                                                                 resourceStats['Probing'], 
                                                                 resourceStats['Banned'])
      if siteStatus == 'Banned' or resourcesStatus == 'Banned':
        result['SAT'] = False
        result['Status'] = 'Banned'
        result['Reason'] =  'Site "%s". Nodes: A:%d/P:%d/B:%d' %(siteStatus,
                                                                 resourceStats['Active'], 
                                                                 resourceStats['Probing'], 
                                                                 resourceStats['Banned'])
        
    
    return result
