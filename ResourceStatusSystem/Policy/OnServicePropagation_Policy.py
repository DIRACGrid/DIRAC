""" The OnServicePropagation_Policy module is a policy module used to update the status of
    the service, based on statistics of nodes for that service, and of it site
"""

from DIRAC.ResourceStatusSystem.Policy.PolicyBase import PolicyBase
from DIRAC.ResourceStatusSystem.Client.Command.ClientsInvoker import ClientsInvoker
from DIRAC.ResourceStatusSystem.Utilities.Exceptions import *
from DIRAC.ResourceStatusSystem.Utilities.Utils import *

class OnServicePropagation_Policy(PolicyBase):
  
  def evaluate(self, args, commandIn_1=None, commandIn_2=None, knownInfo=None):
    """ Evaluate policy on Service Status, using args (tuple). 
        
        :params:
          :attr:`args`: a tuple 
            `args[0]` should be the name of the service
            `args[1]` should be the present status
          
          :attr:`commandIn`: optional command object
          
          :attr:`knownInfo`: optional information dictionary
        
        :returns:
            { 
              `SAT`:True|False, 
              `Status`:Active|Probing|Banned, 
              `Reason`:'x out of y nodes in xxxx status' or 'Site xxxx in yyyy status'
            }
    """ 

    if not isinstance(args, tuple):
      raise TypeError, where(self, self.evaluate)
    
    if args[1] not in ValidStatus:
      raise InvalidStatus, where(self, self.evaluate)

    #get resource stats
    if knownInfo is not None and 'ResourceStats' in knownInfo.keys():
      resourceStats = knownInfo['ResourceStats']
    else:
      if commandIn_1 is not None:
        command = commandIn_1
      else:
        # use standard Command
        from DIRAC.ResourceStatusSystem.Client.Command.Propagation_Command import ResourceStats_Command
        command = ResourceStats_Command()
        
      clientsInvoker = ClientsInvoker()
      clientsInvoker.setCommand(command)
      resourceStats = clientsInvoker.doCommand(('Service', args[0]))
      
    resourcesStatus = 'Active'
    
    if resourceStats['Total'] != 0:
      if resourceStats['Active'] == 0:
        if resourceStats['Probing'] == 0:
          resourcesStatus = 'Banned'
        else:
          resourcesStatus = 'Probing'
    
    if knownInfo is not None and 'SiteStatus' in knownInfo.keys():
      siteStatus = knownInfo['SiteStatus']
    else:
      if commandIn_2 is not None:
        command = commandIn_2
        clientsInvoker = ClientsInvoker()
        clientsInvoker.setCommand(command)
        siteStatus = clientsInvoker.doCommand((args[0], ))
      else:
        # use standard way to get Site status
        from DIRAC.ResourceStatusSystem.DB.ResourceStatusDB import ResourceStatusDB
        rsDB = ResourceStatusDB()
        siteName = rsDB.getGeneralName(args[0], 'Service', 'Site')
        siteStatus = rsDB.getMonitoredsStatusWeb('Site', 
                                                 {'SiteName':siteName}, [], 0, 1)['Records'][0][4]
    
    result = {}
    
    if args[1] == 'Active':
      if siteStatus == 'Active' and resourcesStatus == 'Active':
        result['SAT'] = False
        result['Status'] = 'Active'
        result['Reason'] = 'Status propagated'
      if siteStatus == 'Probing' or resourcesStatus == 'Probing':
        result['SAT'] = True
        result['Status'] = 'Probing'
        result['Reason'] = 'Status propagated'
      if siteStatus == 'Banned' or resourcesStatus == 'Banned':
        result['SAT'] = True
        result['Status'] = 'Banned'
        result['Reason'] = 'Status propagated'
        
    elif args[1] == 'Probing':
      if siteStatus == 'Active' and resourcesStatus == 'Active':
        result['SAT'] = True
        result['Status'] = 'Active'
        result['Reason'] = 'Status propagated'
      if siteStatus == 'Probing' or resourcesStatus == 'Probing':
        result['SAT'] = False
        result['Status'] = 'Probing'
        result['Reason'] = 'Status propagated'
      if siteStatus == 'Banned' or resourcesStatus == 'Banned':
        result['SAT'] = True
        result['Status'] = 'Banned'
        result['Reason'] = 'Status propagated'
        
    elif args[1] == 'Banned':
      if siteStatus == 'Active' and resourcesStatus == 'Active':
        result['SAT'] = True
        result['Status'] = 'Active'
        result['Reason'] = 'Status propagated'
      if siteStatus == 'Probing' or resourcesStatus == 'Probing':
        result['SAT'] = True
        result['Status'] = 'Probing'
        result['Reason'] = 'Status propagated'
      if siteStatus == 'Banned' or resourcesStatus == 'Banned':
        result['SAT'] = False
        result['Status'] = 'Banned'
        result['Reason'] = 'Status propagated'
        
    
    return result
