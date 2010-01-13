""" The OnSENodePropagation_Policy module is a policy module used to update the status of
    the resource SE, based on statistics of its StorageElements
"""

from DIRAC.ResourceStatusSystem.Policy.PolicyBase import PolicyBase
from DIRAC.ResourceStatusSystem.Client.Command.ClientsInvoker import ClientsInvoker
from DIRAC.ResourceStatusSystem.Utilities.Exceptions import *
from DIRAC.ResourceStatusSystem.Utilities.Utils import *

class OnSENodePropagation_Policy(PolicyBase):
  
  def evaluate(self, args, commandIn=None, knownInfo=None):
    """ Evaluate policy on SE nodes status, using args (tuple). 
        
        :params:
          :attr:`args`: a tuple 
            `args[0]` should be the name of the SE node
            `args[1]` should be the present status
          
          :attr:`commandIn`: optional command object
          
          :attr:`knownInfo`: optional information dictionary
        
        :returns:
            { 
              `SAT`:True|False, 
              `Status`:Active|Probing|Banned, 
              `Reason`:'SE: A:%d/P:%d/B:%d'
            }
    """ 

    if not isinstance(args, tuple):
      raise TypeError, where(self, self.evaluate)
    
    if args[1] not in ValidStatus:
      raise InvalidStatus, where(self, self.evaluate)

    #get resource stats
    if knownInfo is not None and 'SEStats' in knownInfo.keys():
      storageElementsStats = knownInfo['SEStats']
    else:
      if commandIn is not None:
        command = commandIn
      else:
        # use standard Command
        from DIRAC.ResourceStatusSystem.Client.Command.Propagation_Command import StorageElementsStats_Command
        command = StorageElementsStats_Command()
        
      clientsInvoker = ClientsInvoker()
      clientsInvoker.setCommand(command)
      storageElementsStats = clientsInvoker.doCommand(('Resource', args[0]))
      
    storageElementsStatus = 'Active'
    
    if storageElementsStats['Total'] != 0:
      if storageElementsStats['Active'] == 0:
        if storageElementsStats['Probing'] == 0:
          storageElementsStatus = 'Banned'
        else:
          storageElementsStatus = 'Probing'
    
    result = {}
    
    if args[1] == 'Active':
      if storageElementsStatus == 'Active':
        result['SAT'] = False
        result['Status'] = 'Active'
        result['Reason'] =  'SEs: A:%d/P:%d/B:%d' %(storageElementsStats['Active'], 
                                                    storageElementsStats['Probing'],
                                                    storageElementsStats['Banned'])
      elif storageElementsStatus == 'Probing':
        result['SAT'] = True
        result['Status'] = 'Probing'
        result['Reason'] =  'SEs: A:%d/P:%d/B:%d' %(storageElementsStats['Active'], 
                                                    storageElementsStats['Probing'],
                                                    storageElementsStats['Banned'])
      elif storageElementsStatus == 'Banned':
        result['SAT'] = True
        result['Status'] = 'Banned'
        result['Reason'] =  'SEs: A:%d/P:%d/B:%d' %(storageElementsStats['Active'], 
                                                    storageElementsStats['Probing'],
                                                    storageElementsStats['Banned'])
        
    elif args[1] == 'Probing':
      if storageElementsStatus == 'Active':
        result['SAT'] = True
        result['Status'] = 'Active'
        result['Reason'] =  'SEs: A:%d/P:%d/B:%d' %(storageElementsStats['Active'], 
                                                    storageElementsStats['Probing'],
                                                    storageElementsStats['Banned'])
      elif storageElementsStatus == 'Probing':
        result['SAT'] = False
        result['Status'] = 'Probing'
        result['Reason'] =  'SEs: A:%d/P:%d/B:%d' %(storageElementsStats['Active'], 
                                                    storageElementsStats['Probing'],
                                                    storageElementsStats['Banned'])
      elif storageElementsStatus == 'Banned':
        result['SAT'] = True
        result['Status'] = 'Banned'
        result['Reason'] =  'SEs: A:%d/P:%d/B:%d' %(storageElementsStats['Active'], 
                                                    storageElementsStats['Probing'],
                                                    storageElementsStats['Banned'])
        
    elif args[1] == 'Banned':
      if storageElementsStatus == 'Active':
        result['SAT'] = True
        result['Status'] = 'Active'
        result['Reason'] =  'SEs: A:%d/P:%d/B:%d' %(storageElementsStats['Active'], 
                                                    storageElementsStats['Probing'],
                                                    storageElementsStats['Banned'])
      elif storageElementsStatus == 'Probing':
        result['SAT'] = True
        result['Status'] = 'Probing'
        result['Reason'] =  'SEs: A:%d/P:%d/B:%d' %(storageElementsStats['Active'], 
                                                    storageElementsStats['Probing'],
                                                    storageElementsStats['Banned'])
      elif storageElementsStatus == 'Banned':
        result['SAT'] = False
        result['Status'] = 'Banned'
        result['Reason'] =  'SEs: A:%d/P:%d/B:%d' %(storageElementsStats['Active'], 
                                                    storageElementsStats['Probing'],
                                                    storageElementsStats['Banned'])
        
    
    return result
