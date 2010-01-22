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
            `args[0]` should be a ValidRes (StorageElement only)

            `args[1]` should be the name of the SE node
            
            `args[2]` should be the present status
          
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
    
    if args[2] not in ValidStatus:
      raise InvalidStatus, where(self, self.evaluate)

    #get resource stats
    if knownInfo is not None and 'StorageElementStats' in knownInfo.keys():
      storageElementStats = knownInfo['StorageElementStats']
    else:
      if commandIn is not None:
        command = commandIn
      else:
        # use standard Command
        from DIRAC.ResourceStatusSystem.Client.Command.RS_Command import StorageElementsStats_Command
        command = StorageElementsStats_Command()
        
      clientsInvoker = ClientsInvoker()
      clientsInvoker.setCommand(command)
      storageElementStats = clientsInvoker.doCommand(('Resource', args[1]))
    
      
    storageElementStats = storageElementStats['StorageElementStats']
    
    storageElementStatus = 'Active'
    
    if storageElementStats['Total'] != 0:
      if storageElementStats['Active'] == 0:
        if storageElementStats['Probing'] == 0:
          storageElementStatus = 'Banned'
        else:
          storageElementStatus = 'Probing'
    
    result = {}
    
    if args[2] == 'Active':
      if storageElementStatus == 'Active':
        result['SAT'] = False
        result['Status'] = 'Active'
        result['Reason'] =  'SEs: A:%d/P:%d/B:%d' %(storageElementStats['Active'], 
                                                    storageElementStats['Probing'],
                                                    storageElementStats['Banned'])
      elif storageElementStatus == 'Probing':
        result['SAT'] = True
        result['Status'] = 'Probing'
        result['Reason'] =  'SEs: A:%d/P:%d/B:%d' %(storageElementStats['Active'], 
                                                    storageElementStats['Probing'],
                                                    storageElementStats['Banned'])
      elif storageElementStatus == 'Banned':
        result['SAT'] = True
        result['Status'] = 'Banned'
        result['Reason'] =  'SEs: A:%d/P:%d/B:%d' %(storageElementStats['Active'], 
                                                    storageElementStats['Probing'],
                                                    storageElementStats['Banned'])
        
    elif args[2] == 'Probing':
      if storageElementStatus == 'Active':
        result['SAT'] = True
        result['Status'] = 'Active'
        result['Reason'] =  'SEs: A:%d/P:%d/B:%d' %(storageElementStats['Active'], 
                                                    storageElementStats['Probing'],
                                                    storageElementStats['Banned'])
      elif storageElementStatus == 'Probing':
        result['SAT'] = False
        result['Status'] = 'Probing'
        result['Reason'] =  'SEs: A:%d/P:%d/B:%d' %(storageElementStats['Active'], 
                                                    storageElementStats['Probing'],
                                                    storageElementStats['Banned'])
      elif storageElementStatus == 'Banned':
        result['SAT'] = True
        result['Status'] = 'Banned'
        result['Reason'] =  'SEs: A:%d/P:%d/B:%d' %(storageElementStats['Active'], 
                                                    storageElementStats['Probing'],
                                                    storageElementStats['Banned'])
        
    elif args[2] == 'Banned':
      if storageElementStatus == 'Active':
        result['SAT'] = True
        result['Status'] = 'Active'
        result['Reason'] =  'SEs: A:%d/P:%d/B:%d' %(storageElementStats['Active'], 
                                                    storageElementStats['Probing'],
                                                    storageElementStats['Banned'])
      elif storageElementStatus == 'Probing':
        result['SAT'] = True
        result['Status'] = 'Probing'
        result['Reason'] =  'SEs: A:%d/P:%d/B:%d' %(storageElementStats['Active'], 
                                                    storageElementStats['Probing'],
                                                    storageElementStats['Banned'])
      elif storageElementStatus == 'Banned':
        result['SAT'] = False
        result['Status'] = 'Banned'
        result['Reason'] =  'SEs: A:%d/P:%d/B:%d' %(storageElementStats['Active'], 
                                                    storageElementStats['Probing'],
                                                    storageElementStats['Banned'])
        
    
    return result
