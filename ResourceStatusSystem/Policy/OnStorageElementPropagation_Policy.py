""" The OnStorageElementPropagation_Policy module is a policy module used to update the status of
    a storage element, based on how it srm node interface is behaving in the RSS
"""

from DIRAC.ResourceStatusSystem.Policy.PolicyBase import PolicyBase
from DIRAC.ResourceStatusSystem.Utilities.Exceptions import *
from DIRAC.ResourceStatusSystem.Utilities.Utils import *

class OnStorageElementPropagation_Policy(PolicyBase):
  
  def evaluate(self, args, commandIn=None, knownInfo=None):
    """ Evaluate policy on SE Status, using args (tuple).
        Get Resources Site status. It is simply propagated.
        
        :params:
          :attr:`args`: a tuple
            `args[0]` should be a ValidRes (just 'StorageElement' - is ignored!)
           
            `args[1]` should be the name of the SE

            `args[2]` should be the present status
          
          :attr:`commandIn`: optional command object
          
          :attr:`knownInfo`: optional information dictionary
        
        :returns:
            { 
              `SAT`:True|False, 
              `Status`:Active|Probing|Banned, 
              `Reason`:'SRM interface is Active|Probing|Banned'
            }
    """ 

    if not isinstance(args, tuple):
      raise TypeError, where(self, self.evaluate)
    
    if args[2] not in ValidStatus:
      raise InvalidStatus, where(self, self.evaluate)

    #get resources stats
    if knownInfo is not None and 'Monitoredtatus' in knownInfo.keys():
      resourceStatus = knownInfo['MonitoredStatus']
    else:
      if commandIn is not None:
        command = commandIn
      else:
        # use standard Commands 
        from DIRAC.ResourceStatusSystem.Client.Command.RS_Command import MonitoredStatus_Command
        command = MonitoredStatus_Command()
        
      res = command.doCommand(('StorageElement', args[1], 'Resource'))
      
      resourceStatus = res['MonitoredStatus']
      
      if resourceStatus is None:
        return {'SAT':None}
      
    resourcesStatus = 'Active'
    
    result = {}
    
    if resourceStatus != args[2]:
      result['SAT'] = True
    else:
      result['SAT'] = False

    result['Status'] = resourceStatus
    result['Reason'] = 'Node status: ' + resourceStatus
    
    return result
    
    