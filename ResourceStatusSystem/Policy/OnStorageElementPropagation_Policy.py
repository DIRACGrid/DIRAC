""" The OnStorageElementPropagation_Policy module is a policy module used to update the status of
    a storage element, based on how it srm node interface is behaving in the RSS
"""

from DIRAC.ResourceStatusSystem.Policy.PolicyBase import PolicyBase

class OnStorageElementPropagation_Policy(PolicyBase):
  
  def evaluate(self):
    """ 
    Evaluate policy on SE Status, using args (tuple).
    Get Resources Site status. It is simply propagated.

    :returns:
        { 
          `SAT`:True|False, 
          `Status`:Active|Probing|Banned, 
          `Reason`:'SRM interface is Active|Probing|Banned'
        }
    """ 

    resourceStatus = super(OnStorageElementPropagation_Policy, self).evaluate()
      
    if resourceStatus is None:
      return {'SAT':None}

    if resourceStatus == 'Unknown':
      return {'SAT':'Unknown'}
      
    
    if resourceStatus == 'Banned' or self.oldStatus == 'Banned':
      if resourceStatus != self.oldStatus:
        self.result['SAT'] = True
      else:
        self.result['SAT'] = False
  
      self.result['Status'] = resourceStatus
      self.result['Reason'] = 'Node status: ' + resourceStatus
    else:
      self.result['SAT'] = None
    
    return self.result
  
  evaluate.__doc__ = PolicyBase.evaluate.__doc__ + evaluate.__doc__  