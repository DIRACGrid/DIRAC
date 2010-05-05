""" The Propagation_Policy module is a policy module used to update the status of
    a validRes, based on statistics of its services (for the site), 
    of its nodes (for the services), or of its SE (for the Storage services). 
"""

from DIRAC.ResourceStatusSystem.Policy.PolicyBase import PolicyBase

class Propagation_Policy(PolicyBase):
  
  def evaluate(self):
    """ 
    Propagation policy on Site or Service, using args (tuple).
    It will get Services or nodes or SE stats. 
  
    :returns:
      { 
      `SAT`:True|False, 
      `Status`:Active|Probing|Banned, 
      `Reason`:'A:X/P:Y/B:Z'
      }
    """ 
    
    stats = super(Propagation_Policy, self).evaluate()
    
    if stats == 'Unknown':
      return {'SAT':'Unknown'}
    
    if stats is None:
      return {'SAT':None}
    
    values = []
    try:
      val = (100 * stats['Active'] + 70 * stats['Probing'] + 30 * stats['Bad']) / stats['Total']
    except ZeroDivisionError:
      return {'SAT':None}
    
    if val == 100:
      status = 'Active'
    elif val == 0:
      status = 'Banned'
    else:
      if val >= 70:
        status = 'Probing'
      else:
        status = 'Bad'
      
    
    if self.args[2] == status:
      self.result['SAT'] = False
    else:
      self.result['SAT'] = True
    self.result['Status'] = status
    self.result['Reason'] =  '%s: Active:%d, Probing :%d, Bad: %d, Banned:%d' %(self.args[3], 
                                                                           stats['Active'], 
                                                                           stats['Probing'], 
                                                                           stats['Bad'], 
                                                                           stats['Banned'])
            
    return self.result

  evaluate.__doc__ = PolicyBase.evaluate.__doc__ + evaluate.__doc__