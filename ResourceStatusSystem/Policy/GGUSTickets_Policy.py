""" The SAMself.results_Policy class is a policy class that checks 
    the SAM job self.results
"""

from DIRAC.ResourceStatusSystem.Policy.PolicyBase import PolicyBase

class GGUSTickets_Policy(PolicyBase):
  
  def evaluate(self):
    """ 
    Evaluate policy on opened tickets, using args (tuple). 
        
    :returns:
        { 
          'SAT':True|False, 
          'Status':Active|Probing, 
          'Reason':'GGUSTickets: n unsolved',
        }
    """ 
      
    GGUS_N = super(GGUSTickets_Policy, self).evaluate()
    
    if GGUS_N == 'Unknown':
      return {'SAT':'Unknown'}
          
    if self.oldStatus == 'Active':
      if GGUS_N >= 1:
        self.result['SAT'] = True
      else:
        self.result['SAT'] = False
    elif self.oldStatus == 'Probing':
      if GGUS_N >= 1:
        self.result['SAT'] = False
      else:
        self.result['SAT'] = True
    else:
      self.result['SAT'] = True
    
    if GGUS_N >= 1:
      self.result['Status'] = 'Probing'
      self.result['Reason'] = 'GGUSTickets unsolved: %d' %(GGUS_N)
    else:
      self.result['Status'] = 'Active'
      self.result['Reason'] = 'NO GGUSTickets unsolved'
  
    return self.result
  
  evaluate.__doc__ = PolicyBase.evaluate.__doc__ + evaluate.__doc__