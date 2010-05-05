""" The JobsEfficiency_Simple_Policy class is a policy class 
    that checks the efficiency of the pilots
"""

from DIRAC.ResourceStatusSystem.Policy.PolicyBase import PolicyBase

class JobsEfficiency_Simple_Policy(PolicyBase):
  
  def evaluate(self):
    """ 
    Evaluate policy on jobs stats, using args (tuple). 
      
    :returns:
      { 
        'SAT':True|False, 
        'Status':Active|Probing|Bad, 
        'Reason':'JobsEff:Good|JobsEff:Fair|JobsEff:Poor|JobsEff:Bad|JobsEff:Idle',
      }
  """ 
    
    status = super(JobsEfficiency_Simple_Policy, self).evaluate()

    if status == 'Unknown':
      return {'SAT':'Unknown'}

    self.result['Reason'] = 'Simple Jobs Efficiency: '

    if self.oldStatus == 'Active':
      if status == 'Good':
        self.result['SAT'] = False
        self.result['Status'] = 'Active'
      elif status == 'Fair':
        self.result['SAT'] = False
        self.result['Status'] = 'Active'
      elif status == 'Poor':
        self.result['SAT'] = True
        self.result['Status'] = 'Probing'
      elif status == 'Idle':
        self.result['SAT'] = None
      elif status == 'Bad':
        self.result['SAT'] = True
        self.result['Status'] = 'Bad'

    elif self.oldStatus == 'Probing':
      if status == 'Good':
        self.result['SAT'] = True
        self.result['Status'] = 'Active'
      elif status == 'Fair':
        self.result['SAT'] = True
        self.result['Status'] = 'Active'
      elif status == 'Poor':
        self.result['SAT'] = False
        self.result['Status'] = 'Probing'
      elif status == 'Idle':
        self.result['SAT'] = None
      elif status == 'Bad':
        self.result['SAT'] = True
        self.result['Status'] = 'Bad'
    
    elif self.oldStatus == 'Bad':
      if status == 'Good':
        self.result['SAT'] = True
        self.result['Status'] = 'Active'
      elif status == 'Fair':
        self.result['SAT'] = True
        self.result['Status'] = 'Active'
      elif status == 'Poor':
        self.result['SAT'] = True
        self.result['Status'] = 'Probing'
      elif status == 'Idle':
        self.result['SAT'] = None
      elif status == 'Bad':
        self.result['SAT'] = False
        self.result['Status'] = 'Bad'

    elif self.oldStatus == 'Banned':
      if status == 'Good':
        self.result['SAT'] = True
        self.result['Status'] = 'Active'
      elif status == 'Fair':
        self.result['SAT'] = True
        self.result['Status'] = 'Active'
      elif status == 'Poor':
        self.result['SAT'] = True
        self.result['Status'] = 'Probing'
      elif status == 'Idle':
        self.result['SAT'] = None
      elif status == 'Bad':
        self.result['SAT'] = True
        self.result['Status'] = 'Bad'

    if status != 'Idle':
      self.result['Reason'] = self.result['Reason'] + status
    
    return self.result
  
  evaluate.__doc__ = PolicyBase.evaluate.__doc__ + evaluate.__doc__