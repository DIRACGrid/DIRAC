""" The PilotsEfficiency_Simple_Policy class is a policy class 
    that checks the efficiency of the pilots
"""

from DIRAC.ResourceStatusSystem.Policy.PolicyBase import PolicyBase

class PilotsEfficiency_Simple_Policy(PolicyBase):
  
  def evaluate(self):
    """ 
    Evaluate policy on pilots stats, using args (tuple). 
        
    returns:
        { 
          'SAT':True|False,
           
          'Status':Active|Probing|Bad, 
          
          'Reason':'PilotsEff:low|PilotsEff:med|PilotsEff:good',
        }
    """ 
    
    status = super(PilotsEfficiency_Simple_Policy, self).evaluate()
    
    if status == 'Unknown':
      return {'SAT':'Unknown'}

    if status == None:
      return {'SAT':None}
    
    self.result['Reason'] = 'Simple pilots Efficiency: '
    
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