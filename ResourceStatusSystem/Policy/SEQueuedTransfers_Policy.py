""" The SEQueuedTransfers_Policy class is a policy class satisfied when a SE has a high number of
    queued transfers.
"""

from DIRAC.ResourceStatusSystem.Policy.PolicyBase import PolicyBase

class SEQueuedTransfers_Policy(PolicyBase):
  
  def evaluate(self):
    """ 
    Evaluate policy on SE Queued Transfers, using args (tuple). 
        
    :returns:
        { 
          'SAT':True|False, 
          'Status':Active|Probing|Bad, 
          'Reason':'QueuedTransfers:High'|'QueuedTransfers:Mid-High'|'QueuedTransfers:Low',
        }
    """ 

    status = super(SEQueuedTransfers_Policy, self).evaluate()

    if status is None or status == -1:
      return {'SAT': None}

    if status == 'Unknown':
      return {'SAT':'Unknown'}
    
    status = int(round(status['Queued transfers']))
    
    if self.oldStatus == 'Active':
      if status > 100:
        self.result['SAT'] = True
        self.result['Status'] = 'Bad'
      elif status < 70:
        self.result['SAT'] = False
        self.result['Status'] = 'Active'
      else:
        self.result['SAT'] = True
        self.result['Status'] = 'Probing'
        
    elif self.oldStatus == 'Probing':
      if status > 100:
        self.result['SAT'] = True
        self.result['Status'] = 'Bad'
      elif status < 70:
        self.result['SAT'] = True
        self.result['Status'] = 'Active'
      else:
        self.result['SAT'] = False
        self.result['Status'] = 'Probing'
    
    elif self.oldStatus == 'Bad':
      if status > 100:
        self.result['SAT'] = False
        self.result['Status'] = 'Bad'
      elif status < 70:
        self.result['SAT'] = True
        self.result['Status'] = 'Active'
      else:
        self.result['SAT'] = True
        self.result['Status'] = 'Probing'
  
    elif self.oldStatus == 'Banned':
      if status > 100:
        self.result['SAT'] = True
        self.result['Status'] = 'Bad'
      elif status < 70:
        self.result['SAT'] = True
        self.result['Status'] = 'Active'
      else:
        self.result['SAT'] = True
        self.result['Status'] = 'Probing'
  
    
    if status is not None and status != -1:
    
      self.result['Reason'] = "Queued transfers on the SE: %d -> " %status
    
      if status > 100:
        str = 'HIGH'
      elif status < 70:
        str = 'Low'
      else:
        str = 'Mid-High'
      
      self.result['Reason'] = self.result['Reason'] + str
      
    return self.result
  
  evaluate.__doc__ = PolicyBase.evaluate.__doc__ + evaluate.__doc__