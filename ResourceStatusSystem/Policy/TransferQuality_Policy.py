""" The DataQuality_Policy class is a policy class to check the data quality.
"""

from DIRAC.ResourceStatusSystem.Policy.PolicyBase import PolicyBase
from DIRAC.ResourceStatusSystem.Policy import Configurations

class TransferQuality_Policy(PolicyBase):
  
  def evaluate(self):
    """ 
    Evaluate policy on Data quality. 
        
    :returns:
        { 
          'SAT':True|False, 
          'Status':Active|Probing|Banned, 
          'Reason':'TransferQuality:None'|'TransferQuality:xx%',
        }
    """ 

    quality = super(TransferQuality_Policy, self).evaluate()

    if quality == None:
      self.result['SAT'] = None
      return self.result
    elif quality == 'Unknown':
      return {'SAT':'Unknown'}

    if 'FAILOVER'.lower() in self.args[1].lower():
      if self.oldStatus == 'Active':
        if quality >= Configurations.Transfer_QUALITY_LOW :
          self.result['SAT'] = False
        else:   
          self.result['SAT'] = True
      elif self.oldStatus == 'Probing':
        if quality < Configurations.Transfer_QUALITY_LOW:
          self.result['SAT'] = False
        else:
          self.result['SAT'] = True
      else:
        self.result['SAT'] = True
        
      self.result['Reason'] = 'TransferQuality: %d -> ' %quality
      if quality < Configurations.Transfer_QUALITY_LOW :
        self.result['Status'] = 'Probing'
        strReason = 'Low'
      elif quality >= Configurations.Transfer_QUALITY_HIGH :
        self.result['Status'] = 'Active'
        strReason = 'High'
      else:   
        self.result['Status'] = 'Active'
        strReason = 'Mean'
        
      self.result['Reason'] = self.result['Reason'] + strReason

    else:
      if self.oldStatus == 'Active':
        if quality >= Configurations.Transfer_QUALITY_HIGH :
          self.result['SAT'] = False
        else:   
          self.result['SAT'] = True
      elif self.oldStatus == 'Probing':
        if quality >= Configurations.Transfer_QUALITY_LOW and quality < Configurations.Transfer_QUALITY_HIGH:
          self.result['SAT'] = False
        else:
          self.result['SAT'] = True
      elif self.oldStatus == 'Bad':
        if quality < Configurations.Transfer_QUALITY_LOW :
          self.result['SAT'] = False
        else:   
          self.result['SAT'] = True
      elif self.oldStatus == 'Banned':
        self.result['SAT'] = True
        
      self.result['Reason'] = 'TransferQuality: %d -> ' %quality
      if quality < Configurations.Transfer_QUALITY_LOW :
        self.result['Status'] = 'Bad'
        strReason = 'Low'
      elif quality >= Configurations.Transfer_QUALITY_HIGH :
        self.result['Status'] = 'Active'
        strReason = 'High'
      elif quality >= Configurations.Transfer_QUALITY_LOW and quality < Configurations.Transfer_QUALITY_HIGH:   
        self.result['Status'] = 'Probing'
        strReason = 'Mean'
        
      self.result['Reason'] = self.result['Reason'] + strReason

    return self.result

  evaluate.__doc__ = PolicyBase.evaluate.__doc__ + evaluate.__doc__