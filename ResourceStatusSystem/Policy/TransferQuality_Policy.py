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
        
      if quality < Configurations.Transfer_QUALITY_LOW :
        self.result['Status'] = 'Probing'
        self.result['Reason'] = 'TransferQuality:Low'
      elif quality >= Configurations.Transfer_QUALITY_HIGH :
        self.result['Status'] = 'Active'
        self.result['Reason'] = 'TransferQuality:High'
      else:   
        self.result['Status'] = 'Active'
        self.result['Reason'] = 'TransferQuality:Mean'

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
        
      if quality < Configurations.Transfer_QUALITY_LOW :
        self.result['Status'] = 'Bad'
        self.result['Reason'] = 'TransferQuality:Low'
      elif quality >= Configurations.Transfer_QUALITY_HIGH :
        self.result['Status'] = 'Active'
        self.result['Reason'] = 'TransferQuality:High'
      elif quality >= Configurations.Transfer_QUALITY_LOW and quality < Configurations.Transfer_QUALITY_HIGH:   
        self.result['Status'] = 'Probing'
        self.result['Reason'] = 'TransferQuality:Mean'
        
    return self.result

  evaluate.__doc__ = PolicyBase.evaluate.__doc__ + evaluate.__doc__