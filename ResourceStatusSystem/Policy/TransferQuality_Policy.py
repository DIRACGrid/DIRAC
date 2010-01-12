""" The DataQuality_Policy class is a policy class to check the data quality.
"""

from DIRAC.ResourceStatusSystem.Policy.PolicyBase import PolicyBase
from DIRAC.ResourceStatusSystem.Client.Command.ClientsInvoker import ClientsInvoker
from DIRAC.ResourceStatusSystem.Utilities.Exceptions import *
from DIRAC.ResourceStatusSystem.Utilities.Utils import *
from DIRAC.ResourceStatusSystem.Policy import Configurations

class TransferQuality_Policy(PolicyBase):
  
  def evaluate(self, args, commandIn=None, knownInfo=None):
    """ evaluate policy on Data quality, using args (tuple). 
        
        :params:
          :attr:`args`: a tuple 
            - `args[0]` should be the name of the SE
            - `args[1]` should be the present status
          
          :attr:`commandIn`: optional command object
          
          :attr:`knownInfo`: optional information dictionary
        
        
        :returns:
            { 
              'SAT':True|False, 
              'Status':Active|Probing|Banned, 
              'Reason':'TransferQuality:None'|'TransferQuality:xx%',
            }
    """ 

    if not isinstance(args, tuple):
      raise TypeError, where(self, self.evaluate)
    
    if args[1] not in ValidStatus:
      raise InvalidStatus, where(self, self.evaluate)

    if knownInfo is not None:
      if 'TransferQuality' in knownInfo.keys():
        quality = knownInfo
    else:
      if commandIn is not None:
        command = commandIn
      else:
        # use standard Command
        from DIRAC.ResourceStatusSystem.Client.Command.DataOperations_Command import TransferQuality_Command
        command = TransferQuality_Command()
        
      clientsInvoker = ClientsInvoker()
      clientsInvoker.setCommand(command)
      quality = clientsInvoker.doCommand((args[0], ))['TransferQuality']
    

    result = {}

    if args[1] == 'Active':
      if quality == None:
        result['SAT'] = None
      elif quality <= Configurations.SE_QUALITY_LOW :
        result['SAT'] = True
        result['Status'] = 'Banned'
        result['Reason'] = 'TransferQuality:Low'
      elif quality >= Configurations.SE_QUALITY_HIGH :
        result['SAT'] = False
        result['Status'] = 'Active'
        result['Reason'] = 'TransferQuality:High'
      elif quality > Configurations.SE_QUALITY_LOW and quality < Configurations.SE_QUALITY_HIGH:   
        result['SAT'] = True
        result['Status'] = 'Probing'
        result['Reason'] = 'TransferQuality:Mean'
    elif args[1] == 'Probing':
      if quality == None:
        result['SAT'] = None
      elif quality <= Configurations.SE_QUALITY_LOW :
        result['SAT'] = True
        result['Status'] = 'Banned'
        result['Reason'] = 'TransferQuality:Low'
      elif quality >= Configurations.SE_QUALITY_HIGH :
        result['SAT'] = True
        result['Status'] = 'Active'
        result['Reason'] = 'TransferQuality:High'
      elif quality > Configurations.SE_QUALITY_LOW and quality < Configurations.SE_QUALITY_HIGH:   
        result['SAT'] = False
        result['Status'] = 'Probing'
        result['Reason'] = 'TransferQuality:Mean'
    elif args[1] == 'Banned':
      if quality == None:
        result['SAT'] = None
      elif quality <= Configurations.SE_QUALITY_LOW :
        result['SAT'] = False
        result['Status'] = 'Banned'
        result['Reason'] = 'TransferQuality:Low'
      elif quality >= Configurations.SE_QUALITY_HIGH :
        result['SAT'] = True
        result['Status'] = 'Active'
        result['Reason'] = 'TransferQuality:High'
      elif quality > Configurations.SE_QUALITY_LOW and quality < Configurations.SE_QUALITY_HIGH:   
        result['SAT'] = True
        result['Status'] = 'Probing'
        result['Reason'] = 'TransferQuality:Mean'
        
    return result

