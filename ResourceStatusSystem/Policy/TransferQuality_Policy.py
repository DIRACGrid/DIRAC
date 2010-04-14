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
            - `args[0]`: string - a ValidRes ('Site', 'Resource', 'StorageElements')

            - `args[1]`: string - should be the name of the SE

            - `args[2]`: string - should be the present status
          
            - args[3]: optional dateTime object: a "from" date
          
            - args[4]: optional dateTime object: a "to" date
          
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
    
    if args[2] not in ValidStatus:
      raise InvalidStatus, where(self, self.evaluate)

    if knownInfo is not None:
      if 'TransferQuality' in knownInfo.keys():
        quality = knownInfo['TransferQuality']
    else:
      if commandIn is not None:
        command = commandIn
      else:
        # use standard Command
        from DIRAC.ResourceStatusSystem.Client.Command.DataOperations_Command import TransferQuality_Command
        command = TransferQuality_Command()
        
      clientsInvoker = ClientsInvoker()
      clientsInvoker.setCommand(command)
      
      if len(args) == 3:
        quality = clientsInvoker.doCommand((args[0], args[1]))
      elif len(args) == 4:
        quality = clientsInvoker.doCommand((args[0], args[1], args[3]))
      elif len(args) == 5:
        quality = clientsInvoker.doCommand((args[0], args[1], args[3], args[4]))
      else:
        raise RSSException, where(self, self.evaluate)
    
      quality = quality['TransferQuality']

    result = {}

    if quality == None:
      result['SAT'] = None
      return result
    elif quality == 'Unknown':
      return {'SAT':'Unknown'}

    if 'FAILOVER'.lower() in args[1].lower():
      if args[2] == 'Active':
        if quality >= Configurations.Transfer_QUALITY_LOW :
          result['SAT'] = False
        else:   
          result['SAT'] = True
      elif args[2] == 'Probing':
        if quality < Configurations.Transfer_QUALITY_LOW:
          result['SAT'] = False
        else:
          result['SAT'] = True
      else:
        result['SAT'] = True
        
      if quality < Configurations.Transfer_QUALITY_LOW :
        result['Status'] = 'Probing'
        result['Reason'] = 'TransferQuality:Low'
      elif quality >= Configurations.Transfer_QUALITY_HIGH :
        result['Status'] = 'Active'
        result['Reason'] = 'TransferQuality:High'
      else:   
        result['Status'] = 'Active'
        result['Reason'] = 'TransferQuality:Mean'

    else:
      if args[2] == 'Active':
        if quality >= Configurations.Transfer_QUALITY_HIGH :
          result['SAT'] = False
        else:   
          result['SAT'] = True
      elif args[2] == 'Probing':
        if quality >= Configurations.Transfer_QUALITY_LOW and quality < Configurations.Transfer_QUALITY_HIGH:
          result['SAT'] = False
        else:
          result['SAT'] = True
      elif args[2] == 'Bad':
        if quality < Configurations.Transfer_QUALITY_LOW :
          result['SAT'] = False
        else:   
          result['SAT'] = True
      elif args[2] == 'Banned':
        result['SAT'] = True
        
      if quality < Configurations.Transfer_QUALITY_LOW :
        result['Status'] = 'Bad'
        result['Reason'] = 'TransferQuality:Low'
      elif quality >= Configurations.Transfer_QUALITY_HIGH :
        result['Status'] = 'Active'
        result['Reason'] = 'TransferQuality:High'
      elif quality >= Configurations.Transfer_QUALITY_LOW and quality < Configurations.Transfer_QUALITY_HIGH:   
        result['Status'] = 'Probing'
        result['Reason'] = 'TransferQuality:Mean'
        
    return result

