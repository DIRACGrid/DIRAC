""" The Res2SiteStatus_Policy class is a policy class that determines the status
    of the sites based on the status of their resources
"""

from DIRAC.ResourceStatusSystem.Policy.PolicyBase import PolicyBase
from DIRAC.ResourceStatusSystem.Client.Command.ClientsInvoker import ClientsInvoker
from DIRAC.ResourceStatusSystem.Utilities.Exceptions import *
from DIRAC.ResourceStatusSystem.Utilities.Utils import *

class Res2SiteStatus_Policy(PolicyBase):
  
  def evaluate(self, args, commandIn=None, knownInfo=None):
    """ evaluate status of a site, based on the status of its resources. 
        - args[0] should be the name of the the site
        - args[1] should be the present status
        
        returns:
            { 
              'SAT':True|False, 
              'Status':Active|Probing|Banned, 
              'Reason':'',
              'Enddate':datetime (if needed)
            }
    """ 

    if not isinstance(args, tuple):
      raise TypeError, where(self, self.evaluate)
    
    if args[1] not in ValidStatus:
      raise InvalidStatus, where(self, self.evaluate)

    if knownInfo is not None:
      if 'ResStat' in knownInfo.keys():
        status = knownInfo
    else:
      if commandIn is not None:
        command = commandIn
      else:
        # use standard Client
        from DIRAC.ResourceStatusSystem.Client.Command.Res2SiteStatus_Command import Res2SiteStatus_Command
        command = GOCDBStatus_Command()
        
      clientsInvoker = ClientsInvoker()
      clientsInvoker.setCommand(command)
      status = clientsInvoker.doCommand((args[0], args[1]))
    

    result = {}
    
#    if args[2] == 'Active':
#      if status == None:
#        result['SAT'] = False
#        result['Reason'] = 'DT:None'
#      else:
#        if status['DT'] == 'OUTAGE':
#          result['SAT'] = True
#          result['Status'] = 'Banned'
#          result['Reason'] = 'DT:OUTAGE'
#          result['Enddate'] = status['Enddate']
#        elif status['DT'] == 'AT_RISK':
#          result['SAT'] = True
#          result['Status'] = 'Probing'
#          result['Reason'] = 'DT:AT_RISK'
#          result['Enddate'] = status['Enddate']
#    
#    elif args[2] == 'Probing':
#      if status == None:
#        result['SAT'] = True
#        result['Status'] = 'Active'
#        result['Reason'] = 'DT:None'
#      else:
#        if status['DT'] == 'OUTAGE':
#          result['SAT'] = True
#          result['Status'] = 'Banned'
#          result['Reason'] = 'DT:OUTAGE'
#          result['Enddate'] = status['Enddate']
#        elif status['DT'] == 'AT_RISK':
#          result['SAT'] = False
#          result['Reason'] = 'DT:AT_RISK'
#      
#    elif args[2] == 'Banned':
#      if status == None:
#        result['SAT'] = True
#        result['Status'] = 'Active'
#        result['Reason'] = 'DT:None'
#      else:
#        if status['DT'] == 'OUTAGE':
#          result['SAT'] = False
#          result['Reason'] = 'DT:OUTAGE'
#        elif status['DT'] == 'AT_RISK':
#          result['SAT'] = True
#          result['Status'] = 'Probing'
#          result['Reason'] = 'DT:AT_RISK'
#          result['Enddate'] = status['Enddate']
    
    
    return result
