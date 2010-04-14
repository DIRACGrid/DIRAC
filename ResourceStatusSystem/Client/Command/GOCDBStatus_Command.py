""" The GOCDBStatus_Command class is a command class to know about 
    present downtimes
"""

import urllib2

from DIRAC import gLogger

from DIRAC.ResourceStatusSystem.Client.Command.Command import Command
from DIRAC.ResourceStatusSystem.Utilities.Exceptions import *
from DIRAC.ResourceStatusSystem.Utilities.Utils import *


#############################################################################

class GOCDBStatus_Command(Command):
  
  def doCommand(self, args, clientIn=None):
    """ Return getStatus from GOC DB Client
    
       :params:
         :attr:`args`: 
           - args[0]: string: should be a ValidRes
      
           - args[1]: string: should be the name of the ValidRes

           - args[2]: string: optional, number of hours in which 
           the down time is starting
    """

    if not isinstance(args, tuple):
      raise TypeError, where(self, self.doCommand)
    
    if args[0] not in ValidRes:
      raise InvalidRes, where(self, self.doCommand)
    
    if clientIn is not None:
      c = clientIn
    else:
      # use standard GOC DB Client
      from DIRAC.ResourceStatusSystem.Client.GOCDBClient import GOCDBClient   
      c = GOCDBClient()
    
    granularity = args[0]
    name = args[1]  
    try:  
      hours = args[2]
    except IndexError:
      hours = None

    if granularity in ('Site', 'Sites'):
      name = getSiteRealName(name)

    try:
      res = c.getStatus(granularity, name, None, hours)
      
      if res is None or res == []:
        return {'DT':None}
      
      if isinstance(res, list):
        #there's more than one DT
        for dt in res:
          if dt['Type'] == 'OnGoing':
            resDT = dt
            break
        try:
          resDT
        except:
          #if I'm here, there's no OnGoing DT
          resDT = res[0]

      else:
        resDT = res

      if resDT['Type'] == 'Programmed':
        resDT['DT'] = resDT['DT'] + " in " + str(resDT['InHours']) + ' hours'
      if 'Type' in resDT.keys():
        del resDT['Type']
      if 'InHours' in resDT.keys():
        del resDT['InHours']
      if 'URL' in resDT.keys():
        del resDT['URL']
      if 'id' in resDT.keys():
        del resDT['id']
      if 'StartDate' in resDT.keys():
        del resDT['StartDate']

      return resDT
        
    except urllib2.URLError:
      gLogger.error("GOCDB timed out for " + granularity + " " + name )
      return  {'DT':'Unknown'}      
    except:
      gLogger.exception("Exception when calling GOCDBClient")
      return {'DT':'Unknown'}

    
#############################################################################

class GOCDBInfo_Command(Command):
  
  def doCommand(self, args, clientIn=None):
    """ Return all info from GOC DB Client
    
       :params:
         :attr:`args`: 
           - args[0]: string: should be a ValidRes
      
           - args[1]: string: should be the name of the ValidRes
    """

    if not isinstance(args, tuple):
      raise TypeError, where(self, self.doCommand)
    
    if args[0] not in ValidRes:
      raise InvalidRes, where(self, self.doCommand)
    
    if clientIn is not None:
      c = clientIn
    else:
      # use standard GOC DB Client
      from DIRAC.ResourceStatusSystem.Client.GOCDBClient import GOCDBClient   
      c = GOCDBClient()
      
    granularity = args[0]
    name = args[1]  
    try:  
      hours = args[2]
    except IndexError:
      hours = None

    if granularity in ('Site', 'Sites'):
      name = getSiteRealName(name)

    try:

      res = c.getStatus(granularity, name, None, hours)

      if res is None or res == []:
        return {'DT':'None'}

      if isinstance(res, list):
        #there's more than one DT
        for dt in res:
          if dt['Type'] == 'OnGoing':
            resDT = dt
            break
        try:
          resDT
        except:
          #if I'm here, there's no OnGoing DT
          resDT = res[0]

      else:
        resDT = res

      if resDT['Type'] == 'Programmed':
        resDT['DT'] = resDT['DT'] + " in " + str(resDT['InHours']) + ' hours'

      return resDT
        
    except urllib2.URLError:
      gLogger.error("GOCDB timed out for " + granularity + " " + name )
      return {'DT':'Unknown'}      
    except:
      gLogger.exception("Exception when calling GOCDBClient")
      return {'DT':'Unknown'}

    try:
      return res['URL']
    except:
      return None

#############################################################################
    