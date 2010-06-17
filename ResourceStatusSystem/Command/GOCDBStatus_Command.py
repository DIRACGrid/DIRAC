""" The GOCDBStatus_Command class is a command class to know about 
    present downtimes
"""

import urllib2

from DIRAC import gLogger
from DIRAC.Core.Utilities.SitesDIRACGOCDBmapping import getGOCSiteName

from DIRAC.ResourceStatusSystem.Command.Command import Command

#############################################################################

class GOCDBStatus_Command(Command):
  
  def doCommand(self):
    """ 
    Return getStatus from GOC DB Client
    
    :attr:`args`: 
     - args[0]: string: should be a ValidRes

     - args[1]: string: should be the name of the ValidRes

     - args[2]: string: optional, number of hours in which 
     the down time is starting
    """
    super(GOCDBStatus_Command, self).doCommand()

    if self.client is None:
      # use standard GOC DB Client
      from DIRAC.Core.LCG.GOCDBClient import GOCDBClient   
      self.client = GOCDBClient()
    
    granularity = self.args[0]
    name = self.args[1]  
    try:  
      hours = self.args[2]
    except IndexError:
      hours = None

    if granularity in ('Site', 'Sites'):
      name = getGOCSiteName(name)['Value']

    try:
      res = self.client.getStatus(granularity, name, None, hours, self.timeout)
      if not res['OK']:
        return {'Result':'Unknown'}
      res = res['Value']
      if res is None or res == []:
        return {'Result':{'DT':None}}
      
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
      else:
        resDT['DT'] = resDT['DT']
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
      
      return {'Result':resDT}
        
    except urllib2.URLError:
      gLogger.error("GOCDB timed out for " + granularity + " " + name )
      return  {'Result':'Unknown'}      
    except:
      gLogger.exception("Exception when calling GOCDBClient for " + granularity + " " + name )
      return {'Result':'Unknown'}

  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__
    
#############################################################################

class GOCDBInfo_Command(Command):
  
  def doCommand(self):
    """ Return all info from GOC DB Client
    
         :attr:`args`: 
           - args[0]: string: should be a ValidRes
      
           - args[1]: string: should be the name of the ValidRes
    """

    if self.client is None:
      # use standard GOC DB Client
      from DIRAC.Core.LCG.GOCDBClient import GOCDBClient   
      self.client = GOCDBClient()
      
    granularity = self.args[0]
    name = self.args[1]  
    try:  
      hours = self.args[2]
    except IndexError:
      hours = None

    if granularity in ('Site', 'Sites'):
      name = getGOCSiteName(name)['Value']

    try:

      res = self.client.getStatus(granularity, name, None, hours, self.timeout)
      if not res['OK']:
        return {'Result':'Unknown'}
      res = res['Value']
      
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

      return {'Result':resDT}
        
    except urllib2.URLError:
      gLogger.error("GOCDB timed out for " + granularity + " " + name )
      return {'Result':'Unknown'}    
    except:
      gLogger.exception("Exception when calling GOCDBClient for " + granularity + " " + name )
      return {'Result':'Unknown'}

  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__
    
#############################################################################
    