""" The SAMResults_Command class is a command class to know about 
    present SAM status
"""

import urllib2

from DIRAC import gLogger

from DIRAC.ResourceStatusSystem.Client.Command.Command import Command
from DIRAC.ResourceStatusSystem.Utilities.Exceptions import *
from DIRAC.ResourceStatusSystem.Utilities.Utils import *

class SAMResults_Command(Command):
  
  def doCommand(self, args, clientIn=None):
    """ Return getStatus from SAM Results Client  
    """

    if not isinstance(args, tuple):
      raise TypeError, where(self, self.doCommand)
    
    if clientIn is not None:
      c = clientIn
    else:
      # use standard GOC DB Client
      from DIRAC.ResourceStatusSystem.Client.SAMResultsClient import SAMResultsClient   
      c = SAMResultsClient()

    granularity = args[0]
    name = args[1]

    if granularity in ('Site', 'Sites'):
      siteName = getSiteRealName(name)
    elif granularity in ('Resource', 'Resources'):
      try:
        siteName = args[2]
        siteName = getSiteRealName(siteName)
      except IndexError:
        from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient import ResourceStatusClient
        rsc = ResourceStatusClient()
        siteName = rsc.getGeneralName(granularity, name, 'Site')
        if siteName is None or siteName == []:
          gLogger.info('%s is not a resource in DIRAC' %name)
          return {'SAM-Status':None}
        siteName = getSiteRealName(siteName)
    else:
      raise InvalidRes, where(self, self.doCommand)
    
    
    try:  
#      if len(args) == 2:
#        res = c.getStatus(granularity, args[1])
#      elif len(args) == 3:
#        res = c.getStatus(granularity, args[1], siteName)
#      elif len(args) == 4:
#        res = c.getStatus(granularity, args[1], siteName, args[3])
      tests = args[3]
    except IndexError:
      tests = None
    finally:
      try:
        res = c.getStatus(granularity, name, siteName, tests)
      except urllib2.URLError:
        gLogger.error("SAM timed out")
        return  {'SAM-Status':None}      
      except:
        gLogger.exception("Exception in SAMResultsClient")
        return  {'SAM-Status':None}

    return {'SAM-Status':res}