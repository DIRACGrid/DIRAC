""" The SAMResults_Command class is a command class to know about 
    present SAM status
"""

import urllib2, httplib

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
    try:  
      siteName = args[2]
    except IndexError:
      siteName = None

    if granularity in ('Site', 'Sites'):
      siteName = getSiteRealName(name)
    elif granularity in ('Resource', 'Resources'):
      if siteName is None:
        from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient import ResourceStatusClient
        rsc = ResourceStatusClient()
        siteName = rsc.getGeneralName(granularity, name, 'Site')
        if siteName is None or siteName == []:
          gLogger.info('%s is not a resource in DIRAC' %name)
          return {'SAM-Status':None}
        siteName = getSiteRealName(siteName)
      else:
        siteName = getSiteRealName(siteName)
    else:
      raise InvalidRes, where(self, self.doCommand)
    
    try:  
      tests = args[3]
    except IndexError:
      tests = None
    finally:
      try:
        res = c.getStatus(granularity, name, siteName, tests)
      except urllib2.URLError:
        gLogger.error("SAM timed out for " + granularity + " " + name )
        return  {'SAM-Status':'Unknown'}      
      except httplib.BadStatusLine:
        gLogger.error("httplib.BadStatusLine: could not read" + granularity + " " + name )
        return  {'SAM-Status':'Unknown'}
      except:
        gLogger.exception("Exception when calling SAMResultsClient")
        return  {'SAM-Status':'Unknown'}

    return {'SAM-Status':res}