""" The SAMResults_Command class is a command class to know about 
    present SAM status
"""

import urllib2, httplib

from DIRAC import gLogger

from DIRAC.ResourceStatusSystem.Client.Command.Command import Command
from DIRAC.ResourceStatusSystem.Utilities.Exceptions import *
from DIRAC.ResourceStatusSystem.Utilities.Utils import *

from DIRAC.ResourceStatusSystem.Client.SAMResultsClient import NoSAMTests

class SAMResults_Command(Command):
  
  def doCommand(self, args, clientIn=None, rsClientIn=None):
    """ Return getStatus from SAM Results Client  
    
       :params:
         :attr:`args`: 
           - args[0]: string: should be a ValidRes
      
           - args[1]: string: should be the (DIRAC) name of the ValidRes
           
           - args[2]: string: optional - should be the (DIRAC) site name of the ValidRes
           
           - args[3]: list: list of tests
    """

    if not isinstance(args, tuple):
      raise TypeError, where(self, self.doCommand)
    
    if clientIn is not None:
      c = clientIn
    else:
      # use standard GOC DB Client
      from DIRAC.ResourceStatusSystem.Client.SAMResultsClient import SAMResultsClient
      c = SAMResultsClient()

    if rsClientIn is not None:
      rsc = rsClientIn
    else:
      # use standard RS Client
      from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient import ResourceStatusClient
      rsc = ResourceStatusClient()

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
        try:
          siteName = rsc.getGeneralName(granularity, name, 'Site')
        except:
          gLogger.error("Can't get a general name for %s %s" %(granularity, name))
          return {'SAM-Status':'Unknown'}      
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
      except NoSAMTests:
        gLogger.error("There are no SAM tests for " + granularity + " " + name )
        return  {'SAM-Status':None}
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