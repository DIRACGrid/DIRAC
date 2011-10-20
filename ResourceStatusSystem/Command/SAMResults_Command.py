################################################################################
# $HeadURL $
################################################################################
__RCSID__ = "$Id:  $"

""" 
  The SAMResults_Command class is a command class to know about 
  present SAM status
"""

import urllib2, httplib

from DIRAC                                           import gLogger
from DIRAC.Core.Utilities.SitesDIRACGOCDBmapping     import getGOCSiteName

from DIRAC.ResourceStatusSystem.Command.Command      import *
from DIRAC.ResourceStatusSystem.Command.knownAPIs    import initAPIs
from DIRAC.ResourceStatusSystem.Utilities.Exceptions import InvalidRes
from DIRAC.ResourceStatusSystem.Utilities.Utils      import where 

################################################################################
################################################################################

class SAMResults_Command(Command):
  
  __APIs__ = [ 'SAMResultsClient', 'ResourceStatusClient' ]
  
  def doCommand(self, rsClientIn=None):
    """ 
    Return getStatus from SAM Results Client  
    
    :attr:`args`: 
     - args[0]: string: should be a ValidRes

     - args[1]: string: should be the (DIRAC) name of the ValidRes
     
     - args[2]: string: optional - should be the (DIRAC) site name of the ValidRes
     
     - args[3]: list: list of tests
    """
    super(SAMResults_Command, self).doCommand()
    self.APIs = initAPIs( self.__APIs__, self.APIs )

    granularity = self.args[0]
    name = self.args[1]
    try:  
      siteName = self.args[2]
    except IndexError:
      siteName = None

    if granularity in ('Site', 'Sites'):
      siteName = getGOCSiteName(name)
      if not siteName['OK']:
        raise RSSException, siteName['Message']
      siteName = siteName['Value']
    elif granularity in ('Resource', 'Resources'):
      if siteName is None:
        siteName = self.APIs[ 'ResourceStatusClient' ].getGridSiteName(granularity, name)
        if not siteName['OK']:
          raise RSSException, siteName['Message']    
        else:
          siteName = siteName[ 'Value' ]
      else:
        siteName = getGOCSiteName(siteName)
        if not siteName['OK']:
          raise RSSException, siteName['Message']
        siteName = siteName['Value']
    else:
      raise InvalidRes, where(self, self.doCommand)
    
    try:  
      tests = self.args[3]
    except IndexError:
      tests = None
    finally:
      try:
        
        res = self.APIs[ 'SAMResultsClient' ].getStatus(granularity, name, siteName, tests, 
                                    timeout = self.timeout)
        if not res['OK']:
          gLogger.error("There are no SAM tests for " + granularity + " " + name )
          return  {'Result':None}
      except urllib2.URLError:
        gLogger.error("SAM timed out for " + granularity + " " + name )
        return  {'Result':'Unknown'}      
      except httplib.BadStatusLine:
        gLogger.error("httplib.BadStatusLine: could not read" + granularity + " " + name )
        return  {'Result':'Unknown'}
      except:
        gLogger.exception("Exception when calling SAMResultsClient for %s %s" %(granularity, name))
        return  {'Result':'Unknown'}

    return {'Result':res['Value']}
  
  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__
  
################################################################################
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #  
################################################################################

'''
  HOW DOES THIS WORK.
    
    will come soon...
'''

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF  