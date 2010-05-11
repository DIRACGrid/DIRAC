""" The SLS_Command class is a command class to properly interrogate the SLS
"""

import urllib2

from DIRAC import gLogger

from DIRAC.ResourceStatusSystem.Client.Command.Command import Command
from DIRAC.ResourceStatusSystem.Utilities.Exceptions import *
from DIRAC.ResourceStatusSystem.Client.SLSClient import NoServiceException

#############################################################################

def _getSESLSName(name):

  splitted = name.split('_', 1)

  if len(splitted) == 1:
    toSplit = splitted[0]
    shortSiteName = toSplit.split('-')[0]
    tokenName = toSplit.split('-')[1]
  else:
    shortSiteName = splitted[0]
    tokenName = splitted[1]
 
  if tokenName == 'MC-DST':
    tokenName = 'MC_DST'
 
  if shortSiteName == 'NIKHEF':
    shortSiteName = 'SARA'
  
  SLSName = shortSiteName + '-' + 'LHCb_' + tokenName

  return SLSName
      
#############################################################################

def _getCastorSESLSName(name):

  splitted = name.split('_', 1)

  if len(splitted) == 1:
    toSplit = splitted[0]
    tokenName = toSplit.split('-')[1]
  else:
    tokenName = splitted[1].replace('-','').replace('_','')
  
  SLSName = 'CASTORLHCB_LHCB' + tokenName
  
  return SLSName
      
#############################################################################

def _getServiceSLSName(name):
  #TBD
  
  return name

#############################################################################

class SLSStatus_Command(Command):
  
  def doCommand(self):
    """ 
    Return getStatus from SLS Client
    
    :attr:`args`: 
     - args[0]: string: should be a ValidRes

     - args[1]: string: should be the (DIRAC) name of the ValidRes
    """

    if self.client is None:
      from DIRAC.ResourceStatusSystem.Client.SLSClient import SLSClient   
      self.client = SLSClient()
      
    if self.args[0] == 'StorageElement':
      #know the SLS name of the SE
      SLSName = _getSESLSName(self.args[1])
    elif self.args[0] == 'Service':
      #know the SLS name of the VO BOX - TBD
      SLSName = _getServiceSLSName(self.args[1])
    else:
      raise InvalidRes, where(self, self.doCommand)
    
    try:
      res = self.client.getAvailabilityStatus(SLSName, timeout = self.timeout)
      return {'Result':res}
    except NoServiceException:
      gLogger.error("No SLS sensors for " + self.args[0] + " " + self.args[1] )
      return  {'Result':None}
    except urllib2.URLError:
      gLogger.error("SLS timed out for " + self.args[0] + " " + self.args[1] )
      return  {'Result':'Unknown'}
    except:
      gLogger.exception("Exception when calling SLSClient")
      return {'Result':'Unknown'}

  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__
    
#############################################################################

class SLSServiceInfo_Command(Command):
  
  def doCommand(self):
    """ 
    Return getServiceInfo from SLS Client
    
    :attr:`args`: 
     - args[0]: string: should be a ValidRes

     - args[1]: string: should be the (DIRAC) name of the ValidRes

     - args[2]: list: list of info requested
    """

    if self.client is None:
      from DIRAC.ResourceStatusSystem.Client.SLSClient import SLSClient   
      self.client = SLSClient()
      
    if self.args[0] == 'StorageElement':
      #know the SLS name of the SE
      SLSName = _getCastorSESLSName(self.args[1])
    elif self.args[0] == 'Service':
      #know the SLS name of the VO BOX - TBD
      SLSName = _getServiceSLSName(self.args[1])
    else:
      raise InvalidRes, where(self, self.doCommand)
    
    try:
      res = self.client.getServiceInfo(SLSName, self.args[2], timeout = self.timeout)
      return {'Result':res}
    except NoServiceException:
      gLogger.error("No (not all) SLS sensors for " + self.args[0] + " " + self.args[1])
      return  {'Result':None}
    except urllib2.URLError:
      gLogger.error("SLS timed out for " + self.args[0] + " " + self.args[1] )
      return  {'Result':'Unknown'}
    except:
      gLogger.exception("Exception when calling SLSClient")
      return {'Result':'Unknown'}

  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__
    
#############################################################################

class SLSLink_Command(Command):
  
  def doCommand(self):
    """ 
    Return getLink from SLS Client
    
    :attr:`args`: 
      - args[0]: string: should be a ValidRes

      - args[1]: string: should be the (DIRAC) name of the ValidRes
    """

    if self.client is None:
      from DIRAC.ResourceStatusSystem.Client.SLSClient import SLSClient   
      self.client = SLSClient()
      
    if self.args[0] == 'StorageElement':
      #know the SLS name of the SE
      SLSName = _getSESLSName(self.args[1])
    elif self.args[0] == 'Service':
      #know the SLS name of the VO BOX - TBD
      SLSName = _getServiceSLSName(self.args[1])
    else:
      raise InvalidRes, where(self, self.doCommand)
    
    try:
      res = self.client.getLink(SLSName, timeout = self.timeout)
      return {'Result':res}
    except urllib2.URLError:
      gLogger.error("SLS timed out for " + self.args[0] + " " + self.args[1] )
      return  {'Result':'Unknown'}
    except:
      gLogger.exception("Exception when calling SLSClient")
      return {'Result':'Unknown'}

  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__
  
#############################################################################

