################################################################################
# $HeadURL $
################################################################################
__RCSID__ = "$Id:  $"

""" 
  The SLS_Command class is a command class to properly interrogate the SLS
"""

import urllib2
import xml.parsers.expat

from DIRAC                                           import gLogger

from DIRAC.ResourceStatusSystem.Command.Command      import *
from DIRAC.ResourceStatusSystem.Command.knownAPIs    import initAPIs
from DIRAC.ResourceStatusSystem.Utilities.Exceptions import InvalidRes
from DIRAC.ResourceStatusSystem.Utilities.Utils      import where

################################################################################
################################################################################

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
      
################################################################################

def _getCastorSESLSName(name):

  splitted = name.split('_', 1)

  if len(splitted) == 1:
    toSplit = splitted[0]
    tokenName = toSplit.split('-')[1]
  else:
    tokenName = splitted[1].replace('-','').replace('_','')
  
  SLSName = 'CASTORLHCB_LHCB' + tokenName
  
  return SLSName
      
################################################################################

def _getServiceSLSName(input, type):

  if type == 'VO-BOX':
    site = input.split('.')[1]
    
    if site == 'GRIDKA':
      site = 'GridKa'
    if site == 'NIKHEF':
      site = 'Nikhef'
  
    name = site + "_VOBOX"
  
  elif type == 'VOMS':
    name = 'VOMS'
  
  return name

################################################################################
################################################################################

class SLSStatus_Command(Command):
  
  __APIs__ = [ 'SLSClient' ]
  
  def doCommand(self):
    """ 
    Return getStatus from SLS Client
    
    :attr:`args`: 
     - args[0]: string: should be a ValidRes

     - args[1]: string: should be the (DIRAC) name of the ValidRes

     - args[2]: string: should be the ValidRes type (e.g. 'VO-BOX')
    """
    
    super(SLSStatus_Command, self).doCommand()
    self.APIs = initAPIs( self.__APIs__, self.APIs )

#    if self.client is None:
#      from DIRAC.Core.LCG.SLSClient import SLSClient   
#      self.client = SLSClient()
      
    if self.args[0] == 'StorageElement':
      #know the SLS name of the SE
      SLSName = _getSESLSName(self.args[1])
    elif self.args[0] == 'Service':
      #know the SLS name of the VO BOX
      SLSName = _getServiceSLSName(self.args[1], self.args[2])
    else:
      raise InvalidRes, where(self, self.doCommand)
    
    try:
      res = self.APIs[ 'SLSClient' ].getAvailabilityStatus(SLSName, timeout = self.timeout)
      if not res['OK']:
        gLogger.error("No SLS sensors for " + self.args[0] + " " + self.args[1] )
        return  {'Result':None}
      return {'Result':res['Value']}
    except urllib2.URLError:
      gLogger.error("SLS timed out for " + self.args[0] + " " + self.args[1] )
      return  {'Result':'Unknown'}
    except: 
      gLogger.exception("Exception when calling SLSClient for %s"%SLSName)
      return {'Result':'Unknown'}

  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__
    
################################################################################
################################################################################

class SLSServiceInfo_Command(Command):
  
  __APIs__ = [ 'SLSClient' ]
  
  def doCommand(self):
    """ 
    Return getServiceInfo from SLS Client
    
    :attr:`args`: 
     - args[0]: string: should be a ValidRes

     - args[1]: string: should be the (DIRAC) name of the ValidRes

     - args[2]: list: list of info requested
    """
    
    super(SLSServiceInfo_Command, self).doCommand()
    self.APIs = initAPIs( self.__APIs__, self.APIs )
    
#    if self.client is None:
#      from DIRAC.Core.LCG.SLSClient import SLSClient   
#      self.client = SLSClient()
      
    if self.args[0] == 'StorageElement':
      #know the SLS name of the SE
      SLSName = _getCastorSESLSName(self.args[1])
    elif self.args[0] == 'Service':
      #know the SLS name of the VO BOX
      SLSName = _getServiceSLSName(self.args[1], self.args[2])
    else:
      raise InvalidRes, where(self, self.doCommand)
    
    try:
    
      #gLogger.info(SLSName,self.args[2])   
      res = self.APIs[ 'SLSClient' ].getServiceInfo(SLSName, self.args[2], timeout = self.timeout)
      if not res[ 'OK' ]:
        gLogger.error("No SLS sensors for " + self.args[0] + " " + self.args[1] )
        res = None      
      else:
        res = res[ 'Value' ]
      return { 'Result' : res }
    
    except urllib2.HTTPError:
      gLogger.error( "No (not all) SLS sensors for " + self.args[0] + " " + self.args[1])
      return  {'Result':None}
    except urllib2.URLError:
      gLogger.error( "SLS timed out for " + self.args[0] + " " + self.args[1] )
      return  {'Result':'Unknown'}
    except xml.parsers.expat.ExpatError:
      gLogger.error( "Error parsing xml for " + self.args[0] + " " + self.args[1])
      return { 'Result' : 'Unknown' }
    except:
      gLogger.exception("Exception when calling SLSClient for " + self.args[0] + " " + self.args[1])
      return { 'Result' : 'Unknown' }


  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__
    
################################################################################
################################################################################

class SLSLink_Command(Command):
  
  __APIs__ = [ 'SLSClient' ]
  
  def doCommand(self):
    """ 
    Return getLink from SLS Client
    
    :attr:`args`: 
      - args[0]: string: should be a ValidRes

      - args[1]: string: should be the (DIRAC) name of the ValidRes
    """
    
    super(SLSLink_Command, self).doCommand()
    self.APIs = initAPIs( self.__APIs__, self.APIs )

#    if self.client is None:
#      from DIRAC.Core.LCG.SLSClient import SLSClient   
#      self.client = SLSClient()
      
    if self.args[0] == 'StorageElement':
      #know the SLS name of the SE
      SLSName = _getSESLSName(self.args[1])
    elif self.args[0] == 'Service':
      #know the SLS name of the VO BOX
      SLSName = _getServiceSLSName(self.args[1], self.args[2])
    else:
      raise InvalidRes, where(self, self.doCommand)
    
    try:
      res = self.APIs[ 'SLSClient' ].getLink(SLSName)
      if not res['OK']:
        gLogger.error("No SLS sensors for " + self.args[0] + " " + self.args[1] )
        return  {'Result':None}
      return {'Result':res['Value']}
    except urllib2.URLError:
      gLogger.error("SLS timed out for " + self.args[0] + " " + self.args[1] )
      return  {'Result':'Unknown'}
    except:
      gLogger.exception("Exception when calling SLSClient")
      return {'Result':'Unknown'}

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