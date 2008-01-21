########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Interfaces/API/DiracAdmin.py,v 1.1 2008/01/21 16:47:36 paterson Exp $
# File :   DiracAdmin.py
# Author : Stuart Paterson
########################################################################

from DIRAC.Core.Base import Script
Script.parseCommandLine()

"""DIRAC Administrator API Class

All administrative functionality is exposed through the DIRAC Admin API.  Examples include
site banning and unbanning, WMS proxy uploading etc.

"""

__RCSID__ = "$Id: DiracAdmin.py,v 1.1 2008/01/21 16:47:36 paterson Exp $"

import DIRAC
from DIRAC.Core.DISET.RPCClient                          import RPCClient
from DIRAC.Core.Utilities.GridCredentials                import getGridProxy,getCurrentDN
from DIRAC                                               import gConfig, gLogger, S_OK, S_ERROR

import re, os, sys, string, time, shutil, types

COMPONENT_NAME='/Interfaces/API/DiracAdmin'

class DiracAdmin:

  #############################################################################
  def __init__(self):
    """Internal initialization of the DIRAC Admin API.
    """
    self.log = gLogger.getSubLogger('DIRACAdminAPI')
    self.site       = gConfig.getValue('/LocalSite/Site','Unknown')
    self.setup      = gConfig.getValue('/DIRAC/Setup','Unknown')
    self.section    = COMPONENT_NAME
    self.cvsVersion = 'CVS version '+__RCSID__
    self.diracInfo  = 'DIRAC version v%dr%d build %d' \
                       %(DIRAC.majorVersion,DIRAC.minorVersion,DIRAC.patchLevel)

    self.dbg = False
    if gConfig.getValue(self.section+'/LogLevel','DEBUG') == 'DEBUG':
      self.dbg = True

    self.scratchDir = gConfig.getValue(self.section+'/ScratchDir','/tmp')
    self.wmsAdmin = RPCClient('WorkloadManagement/WMSAdministrator')
    self.currentDir = os.getcwd()

  #############################################################################
  def uploadProxy(self,group,permanent=True):
    """Upload a proxy to the DIRAC WMS.  This method

       Example usage:

       >>> print diracAdmin.uploadProxy('lhcb_pilot')
       {'OK': True, 'Value': 0L}

       @param group: DIRAC Group
       @type job: string
       @return: S_OK,S_ERROR

       @param permanent: Indefinitely update proxy
       @type permanent: boolean

    """
    proxy  = getGridProxy()
    proxy = open(proxy,'r').read()
    activeDN = getCurrentDN()
    dn = activeDN['Value']
    result = self.wmsAdmin.uploadProxy(proxy,dn,group)
    if not result['OK']:
      self.log.warn('Uploading proxy failed')
      self.log.warn(result)
      return result

    result = self.wmsAdmin.setProxyPersistencyFlag(permanent,dn,group)
    if not result['OK']:
      self.log.warn('Setting proxy update flag failed')
      self.log.warn(result)
    return result

  #############################################################################
  def getSiteMask(self):
    """Retrieve current site mask from WMS Administrator service.

       Example usage:

       >>> print diracAdmin.getSiteMask()
       {'OK': True, 'Value': 0L}

       @return: S_OK,S_ERROR

    """
    result = self.wmsAdmin.getSiteMask()
    return result

  #############################################################################
  def getCSDict(self,sectionPath):
    """Retrieve a dictionary from the CS for the specified path.

       Example usage:

       >>> print diracAdmin.getCSPathDict('Resources/Computing/OSCompatibility')
       {'OK': True, 'Value': {'slc4_amd64_gcc34': 'slc4_ia32_gcc34,slc4_amd64_gcc34', 'slc4_ia32_gcc34': 'slc4_ia32_gcc34'}}

       @return: S_OK,S_ERROR

    """
    result = gConfig.getOptionsDict(sectionPath)
    return result

  #############################################################################
  def addSiteInMask(self,site):
    """Adds the site to the site mask.

       Example usage:

       >>> print diracAdmin.addSiteInMask()
       {'OK': True, 'Value': }

       @return: S_OK,S_ERROR

    """
    self.log.info('Allowing %s in site mask' % site)
    result = self.wmsAdmin.allowSite(site)
    return result

  #############################################################################
  def banSiteFromMask(self,site):
    """Removes the site from the site mask.

       Example usage:

       >>> print diracAdmin.banSiteFromMask()
       {'OK': True, 'Value': }

       @return: S_OK,S_ERROR

    """
    self.log.info('Removing %s from site mask' % site)
    result = self.wmsAdmin.banSite(site)
    return result

  #############################################################################
  def clearMask(self):
    """Removes all sites from the site mask.  Should be used with care.

       Example usage:

       >>> print diracAdmin.clearMask()
       {'OK': True, 'Value':''}

       @return: S_OK,S_ERROR

    """
    result = self.wmsAdmin.clearMask()
    return result

  #############################################################################
  def getProxy(self,ownerDN,ownerGroup,directory='',validity=12):
    """Retrieves a proxy with default 12hr validity from the WMS and stores
       this in a file in the local directory by default.  For scripting in python
       with this function, the X509_USER_PROXY environment variable is also set up.

       Example usage:

       >>> print diracAdmin.getProxy()
       {'OK': True, 'Value': }

       @return: S_OK,S_ERROR

    """
    if not directory:
      directory = self.currentDir

    if not os.path.exists(directory):
      self.__report('Directory %s does not exist' % directory)

    result = self.wmsAdmin.getProxy(ownerDN,ownerGroup,validity)
    if not result['OK']:
      self.log.warn('Problem retrieving proxy from WMS')
      self.log.warn(result['Message'])
      return result

    proxy = result['Value']
    if not proxy:
      self.log.warn('Null proxy returned from WMS Administrator')
      return result

    name = string.split(ownerDN,'=')[-1].replace(' ','').replace('/','')
    if not name:
      name = 'tempProxy'

    proxyPath = '%s/proxy%s' %(directory,name)
    if os.path.exists(proxyPath):
      os.remove(proxyPath)

    fopen = open(proxyPath,'w')
    fopen.write(proxy)
    fopen.close()

    os.putenv('X509_USER_PROXY',proxyPath)
    self.log.info('Proxy written to %s' %(proxyPath))
    self.log.info('Setting X509_USER_PROXY=%s' %(proxyPath))
    result = S_OK(proxyPath)
    return result

  #############################################################################
  def __errorReport(self,error,message=None):
    """Internal function to return errors and exit with an S_ERROR()
    """
    if not message:
      message = error

    self.log.warn(error)
    return S_ERROR(message)

  #EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#