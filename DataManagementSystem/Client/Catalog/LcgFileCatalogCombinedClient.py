""" File catalog client for the LFC service combined with
    multiple read-only mirrors
"""
from DIRAC  import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.DataManagementSystem.Client.Catalog.LcgFileCatalogClient import LcgFileCatalogClient
from DIRAC.Core.Utilities.Subprocess import pythonCall
import random, time,os

class LcgFileCatalogCombinedClient:

  ro_methods = ['exists','isLink','readLink','isFile','getFileMetadata','getReplicas',
                'getReplicaStatus','getFileSize','isDirectory','getDirectoryReplicas',
                'listDirectory','getDirectoryMetadata','getDirectorySize']

  write_methods = ['createLink','removeLink','addFile','addReplica','removeReplica',
                   'removeFile','setReplicaStatus','setReplicaHost','createDirectory',
                   'removeDirectory']

  def __init__(self, infosys=None, master_host=None, mirrors = []):
    """ Default constructor
    """
    # Obtain the site configuration
    result = gConfig.getOption('/LocalSite/Site')
    if not result['OK']:
      gLogger.error('Failed to get the /LocalSite/Site')
      self.site = 'Unknown'
    else:
      self.site = result['Value']

    if not infosys:
      configPath = '/Resources/FileCatalogs/LFC/LcgGfalInfosys'
      infosys = gConfig.getValue(configPath)

    if not master_host:
      configPath = '/Resources/FileCatalogs/LFC/LFCMaster'
      master_host = gConfig.getValue(configPath)
    # Create the master LFC client first
    self.lfc = LcgFileCatalogClient(infosys,master_host)
    
    self.valid = False
    if self.lfc.isOK():
      self.valid = True 

    if not mirrors:
      configPath = '/Resources/FileCatalogs/LFC/LFCReadOnlyMirrors'
      mirrors = gConfig.getValue(configPath,[])
    # Create the mirror LFC instances
    self.mirrors = []
    for mirror in mirrors:
      lfc = LcgFileCatalogClient(infosys,mirror)
      self.mirrors.append(lfc)
    random.shuffle(self.mirrors)
    self.nmirrors = len(self.mirrors)

    # Keep the environment for the master instance
    self.master_host = self.lfc.host
    os.environ['LFC_HOST'] = self.master_host
    os.environ['LCG_GFAL_INFOSYS'] = infosys
    self.name = 'LFC'
    self.timeout = 3000

  def isOK(self):
    return self.valid

  def getName(self,DN=''):
    """ Get the file catalog type name
    """
    return self.name

  def __getattr__(self, name):
    self.call = name
    if name in LcgFileCatalogCombinedClient.write_methods:
      return self.w_execute
    elif name in LcgFileCatalogCombinedClient.ro_methods:
      return self.r_execute
    else:
      raise AttributeError

  def w_execute(self, *parms, **kws):
    """ Write method executor.
        Dispatches execution of the methods which need Read/Write
        access to the master LFC instance
    """

    # If the DN argument is given, this is an operation on behalf
    # of the user with this DN, prepare setAuthorizationId call
    userDN = ''
    if kws.has_key('DN'):
      userDN = kws['DN']
      del kws['DN']

    # Try the method 3 times just in case of intermittent errors
    max_retry = 2
    count = 0
    result = S_ERROR()

    while (not result['OK']) and (count <= max_retry):
      if count > 0:
        # If retrying, wait a bit
        time.sleep(1)
      try:
        result = S_OK()
        if userDN:
          resAuth = pythonCall(self.timeout,self.lfc.setAuthorizationId,userDN)
          if not resAuth['OK']:
            result = S_ERROR('Failed to set user authorization')
        if result['OK']:
          method = getattr(self.lfc,self.call)
          resMeth = pythonCall(self.timeout,method,*parms,**kws)
          if not resMeth['OK']:
            result = S_ERROR('Timeout calling '+self.call+" method")
          else:
            result = resMeth['Value']
      except Exception,x:
        result = S_ERROR('Exception while calling LFC Master service '+str(x))
      count += 1

    return result

  def r_execute(self, *parms, **kws):
    """ Read-only method executor.
        Dispatches execution of the methods which need Read-only
        access to the mirror LFC instances
    """

    # If the DN argument is given, this is an operation on behalf
    # of the user with this DN, prepare setAuthorizationId call
    userDN = ''
    if kws.has_key('DN'):
      userDN = kws['DN']
      del kws['DN']

    result = S_ERROR()
    # Try the method 3 times just in case of intermittent errors
    max_retry = 2
    count = 0

    while (not result['OK']) and (count <= max_retry):
      i = 0
      while not result['OK'] and i < self.nmirrors:
        # Switch environment to the mirror instance
        os.environ['LFC_HOST'] = self.mirrors[i].host
        try:
          result = S_OK()
          if userDN:
            resAuth = pythonCall(self.timeout,self.mirrors[i].setAuthorizationId,userDN)
            if not resAuth['OK']:
              result = S_ERROR('Failed to set user authorization')
          if result['OK']:
            method = getattr(self.mirrors[i],self.call)
            resMeth = pythonCall(self.timeout,method,*parms,**kws)
            if not resMeth['OK']:
              result = S_ERROR('Timout calling '+self.call+" method")
            else:
              result = resMeth['Value']
        except Exception,x:
          result = S_ERROR('Exception while calling LFC mirror service '+str(x))
        i += 1
      count += 1

    # Return environment to the master LFC instance
    os.environ['LFC_HOST'] = self.master_host

    # Call the master LFC if all the mirrors failed
    if not result['OK']:
      try:
        result = S_OK()
        if userDN:
          resAuth = pythonCall(self.timeout,self.lfc.setAuthorizationId,userDN)
          if not resAuth['OK']:
            result = S_ERROR('Failed to set user authorization')
        if result['OK']:
          method = getattr(self.lfc,self.call)
          resMeth = pythonCall(self.timeout,method,*parms,**kws)
          if not resMeth['OK']:
            result = S_ERROR('Timout calling '+self.call+" method")
          else:
            result = resMeth['Value']
      except Exception,x:
        result = S_ERROR('Exception while calling LFC Master service '+str(x))

    return result
