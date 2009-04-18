########################################################################
# $Id: SiteMappingHandler.py,v 1.11 2009/04/18 18:26:59 rgracian Exp $
########################################################################

""" The SiteMappingHandler...
"""

__RCSID__ = "$Id: SiteMappingHandler.py,v 1.11 2009/04/18 18:26:59 rgracian Exp $"

from types import *
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client import PathFinder
import os, re

baseFilePath = ''

def initializeSiteMappingHandler( serviceInfo ):

  gLogger.verbose('-----> Initializing handler...')
  csSection = PathFinder.getServiceSection('Monitoring/SiteMapping')
  gLogger.verbose('csSection: %s' % csSection)
  
  global baseFilePath
  
  cacheDir = gConfig.getValue(csSection+'/CacheDir','/opt/dirac/work')
  cacheDir = cacheDir.rstrip('/')
  baseFilePath = cacheDir + '/SiteMapping'
  if not os.path.exists(baseFilePath):
    os.mkdir(baseFilePath)
  gLogger.verbose('baseFilePath: %s' % baseFilePath)
      
  return S_OK()

class SiteMappingHandler( RequestHandler ):
  
  ###########################################################################  
  def transfer_toClient(self, fileName, token, fileHelper):
    """ Transfers the file data to the client
        
        Note to self:
        In the receiveFile call, the 1st argument is the name of the new local file to create;
          anything sent via stringToNetwork is stored there
        You should therefore use fileId, which is also the 2nd argument of receiveFile, as a means
          of identifying the remote file you want.
    """
    
    # Strip the path (security measure)
    baseName = self.getBaseFile(fileName)
    gLogger.verbose('Requested file: %s' % fileName)
    gLogger.verbose('Processed base name: %s' % baseName)
    
    # Now prefix our own base path
    fileToRead = '%s/%s' % (baseFilePath, baseName)
    gLogger.verbose('Reading file: %s' % fileToRead)
    
    try:
      fh = open(fileToRead, 'rb')
      dataToWrite = fh.read()
      fh.close()
    except Exception, x:
      return S_ERROR('Failed to read file %s: %s' % (fileToRead, x))
      
    result = fileHelper.stringToNetwork(dataToWrite)
    if not result['OK']:
      gLogger.verbose('Failed to read file %s' % fileToRead)
      return S_ERROR('stringToNework failed.')
    else:
      gLogger.verbose('File successfully read: %d bytes from %s' % (len(dataToWrite), fileToRead))
      return S_OK('Transfer complete.')
  
  ###########################################################################  
  def getBaseFile(self, filePath):
    """ Returns the file name without the path
    """
    fileParts = filePath.split('/')
    baseName = fileParts[len(fileParts) - 1]
    return baseName

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
