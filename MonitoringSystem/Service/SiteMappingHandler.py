########################################################################
# $Id: SiteMappingHandler.py,v 1.2 2008/07/01 10:05:13 asypniew Exp $
########################################################################

""" The SiteMappingHandler...
"""

__RCSID__ = "$Id: SiteMappingHandler.py,v 1.2 2008/07/01 10:05:13 asypniew Exp $"

from types import *
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.Core.Utilities.MappingFileCache import MappingFileCache
from DIRAC import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client import PathFinder
from DIRAC.Core.Utilities.Mapping import Mapping
import os

siteData = Mapping()
fileCache = False

sectionFiles = {}#'SiteMask' : 'sitemask.kml', 'JobSummary' : 'jobsummary.kml', 'PilotSummary' : 'pilotsummary.kml'}
sectionTag = {}#'SiteMask' : 'SM', 'JobSummary' : 'JS', 'PilotSummary' : 'PS'}
sectionDict = {}#'kml' : sectionFiles, 'png' : sectionTag}

# Be prepared for ALL files to possible be deleted from this directory
baseFilePath = ''

def initializeSiteMappingHandler( serviceInfo ):

  csSection = PathFinder.getServiceSection( 'Monitoring/SiteMapping' )
  global siteData
  global fileCache
  global sectionFiles, sectionTag, sectionDict
  global baseFilePath
  sectionFiles = gConfig.getOptionsDict(csSection+'/PlotViews')['Value']
  sectionTag = gConfig.getOptionsDict(csSection+'/PlotTags')['Value']
  sectionDict = {'kml' : sectionFiles, 'png' : sectionTag}
  cacheDir = gConfig.getValue(csSection+'/CacheDir','')
  baseFilePath = cacheDir + '/SiteMapping'
  if not os.path.exists(baseFilePath):
    os.mkdir(baseFilePath)
  cacheTimeToLive = gConfig.getValue(csSection+'/CacheTime', 60)
  fileCache = MappingFileCache(int(cacheTimeToLive))

  return S_OK()

class SiteMappingHandler( RequestHandler ):
  
  ###########################################################################
  def purgeAll(self):
    """ Manually purge all of the expired files
    """
    fileCache.purge()
    gLogger.verbose('File cache manually purged.')
    return S_OK('File cache manually purged.')
  
  ########################################################################### 
  def updateData(self, section): 
    """ Update site data
    """ 
    
    # First, update the list of sites.
    # If you ONLY want to update this, set section=False
    result = siteData.updateData('SiteData')
    if not result['OK']:
      return result
      
    if section:
      gLogger.verbose('Site data updated. Section: %s' % section)
      return siteData.updateData(section)
    else:
      return result
          
  ###########################################################################
  def getFile(self, fileName):
    """ Returns the contents of the given file, purging and reupdating if necessary
    """
        
    # This processing prevents exploits (such as arbitrary file concatenation or arbitrary file purging)
    baseName = self.getBaseFile(fileName)
    gLogger.verbose('Requested file: %s\nProcessed base name: %s' % (fileName, baseName))

    gLogger.verbose('File requested: %s/%s' % (baseFilePath, baseName))
    
    # Double-check that the cache is up-to-date
    fileCache.purge()
        
    result = fileCache.getFileData('%s/%s' % (baseFilePath, baseName))
    if not result['OK']:
      gLogger.verbose('File not found. Let\'s try an update...')
      # The file was not found, so (hopefully) it just needs to be regenerated
      
      # First interpret the file name
      ext = self.getExt(baseName)
      gLogger.verbose('File type detected as: %s' % ext)
      section = self.translateFile(baseName, ext)
      if not section:
        return S_ERROR('Unable to determine section from file name.')
      gLogger.verbose('Section detected as: %s' % section)
      
      # Now update the appropriate section
      result = self.updateData(section)
      if not result['OK']:
        return S_ERROR('Failed to update site data.')
      
      # Then generate the relevant data  
      gLogger.verbose('...Data updated. Generating files...')
      result = self.generateSection(section, ext, baseFilePath, fileCache, sectionDict)
      if not result['OK']:
        return S_ERROR('Failed to generate data.')
      
      # Now to check whether it was just an expiration issue, or actually a bad path
      gLogger.verbose('...Now let\'s check the file again...')  
      result = fileCache.getFileData('%s/%s' % (baseFilePath, baseName))
      if not result['OK']:
        return S_ERROR('File does not exist: %s/%s' % (baseFilePath, baseName))
        
    gLogger.verbose('File found: %s' % result['Value'])
      
    return result
  
  ###########################################################################  
  def generateSection(self, section, sectionType, baseFilePath, fileCache, sectionDict):
    """ Generates data for one or all section types (KML/PNG) for a given section
    """
    generatorFunction = {'kml' : siteData.generateKML, 'png' : siteData.generateIcons}
    if not sectionType:
      funcDict = generatorFunction
    else:
      if not sectionType in generatorFunction:
        return S_ERROR('Invalid generator type: %s' % sectionType)
      funcDict = {sectionType : generatorFunction[sectionType]}
      
    for func in funcDict:
      result = funcDict[func](section, baseFilePath, fileCache, sectionDict)
      if not result:
        return S_ERROR('Failed to generate data of type %s in section %s' % (func, section))
        
    return S_OK()
  
  ###########################################################################  
  def transfer_toClient(self, fileId, token, fileHelper):
    """ Transfers the file data to the client
        
        Note to self:
        In the receiveFile call, the 1st argument is the name of the new local file to create;
          anything sent via stringToNetwork is stored there
        You should therefore use fileId, which is also the 2nd argument of receiveFile, as a means
          of identifying the remote file you want.
    """
    gLogger.verbose('fileId: %s' % fileId)
    dataToWrite = False
    if fileId['Type'] == 'File':
      result = self.getFile(fileId['Data'])
      if not result['OK']:
        return S_ERROR('Failed to get file data. Result: %s' % result)
      dataToWrite = result['Value']
    elif fileId['Type'] == 'Section':
    
      # First, update site data
      result = self.updateData(fileId['Data'])
      if not result['OK']:
        return S_ERROR('Failed to update section data.')
        
      # Next, generate KML/PNG data
      result = self.generateSection(fileId['Data'], False, baseFilePath, fileCache, sectionDict)
      if not result['OK']:
        return S_ERROR('Failed to generate sections. Error: %s' % result)
      
      # Now enumerate the relevant files
      dataToWrite = sectionFiles[fileId['Data']] + '\n'
      fileList = os.listdir(baseFilePath)
      for f in fileList:
        ext = self.getExt(f)
        if ext == 'kml':
          continue
        elif ext == 'png':
          if f.find(sectionTag[fileId['Data']]) == 0:
            dataToWrite += f + '\n'
    elif fileId['Type'] == 'All':
      for s in sectionTag:
        # Update each section's data
        result = self.updateData(s)
        if not result['OK']:
          return S_ERROR('Failed to update data for section %s' % s)
        # Generate each section's files
        result = self.generateSection(s, False, baseFilePath, fileCache, sectionDict)
        if not result['OK']:
          return S_ERROR('Failed to generate section files for section %s' % s)
      
      fileList = os.listdir(baseFilePath)
      dataToWrite = ''
      for f in fileList:
        dataToWrite += f + '\n'
    else:
      return S_ERROR('Invalid fileId.')
      
    result = fileHelper.stringToNetwork(dataToWrite)
    return S_OK('Transfer complete.')
  
  ###########################################################################  
  def translateFile(self, fileName, ext):
    """ Returns the section related to the given file name
    """
    for section in sectionDict[ext]:
      if fileName.find(sectionDict[ext][section]) == 0:
        break
    else:
      return False
      
    return section
  
  ###########################################################################  
  def getExt(self, fileName):
    """ Returns the LOWERCASE extension of the given file
    """
    lastPeriod = fileName.rfind('.')
    lastSlash = fileName.rfind('/')
    if lastPeriod < lastSlash:
      return False
      
    fileParts = fileName.split('.')
    ext = fileParts[len(fileParts) - 1]
    return ext.lower()
  
  ###########################################################################  
  def getBaseFile(self, filePath):
    """ Returns the file name without the path
    """
    fileParts = filePath.split('/')
    baseName = fileParts[len(fileParts) - 1]
    return baseName

  #EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
