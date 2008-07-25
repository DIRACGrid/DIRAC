########################################################################
# $Id: SiteMappingHandler.py,v 1.8 2008/07/25 13:04:52 asypniew Exp $
########################################################################

""" The SiteMappingHandler...
"""

__RCSID__ = "$Id: SiteMappingHandler.py,v 1.8 2008/07/25 13:04:52 asypniew Exp $"

from types import *
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.Core.Utilities.MappingFileCache import MappingFileCache
from DIRAC import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client import PathFinder
from DIRAC.Core.Utilities.Mapping import Mapping
import os, re

siteData = Mapping()
fileCache = False

dataDict = {}
gAnimationThread = False

# Be prepared for ALL files from this directory to possibly be deleted
baseFilePath = ''

def initializeSiteMappingHandler( serviceInfo ):

  gLogger.verbose('-----> Initializing handler...')
  csSection = PathFinder.getServiceSection('Monitoring/SiteMapping')
  gLogger.verbose('csSection: %s' % csSection)
  
  global siteData
  global fileCache
  global dataDict
  global baseFilePath
  
  # Parse the sectionFiles
  sectionFiles = gConfig.getOptionsDict(csSection+'/PlotViews')['Value']
  for section in sectionFiles:
      sectionFiles[section] = sectionFiles[section].split(':')
      
  sectionTag = gConfig.getOptionsDict(csSection+'/PlotTags')['Value']
  iconPath = gConfig.getValue(csSection+'/IconPath', '')
  
  animatedRanges = gConfig.getOptionsDict(csSection+'/AnimatedRanges')['Value']
  for r in animatedRanges:
    if r.find('.'):
      animatedRanges[r] = float(animatedRanges[r])
    else:
      animatedRanges[r] = int(animatedRanges[r])
      
  updateExpiration = gConfig.getValue(csSection+'/UpdateExpiration', 10)
  
  # Do NOT interpret it, in case it is NOT a file path
#  if iconPath:
#    iconPath = iconPath.rstrip('/')
#    iconPath += '/'

  dataDict = {'kml' : sectionFiles, 'img' : sectionTag, 'IconPath' : iconPath, 'Animated' : animatedRanges, 'UpdateExpiration' : updateExpiration}

  gLogger.verbose('dataDict: %s' % dataDict)
  
  cacheDir = gConfig.getValue(csSection+'/CacheDir','')
  cacheDir = cacheDir.rstrip('/')
  baseFilePath = cacheDir + '/SiteMapping'
  if not os.path.exists(baseFilePath):
    os.mkdir(baseFilePath)
  gLogger.verbose('baseFilePath: %s' % baseFilePath)
  
  try:
    fileTest = open('%s/%s' % (baseFilePath, 'permission.tmp'), 'w')
    fileTest.write('Z')
    fileTest.close()
    fileTest = open('%s/%s' % (baseFilePath, 'permission.tmp'), 'r')
    inputTest = fileTest.read()
    fileTest.close()
    os.unlink('%s/%s' % (baseFilePath, 'permission.tmp'))
    if inputTest != 'Z':
      raise Exception
  except:
    gLogger.verbose('---- ERROR: File cache cannot access data in %s' % baseFilePath)
    
  cacheTimeToLive = gConfig.getValue(csSection+'/CacheTime', 60)
  gLogger.verbose('cacheTimeToLive: %s' % cacheTimeToLive)
  
  cacheExceptions = gConfig.getValue(csSection+'/CacheExceptions', '')
  if cacheExceptions:
    cacheExceptions = cacheExceptions.split(';')
    dataDict['CacheExceptions'] = cacheExceptions
    # We are going to add on the baseFilePath so that the file cache will include the entire file in its exclusions
    for i in range(len(cacheExceptions)):
      cacheExceptions[i] = re.escape(baseFilePath + '/') + cacheExceptions[i]
      
  gLogger.verbose('cacheExceptions: %s' % cacheExceptions)
  
  fileCache = MappingFileCache(int(cacheTimeToLive), cacheExceptions)
    
  return S_OK()

class SiteMappingHandler( RequestHandler ):

  ###########################################################################
  types_updateTimeSeries = []
  def export_updateTimeSeries(self):
    """ Simple wrapper for updating time series data
    """
    gLogger.verbose('Time series update requested received.')
    result = self.updateData('JobSummary', False)
    if not result['OK']:
      return result
    return S_OK('Time series data updated.')

  ###########################################################################
  def isSiteDataLoaded(self):
    """ Returns True or False depending on whether any site data is present
    """
    if 'LCG.CERN.ch' not in siteData.siteData:
      return False
    else:
      return True
  
  ###########################################################################
  def purgeAll(self):
    """ Manually purge all of the expired files
    """
    fileCache.purge()
    gLogger.verbose('File cache manually purged.')
    return S_OK('File cache manually purged.')
  
  ########################################################################### 
  def updateData(self, section, updateData=True): 
    """ Update site data
        To update the site data AND THEN update the given section, set 'section' and updateData=True
        To update the given section BUT NOT the site data, set 'section' and updateData=False
        To update ONLY the site data, set section=False (updateData is ignored)
        HOWEVER, if no site data is loaded, then SiteData is ALWAYS called.
    """ 
    
    result = S_OK()
    
    # First, update the list of sites.
    # If you ONLY want to update this, set section=False
    if not section or updateData or not self.isSiteDataLoaded():
      result = siteData.updateData('SiteData', dataDict)
      gLogger.verbose('Site data updated.')
      if not result['OK']:
        return result
      
    if section:
      result = siteData.updateData(section, dataDict)
      if result['OK']:
        gLogger.verbose('Site data updated. Section: %s' % section)

    return result
    
  ###########################################################################
  def checkDependencies(self, baseName):
    """ Ensures that all dependencies for baseName are in the cache
    """
    
    gLogger.verbose('Checking dependencies for: %s' % baseName)
        
    # If we don't even have a list, then obviously stuff isn't there
    if not siteData.dependencies:
      gLogger.verbose('Dependency list does not exist.')
      return False
      
    # Found out which section we need to check
    ext = self.getExt(baseName)
    gLogger.verbose('File type detected as: %s' % ext)
    dataType = self.translateType(ext)
    gLogger.verbose('Data type detected as: %s' % dataType)
    section = self.translateFile(baseName, dataType)
    
    # If the section isn't listed, then obviously it isn't updated.
    if section not in siteData.dependencies:
      gLogger.verbose('Section %s not found in dependency list.' % section)
      return False
          
    gLogger.verbose('Dependencies are located in section: %s' % section)
      
    # Now generate a list of cached files
    fileList = os.listdir(baseFilePath)
    
    # Now make sure everything is there
    for fileName in siteData.dependencies[section]:
      if fileName not in fileList:
        for reg in dataDict['CacheExceptions']:
          if re.match(reg, fileName):
            # It's an exception: ignore it
            break
        else:
          # It should have been there.
          gLogger.verbose('Dependency check failed. File missing: %s' % fileName)
          return False
    else:
      gLogger.verbose('All dependencies appear to be in order.')
      return True
          
  ###########################################################################
  def getFile(self, fileName, params=[]):
    """ Returns the contents of the given file, purging and reupdating if necessary
    """
        
    # This processing prevents exploits (such as arbitrary file concatenation or arbitrary file purging)
    baseName = self.getBaseFile(fileName)
    gLogger.verbose('Requested file: %s\nProcessed base name: %s' % (fileName, baseName))
    gLogger.verbose('File requested: %s/%s' % (baseFilePath, baseName))
    
    # Double-check that the cache is up-to-date
    fileCache.purge()
    
    # Contigent updating?
    if 'DependencyUpdateAll' in params:
      # If a dependency is missing, then force an update;
      #   otherwise, don't do anything
      if not self.checkDependencies(baseName):
        gLogger.verbose('Dependency check failed. Forcing update.')
        params.append('ForceUpdateAll')
    
    # If we want to force an update, just pretend the file doesn't exist
    if 'ForceUpdateAll' in params:
      gLogger.verbose('Forced update received for file: %s' % baseName)
      result = {'OK' : False}
    else:
      result = fileCache.getFileData('%s/%s' % (baseFilePath, baseName))
      
    if not result['OK']:  
      gLogger.verbose('File not found. Let\'s try an update...')
      # The file was not found, so (hopefully) it just needs to be regenerated
      
      # First interpret the file name
      ext = self.getExt(baseName)
      gLogger.verbose('File type detected as: %s' % ext)
      dataType = self.translateType(ext)
      gLogger.verbose('Data type detected as: %s' % dataType)
      section = self.translateFile(baseName, dataType)
      if not section:
        return S_ERROR('Unable to determine section from file name.')
      gLogger.verbose('Section detected as: %s' % section)
      
      # Now update the appropriate section
      result = self.updateData(section, True)
      if not result['OK']:
        return S_ERROR('Failed to update site data.')
      
      # Then generate the relevant data  
      gLogger.verbose('...Data updated. Generating files...')
      
      # If we want to update everything, set dataType = False so all types are generated
      if 'ForceUpdateAll' in params:
        dataType = False
      result = self.generateSection(section, dataType, baseFilePath, fileCache, dataDict)
      if not result['OK']:
        return S_ERROR('Failed to generate data.')
      
      # Now to check whether it was just an expiration issue, or actually a bad path
      gLogger.verbose('...Now let\'s check the file again...')  
      result = fileCache.getFileData('%s/%s' % (baseFilePath, baseName))
      if not result['OK']:
        return S_ERROR('File does not exist: %s/%s' % (baseFilePath, baseName))
        
    gLogger.verbose('File found: %s/%s' % (baseFilePath, baseName))
      
    return result
  
  ###########################################################################  
  def generateSection(self, section, dataType, baseFilePath, fileCache, dataDict):
    """ Generates data for one or all section types (KML/IMG) for a given section
    """
    generatorFunction = {'kml' : siteData.generateKML, 'img' : siteData.generateIcons}
    if not dataType:
      funcDict = generatorFunction
    else:
      if not dataType in generatorFunction:
        return S_ERROR('Invalid generator type: %s' % dataType)
      funcDict = {dataType : generatorFunction[dataType]}
      
    for func in funcDict:
      result = funcDict[func](section, baseFilePath, fileCache, dataDict)
      if not result['OK']:
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
          
        Anyway...
        fileId should be a dictionary with keys 'Type' and 'Data'
          The value for 'Type' should be 'File', 'Section', or 'All' depending on what you want
          If 'Type' is 'File', then 'Data' should be the file name to retrieve
          If 'Type' is 'Section', then 'Data' should be the section to enumerate
          If 'Type' is 'All', then 'Data' is ignored.
        If 'Type' is NOT 'File', then the returned file will be a '\n' separated list of file names
          requested. You need to call the transfer client again with each of these file names to
          actually retrieve the useful data.
    """
    gLogger.verbose('Transfer requested received: %s' % fileId)
    dataToWrite = False
    if fileId['Type'] == 'File' or fileId['Type'] == 'File_CheckDependencies' or fileId['Type'] == 'File_ForceAll':
      if fileId['Type'] == 'File_CheckDependencies':
        params = ['DependencyUpdateAll']
      elif fileId['Type'] == 'File_ForceAll':
        params = ['ForceUpdateAll']
      else:
        params = []
      result = self.getFile(fileId['Data'], params)
      if not result['OK']:
        return S_ERROR('Failed to get file data. Result: %s' % result)
      dataToWrite = result['Value']
    elif fileId['Type'] == 'Section' or fileId['Type'] == 'Association':
    
      # Since Association is so similar to section,
      #   I've just shoved its code in here.
      if fileId['Type'] == 'Association':
        gLogger.verbose('Received association request.')
        ext = self.getExt(fileId['Data'])
        gLogger.verbose('File type detected as: %s' % ext)
        dataType = self.translateType(ext)
        gLogger.verbose('Data type detected as: %s' % dataType)
        section = self.translateFile(fileId['Data'], dataType)
        if not section:
          return S_ERROR('Unable to determine section from file name.')
        gLogger.verbose('Section detected as: %s' % section)
        # Now just continue like a regular section update
        fileId['Type'] = 'Section'
        fileId['Data'] = section
    
      # First, update site data
      result = self.updateData(fileId['Data'], True)
      if not result['OK']:
        return S_ERROR('Failed to update section data.')
        
      # Next, generate KML/IMG data
      result = self.generateSection(fileId['Data'], False, baseFilePath, fileCache, dataDict)
      if not result['OK']:
        return S_ERROR('Failed to generate sections. Error: %s' % result)
      
      # Now enumerate the relevant files
      dataToWrite = ''
      for f in dataDict['kml'][fileId['Data']]:
        dataToWrite += f + '\n'
      fileList = os.listdir(baseFilePath)
      for f in fileList:
        ext = self.getExt(f)
        dataType = self.translateType(ext)
        
        if dataType == 'kml':
          # We already listed this one
          continue
        elif dataType == 'img':
          # Search for the tag which designates the appropriate section
          if f.find(dataDict[dataType][fileId['Data']]) == 0:
            dataToWrite += f + '\n'
    elif fileId['Type'] == 'All':
      # Update site data only once
      result = self.updateData(False, False)
      for s in dataDict['img']:
        # Update each section's data
        result = self.updateData(s, False)
        if not result['OK']:
          return S_ERROR('Failed to update data for section %s' % s)
        # Generate each section's files
        result = self.generateSection(s, False, baseFilePath, fileCache, dataDict)
        if not result['OK']:
          return S_ERROR('Failed to generate section files for section %s' % s)
      
      fileList = os.listdir(baseFilePath)
      dataToWrite = ''
      for f in fileList:
        dataToWrite += f + '\n'
    else:
      return S_ERROR('Invalid fileId.')
      
#    print '--------- DEBUG -------'
#    print 'dataToWrite: %s' % dataToWrite
    result = fileHelper.stringToNetwork(dataToWrite)
    if not result['OK']:
      return S_ERROR('stringToNework failed.')
    else:
      return S_OK('Transfer complete.')
  
  ###########################################################################  
  def translateFile(self, fileName, dataType):
    """ Returns the section related to the given file name
    """
    for section in dataDict[dataType]:
      # If it is KML, then there might be multiple associated files in search through
      if dataType == 'kml':
        for f in dataDict[dataType][section]:
          if fileName.find(f) == 0:
            return section
      # If it is IMG, then there is only one associated file tag to look for
      else:
        if fileName.find(dataDict[dataType][section]) == 0:
          return section
      
    return False
  
  ###########################################################################  
  def translateType(self, ext):
    """ Converts a file extension into a data type (KML or IMG)
    """
    if ext == 'gif' or ext == 'png':
      dataType = 'img'
    elif ext == 'kml':
      dataType = 'kml'
    else:
      dataType = False
      
    return dataType
  
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
