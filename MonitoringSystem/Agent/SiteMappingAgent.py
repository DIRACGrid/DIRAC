""" SiteMapping Agent--responsible for maintaining up-to-date data by pinging the service with requests
"""

from DIRAC.Core.Base.Agent import Agent
from DIRAC import S_OK, S_ERROR, gConfig, gLogger
from DIRAC.Core.Utilities.Mapping2 import Mapping
from DIRAC.ConfigurationSystem.Client import PathFinder
import os, re

AGENT_NAME = 'Monitoring/SiteMappingAgent'

siteData = Mapping()
dataDict = {}
baseFilePath = ''

class SiteMappingAgent(Agent):

  ###########################################################################
  def __init__(self):
    """ Standard constructor
    """
    Agent.__init__(self,AGENT_NAME)

  ###########################################################################
  def initialize(self):
    """ Initialize the agent; retrieve configuration information
    """
    result = Agent.initialize(self)
    if not result['OK']:
      self.log.error('Agent could not initialize')
      return result
      
    # Retrieve our current configuration directory
    gLogger.verbose('-----> Initializing handler...')
    csSection = PathFinder.getAgentSection('Monitoring/SiteMapping')
    gLogger.verbose('csSection: %s' % csSection)
  
    # We need to initialize these global variables
    global siteData
    global dataDict
    global baseFilePath
  
    # Retrieve the file names for the KML
    result = gConfig.getOptionsDict(csSection+'/PlotViews')
    if not result['OK']:
      return S_ERROR('Failed to retrieve plot view configuration data. Error: %s' % result)
    sectionFiles = result['Value']
    
    for section in sectionFiles:
      sectionFiles[section] = sectionFiles[section].split(':')
    
    # Retrieve the image tags and the icon prefix
    result = gConfig.getOptionsDict(csSection+'/PlotTags')
    if not result['OK']:
      return S_ERROR('Failed to retrieve plot tag configuration data. Error: %s' % result)
    sectionTag = result['Value']
    
    iconPath = gConfig.getValue(csSection+'/IconPath', '')
  
    # Retrieve the animation data
    result = gConfig.getOptionsDict(csSection+'/AnimatedRanges')
    if not result['OK']:
      return S_ERROR('Failed to retrieve animation range configuration data. Error: %s' % result)
    animatedRanges = result['Value']
    for r in animatedRanges:
      if r.find('.'):
        animatedRanges[r] = float(animatedRanges[r])
      else:
        animatedRanges[r] = int(animatedRanges[r])
    
    # Compile the data
    dataDict = {'kml' : sectionFiles, 'img' : sectionTag, 'IconPath' : iconPath, 'Animated' : animatedRanges}
    gLogger.verbose('dataDict: %s' % dataDict)
  
    # Retrieve and create the directory for files
    cacheDir = gConfig.getValue(csSection+'/CacheDir','/opt/dirac/work')
    cacheDir = cacheDir.rstrip('/')
    baseFilePath = cacheDir + '/SiteMappingAgent'
    if not os.path.exists(baseFilePath):
      os.mkdir(baseFilePath)
    gLogger.verbose('baseFilePath: %s' % baseFilePath)
  
    # Check permissions
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
      return S_ERROR('Permissions conflict in cache directory.')
      
    # Create the execution counter
    self.__counter = 0
        
    return S_OK()

  ###########################################################################
  def execute(self):
    """ The main agent execution method
    """
    
    cycle = self.__counter % 5
    
    gLogger.verbose('Execution counter: %d (cycle %d)' % (self.__counter, cycle))
    
    if cycle == 0:
      self.generateAll()
    else:
      self.updateTimeSeries()
    
    self.__counter += 1
    if self.__counter >= 100:
      self.__counter = 0

    return S_OK()
    
  ###########################################################################
  def updateTimeSeries(self):
    """ Simple wrapper for updating time series data
    """
    gLogger.verbose('Time series update requested received.')
    result = self.updateData('JobSummary', False)
    if not result['OK']:
      return result
    gLogger.verbose('Time series update complete.')
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
  def generateSection(self, section, dataType):
    """ Generates data for one or all section types (KML/IMG) for a given section.
        If dataType=None, then this will generate both KML and IMG files for the section.
        To generate all sections, use the generateAll() call instead
        baseFilePath is the directory to put the files in.
    """
    generatorFunction = {'kml' : siteData.generateKML, 'img' : siteData.generateIcons}
    if not dataType:
      funcDict = generatorFunction
    else:
      if not dataType in generatorFunction:
        return S_ERROR('Invalid generator type: %s' % dataType)
      funcDict = {dataType : generatorFunction[dataType]}
      
    for func in funcDict:
      result = funcDict[func](section, baseFilePath, dataDict)
      if not result['OK']:
        return S_ERROR('Failed to generate data of type %s in section %s' % (func, section))
        
    return S_OK()
    
  ########################################################################### 
  def generateAll(self):
    """ Generates new files for all sections
    """
    gLogger.verbose('Global generation cycle...')
    
    # Update the site listing
    result = self.updateData(False, False)
    if not result['OK']:
      return S_ERROR('Failed to update base site data.')
      
    for s in dataDict['img']:
    
      # Update each section's data
      result = self.updateData(s, False)
      if not result['OK']:
        return S_ERROR('Failed to update data for section %s' % s)
        
      # Generate each section's files
      result = self.generateSection(s, False)
      if not result['OK']:
        return S_ERROR('Failed to generate section files for section %s' % s)
        
    gLogger.verbose('Global generation complete.')

  #EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
