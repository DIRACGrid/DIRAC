########################################################################
# $Id: Mapping.py,v 1.4 2008/07/04 14:38:08 asypniew Exp $
########################################################################

""" All of the data collection and handling procedures for the SiteMappingHandler
"""

from DIRAC.Core.Utilities.KMLData import KMLData
from DIRAC.Core.Utilities.MappingTable import MappingTable
from DIRAC import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.Core.DISET.RPCClient import RPCClient
#from DIRAC.WorkloadManagementSystem.DB.JobDB import JobDB

import pylab

#jobDB = JobDB()

tier0 = ['LCG.CERN.ch']
tier1 = ['LCG.GRIDKA.de', 'LCG.CNAF.it', 'LCG.PIC.es', 'LCG.RAL.uk', 'LCG.IN2P3.fr', 'LCG.NIKHEF.nl']

class Mapping:

  ###########################################################################
  def __init__(self):
    self.siteData = {}
    self.plotFigure = pylab.figure(figsize=(1,1))
    self.plotFigure.figurePatch.set_alpha(0.0)
    
  ###########################################################################
  def getDict(self):
    return S_OK(self.siteData)

  ###########################################################################
  def updateData(self, section):
  
    gLogger.verbose('Update request received. Section: %s' % section)
  
    wmsAdmin = RPCClient('WorkloadManagement/WMSAdministrator',useCertificates=True)
    jobMon = RPCClient('WorkloadManagement/JobMonitoring',useCertificates=True)
    
    ##############################
    if section == 'SiteData':
      siteListPath = 'Resources/Sites/LCG'
      result = gConfig.getSections(siteListPath)
      if not result['OK']:
        gLogger.verbose('Site list could not be retrieved.')
        return S_ERROR('Site list could not be retrieved.')
        
      siteList = result['Value']
      for site in siteList:
        result = gConfig.getOptions(siteListPath + '/' + site)
        if not result['OK']:
          gLogger.verbose('Site section could not be retrieved.')
          return S_ERROR('Site section could not be retrieved.')
        siteSection = result['Value']
        if 'Name' in siteSection:
          if 'Coordinates' in siteSection:
            result = gConfig.getValue(siteListPath + '/' + site + '/Coordinates')
            if not result:
              continue
            coord = result.split(':')
            if site in tier0:
              cat = 'T0'
            elif site in tier1:
              cat = 'T1'
            else:
              cat = 'T2'
            self.siteData[site] = {'Coord' : (float(coord[0]), float(coord[1])), 'Cat' : cat}
            
            self.siteData[site]['Mask'] = 'Unknown'
          
    ##############################
    elif section == 'SiteMask':
      # Update site mask data
      #mask = self.getSiteMask()
      mask = wmsAdmin.getSiteMask()
      gLogger.verbose('Mask: %s' % mask)
      if not mask['OK']:
        gLogger.verbose('Failed to retrieve site mask. Error: %s' % mask['Value'])
        return S_ERROR('Failed to retrieve site mask.')
      gLogger.verbose('Site mask: %s' % mask['Value'])

      #Get the overall list of sites from the Monitoring
      distinctSites = jobMon.getSites()
      if not distinctSites['OK']:
        gLogger.verbose('Failed to retrieve site mask. Error: %s' % distinctSites['Value'])
        return S_ERROR('Failed to retrieve site listing.')
      gLogger.verbose('Site listing: %s' % distinctSites['Value'])
    
      # Generate a dictionary of valid sites
      for node in distinctSites['Value']:
        if node not in self.siteData:
          continue
        if node in mask['Value']:
          self.siteData[node]['Mask'] = 'Good'
        else:
          self.siteData[node]['Mask'] = 'Banned'
    
    ##############################      
    elif section == 'JobSummary':
      #Update job summary data
      siteSummary = jobMon.getSiteSummary()
      if not siteSummary['OK']:
        gLogger.verbose('Failed to retrieve site summary data. Result: %s' % siteSummary)
        return S_ERROR('Site summary data could not be retrieved.')
    
      for node in siteSummary['Value']:
        if node not in self.siteData:
          continue
        self.siteData[node]['JobSummary'] = {'Done' : 0, 'Running' : 0, 'Stalled' : 0, 'Waiting' : 0, 'Failed' : 0}
        for key in siteSummary['Value'][node]:
          self.siteData[node]['JobSummary'][key] = int(siteSummary['Value'][node][key])
          
    ##############################
    elif section == 'PilotSummary':
      # Begin by initializing every site (so we don't get dictionary key exceptions)
      #for node in self.siteData:
      #  self.siteData[node]['PilotSummary'] = {}
      # MOVED: It is now located jsut before the key is accessed
      
      # Retrieve data on the pilots, and compare them to each sites' computing elements ('children')
      # so that we can categorize the data  
      pilotSummary = wmsAdmin.getPilotSummary()
      if not pilotSummary['OK']:
        return S_ERROR('Site pilot data could not be retrieved.')
      children = gConfig.getOptions('Resources/GridSites/LCG')
      
      # Iterate through every pilot
      for child in pilotSummary['Value']:
      
        # If the pilot is not in the Resources/GridSites/LCG list, then
        # we won't be able to detect its parent, making it orphaned (we don't want that)
        if child not in children['Value']:
          continue
        
        # Ah, but it does have a parent!
        # Make sure it is one we recognize
        parent = gConfig.getValue('Resources/GridSites/LCG/' + child)
        if parent not in self.siteData:
          continue
                
        # Yes, it is. Add it to the site database
        self.siteData[parent]['PilotSummary'] = {}
        self.siteData[parent]['PilotSummary'][child] = {'Done' : 0, 'Aborted' : 0, 'Submitted' : 0, 'Cleared' : 0, 'Ready' : 0, 'Scheduled' : 0, 'Running' : 0}
        for key in pilotSummary['Value'][child]:
          self.siteData[parent]['PilotSummary'][child][key] = int(pilotSummary['Value'][child][key])
          
    else:
      return S_ERROR('Invalid update section %s' % section)
      
    gLogger.verbose('Update complete. Current site data: %s' % self.siteData)
    
    return S_OK(self.siteData)
  
  ###########################################################################
  def generateKML(self, section, filePath, fileCache, dataDict):
  
    gLogger.verbose('KML generation request received. Section: %s' % section)
    
    # scaleData is for scaling done on a tier basis
    scaleData = {'T0' : 1.0, 'T1' : 0.9, 'T2' : 0.6}
    # maxScale, minScale are for scaling done on a site data basis (e.g., number of jobs)
    maxScale = 1
    minScale = 0.2
    
    # I want these scaling variables to range from 0 to 1.
    # So, if the browser or plot generation files generate files which are displaying
    #   too small, adjust this scaleAdjust rather than fooling around with all the other values.
    scaleAdjust = 0.3
    for x in scaleData:
      scaleData[x] += scaleAdjust
    maxScale += scaleAdjust
    minScale += scaleAdjust
    
    KML = KMLData()
    #fileName = ''
    
    sectionFile = dataDict['kml']
    sectionTag = dataDict['png']
    
    iconPath = dataDict['IconPath']
    if iconPath:
      iconPath = iconPath.rstrip('/')
      iconPath += '/'
    
    ##############################
    if section == 'SiteMask':
      #fileName = 'sitemask.kml'
      KML.addMultipleScaledStyles(iconPath, ('%s-green' % sectionTag[section], '%s-red' % sectionTag[section], '%s-gray' % sectionTag[section]), scaleData, '.png')	
      for node in self.siteData:
        if 'Mask' not in self.siteData[node]:
          continue
        if self.siteData[node]['Mask'] == 'Good':
          icon = '%s-green%s' % (sectionTag[section], self.siteData[node]['Cat'])
        elif self.siteData[node]['Mask'] == 'Banned':
          icon = '%s-red%s' % (sectionTag[section], self.siteData[node]['Cat'])
        else:
          icon = '%s-gray%s' % (sectionTag[section], self.siteData[node]['Cat'])
        KML.addNode(node, 'More info', icon, self.siteData[node]['Coord'])
    
    ##############################    
    elif section == 'JobSummary':
      #fileName = 'jobsummary.kml'
      
      # This algorithm computes the relative sizes for each node
      # First, collect a data set
      numJobs = []
      for node in self.siteData:
        if 'JobSummary' not in self.siteData[node]:
          continue
        num = 0
        for state in self.siteData[node]['JobSummary']:
          num += self.siteData[node]['JobSummary'][state]
        numJobs.append(num)  
      # Now sort the data into percentile categories
      numJobs.sort()
      dataSize = len(numJobs)
      numCat = False # For continuous scaling, set to False
      if numCat:
        if not dataSize:  # Just to be safe
          dataSize = 1
        sectionSize = dataSize // numCat
        if dataSize % numCat != 0:  # This avoids over-scaling later on (scaling greater than maxScale)
          sectionSize += 1
      scaleDict = {}
      for i in range(0, dataSize):
        if numCat:
          scaleValue = (float(i) // sectionSize) + 1
          gLogger.verbose('debug: %.2f' % scaleValue)
          scaleValue /= numCat
        else:
          scaleValue = float(i + 1) / dataSize
        scaleDict[str(numJobs[i])] = (float(scaleValue) * (maxScale - minScale)) + minScale
        gLogger.verbose('i=%d, scaleValue=%.2f, scaleDict=%.2f' % (i, scaleValue, scaleDict[str(numJobs[i])]))
        
      # Now actually generate the KML  
      for node in self.siteData:
        if 'JobSummary' not in self.siteData[node]:
          continue
          
        total = 0
        for state in self.siteData[node]['JobSummary']:
          total += self.siteData[node]['JobSummary'][state]
          
        # Generate node style
        KML.addNodeStyle('%s-%s' % (sectionTag[section],node), '%s%s-%s.png' % (iconPath,sectionTag[section],node), scaleDict[str(total)])#1)#scaleData[self.siteData[node]['Cat']])
        # Generate description
        description = "Done: %d<br/>Running: %d<br/>Stalled: %d<br/>Waiting: %d<br/>Failed: %d" %\
                      (self.siteData[node]['JobSummary']['Done'], self.siteData[node]['JobSummary']['Running'], self.siteData[node]['JobSummary']['Stalled'],\
                      self.siteData[node]['JobSummary']['Waiting'], self.siteData[node]['JobSummary']['Failed'])
        # Add the node
        KML.addNode(node, description, '%s-%s' % (sectionTag[section],node), self.siteData[node]['Coord'])
      
    ##############################
    elif section == 'PilotSummary':
      #fileName = 'pilotsummary.kml'
      for node in self.siteData:
        if 'PilotSummary' not in self.siteData[node]:
          continue
        # Generate node style
        KML.addNodeStyle('%s-%s' % (sectionTag[section],node), '%s%s-%s.png' % (iconPath,sectionTag[section],node), scaleData[self.siteData[node]['Cat']])
        # Generate description
      
        table = MappingTable()
        table.setColumns(['CE Name', 'Done', 'Cleared', 'Aborted', 'Ready', 'Scheduled', 'Running', 'Submitted'])
        for child in self.siteData[node]['PilotSummary']:
          table.addRow([child, self.siteData[node]['PilotSummary'][child]['Done'],
                               self.siteData[node]['PilotSummary'][child]['Cleared'],
                               self.siteData[node]['PilotSummary'][child]['Aborted'],
                               self.siteData[node]['PilotSummary'][child]['Ready'],
                               self.siteData[node]['PilotSummary'][child]['Scheduled'],
                               self.siteData[node]['PilotSummary'][child]['Running'],
                               self.siteData[node]['PilotSummary'][child]['Submitted']])
        
        # Write the node
        KML.addNode(node, table.tableToHTML(), '%s-%s' % (sectionTag[section],node), self.siteData[node]['Coord'])
      
    ##############################
    else:
      return S_ERROR('Invalid generation section %s' % section)
      
    data = KML.getKML()
    gLogger.verbose('KML generation complete. Data: %s' % data)
    
    KML.writeFile('%s/%s' % (filePath, sectionFile[section]))
    gLogger.verbose('KML data stored to: %s/%s' % (filePath, sectionFile[section]))
    
    fileCache.addToCache('%s/%s' % (filePath, sectionFile[section]))
    
    return S_OK(data)       
    
  #############################################################################
  def generateIcons(self, section, filePath, fileCache, dataDict):
 
    sectionFile = dataDict['kml']
    sectionTag = dataDict['png']
  
    gLogger.verbose('Icon generation request received. Section: %s' % section)
    
    ##############################
    if section == 'SiteMask':
      colorDict = {'green' : '#00ff00', 'red' : '#ff0000', 'gray' : '#666666'}
      for color in colorDict:
        
        fileName = '%s/%s-%s.png' % (filePath, sectionTag[section], color)
        
        data = (100,)
        
        pylab.pie(data, colors=(colorDict[color],))
        pylab.savefig(fileName)
        
        fileCache.addToCache(fileName)
    
    ##############################  
    elif section == 'JobSummary':
      # Done, Running, Stalled, Waiting, Failed, respectively
      colorList = ('#00ff00', '#ff7f00', '#0000ff', '#ffff00', '#ff0000')
      
      # Generate icons
      for node in self.siteData:
        if 'JobSummary' not in self.siteData[node]:
          continue
        
        fileName = '%s/%s-%s.png' % (filePath,sectionTag[section],node)
        
        data = (self.siteData[node]['JobSummary']['Done'], self.siteData[node]['JobSummary']['Running'],\
                self.siteData[node]['JobSummary']['Stalled'], self.siteData[node]['JobSummary']['Waiting'],\
                self.siteData[node]['JobSummary']['Failed'])
                
        pylab.pie(data, colors=colorList)
        pylab.savefig(fileName)
        
        fileCache.addToCache(fileName)
    
    ##############################    
    elif section == 'PilotSummary':
      # Done + Cleared, Aborted
      colorList = ('#00ff00', '#ff0000')
      
      for node in self.siteData:
        if 'PilotSummary' not in self.siteData[node]:
          continue
          
        doneCleared = 0
        aborted = 0
        for child in self.siteData[node]['PilotSummary']:
          doneCleared += self.siteData[node]['PilotSummary'][child]['Done'] + self.siteData[node]['PilotSummary'][child]['Cleared']
          aborted += self.siteData[node]['PilotSummary'][child]['Aborted']
                   
        # Generate plot icon for node
        fileName = '%s/%s-%s.png' % (filePath,sectionTag[section],node)
        data = (doneCleared, aborted)
        pylab.pie(data, colors=colorList)
        pylab.savefig(fileName)
        fileCache.addToCache(fileName)
      
    ##############################  
    else:
      return S_ERROR('Invalid icon generation section %s' % section)
      
    gLogger.verbose('Icon generation complete.')
            
    return S_OK('%s icons generated in %s.' % (section, filePath))
    
  #############################################################################
#  def getSiteMask(self):
#    """ Return the site mask
#    """
#    return jobDB.getSiteMask('Active')


  #EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
