########################################################################
# $Id: Mapping.py,v 1.10 2008/07/29 11:33:29 asypniew Exp $
########################################################################

""" All of the data collection and handling procedures for the SiteMappingHandler
"""

from DIRAC.Core.Utilities.KMLData import KMLData
from DIRAC import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.AccountingSystem.Client.ReportsClient import ReportsClient
from DIRAC.Core.Utilities.TimeSeries import TimeSeries

import datetime, os, time
import pylab

tier0 = ['LCG.CERN.ch']
tier1 = ['LCG.GRIDKA.de', 'LCG.CNAF.it', 'LCG.PIC.es', 'LCG.RAL.uk', 'LCG.IN2P3.fr', 'LCG.NIKHEF.nl']

class Mapping:

  ###########################################################################
  def __init__(self):
    self.siteData = {}
    self.timeSeries = {}
    
  ###########################################################################
  def getDict(self):
    return S_OK(self.siteData)

  ###########################################################################
  def updateData(self, section, dataDict):
  
    gLogger.verbose('Update request received. Section: %s' % section)
  
    wmsAdmin = RPCClient('WorkloadManagement/WMSAdministrator',useCertificates=True)
    jobMon = RPCClient('WorkloadManagement/JobMonitoring',useCertificates=True)
    storUse = RPCClient('DataManagement/StorageUsage',useCertificates=True)
        
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
      mask = wmsAdmin.getSiteMask()
      if not mask['OK']:
        gLogger.verbose('Failed to retrieve site mask. Error: %s' % mask['Value'])
        return S_ERROR('Failed to retrieve site mask.')

      # Get the overall list of sites from the Monitoring
      distinctSites = jobMon.getSites()
      if not distinctSites['OK']:
        gLogger.verbose('Failed to retrieve site mask. Error: %s' % distinctSites['Value'])
        return S_ERROR('Failed to retrieve site listing.')
    
      # Generate a dictionary of valid sites
      for node in distinctSites['Value']:
        if node not in self.siteData:
          continue
        if node in mask['Value']:
          self.siteData[node]['Mask'] = 'Allowed'
        else:
          self.siteData[node]['Mask'] = 'Banned'
    
    ##############################      
    elif section == 'JobSummary':
      # Update job summary data
      siteSummary = jobMon.getSiteSummary()
      if not siteSummary['OK']:
        gLogger.verbose('Failed to retrieve site summary data. Result: %s' % siteSummary)
        return S_ERROR('Site summary data could not be retrieved.')
           
      for node in siteSummary['Value']:
        if node not in self.siteData:
          continue
          
        total = 0
        # First, grab the data
        self.siteData[node]['JobSummary'] = {'Done' : 0, 'Running' : 0, 'Stalled' : 0, 'Waiting' : 0, 'Failed' : 0}
        for key in siteSummary['Value'][node]:
          self.siteData[node]['JobSummary'][key] = int(siteSummary['Value'][node][key])      
          total += self.siteData[node]['JobSummary'][key]
          
        # Now store the time series data
        if node not in self.timeSeries:
          self.timeSeries[node] = {}
        if 'TotalJobs' not in self.timeSeries[node]:
          self.timeSeries[node]['TotalJobs'] = TimeSeries(False, datetime.timedelta(seconds=dataDict['Animated']['MaxAge']), datetime.timedelta(seconds=dataDict['Animated']['MinAge']))
        self.timeSeries[node]['TotalJobs'].add(total)
          
    ##############################
    elif section == 'PilotSummary':      
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
    
    ##############################
    elif section == 'DataStorage':
      storageSummary = storUse.getStorageSummary()
      if not storageSummary['OK']:
        return S_ERROR('Failed to retrieve data storage summary.')
                      
      for element in storageSummary['Value']:
        # Sort each storage element into a list of the form [name, type]
        if element.find('_') != -1:
          se = element.split('_')
        else:
          se = element.split('-')
                    
        for node in self.siteData:
          if node.find(se[0]) != -1:
            if 'DataStorage' not in self.siteData[node]:
              self.siteData[node]['DataStorage'] = {}
            self.siteData[node]['DataStorage'][se[1].upper()] = storageSummary['Value'][element]
            break
            
    ##############################    
    elif section == 'Animated':
      # Um, don't do anything.
      gLogger.verbose('Animated section update -- nothing to do.')
    
    ##############################    
    else:
      return S_ERROR('Invalid update section %s' % section)
          
    return S_OK(self.siteData)
  
  ###########################################################################
  def generateKML(self, section, filePath, dataDict):
  
    gLogger.verbose('KML generation request received. Section: %s' % section)
    
    # scaleData is for scaling done on a tier basis
    scaleData = {'T0' : 1.0, 'T1' : 0.9, 'T2' : 0.6}
    
    # maxScale, minScale are for scaling done on a site data basis (e.g., number of jobs)
    maxScale = 1
    minScale = 0.2
    
    # These are block-level tags for styling names/descriptions
    # They references class in infostyles.css
    tagStyleNodeName = '<h6 class=\"nodeNameSM\">'
    tagStyleNodeDescription = '<h6 class=\"nodeDescriptionSM\">'
    tagStyleClose = '</h6>'
    
    # I want these scaling variables to range from 0 to 1.
    # So, if the browser or plot generation files generate files which are displaying
    #   too small, adjust this scaleAdjust rather than fooling around with all the other values.
    scaleAdjust = 0.3
    for x in scaleData:
      scaleData[x] += scaleAdjust
    maxScale += scaleAdjust
    minScale += scaleAdjust
        
    sectionFile = dataDict['kml']
    sectionTag = dataDict['img']
    animatedRanges = dataDict['Animated']
    
    iconPath = dataDict['IconPath']
    
    ##############################
    if section == 'SiteMask':
      KML = KMLData()
      KML.addMultipleScaledStyles(iconPath, ('%s-green' % sectionTag[section], '%s-red' % sectionTag[section], '%s-gray' % sectionTag[section]), scaleData, '.png')	
      for node in self.siteData:
        if 'Mask' not in self.siteData[node]:
          continue
        if self.siteData[node]['Mask'] == 'Allowed':
          icon = '%s-green%s' % (sectionTag[section], self.siteData[node]['Cat'])
        elif self.siteData[node]['Mask'] == 'Banned':
          icon = '%s-red%s' % (sectionTag[section], self.siteData[node]['Cat'])
        else:
          icon = '%s-gray%s' % (sectionTag[section], self.siteData[node]['Cat'])
        
        west_east = '%.4f&deg; ' % abs(self.siteData[node]['Coord'][0])
        if self.siteData[node]['Coord'][0] < 0:
          west_east += 'W'
        else:
          west_east += 'E'
        
        north_south = '%.4f&deg; ' % abs(self.siteData[node]['Coord'][1])  
        if self.siteData[node]['Coord'][1] < 0:
          north_south += 'S'
        else:
          north_south += 'N'
        
        description = 'Status: %s<br/>Location: %s, %s<br/>Category: %s' % (self.siteData[node]['Mask'], west_east, north_south, self.siteData[node]['Cat'])
        KML.addNode(tagStyleNodeName + node + tagStyleClose, tagStyleNodeName + node + tagStyleClose + tagStyleNodeDescription + description + tagStyleClose, icon, self.siteData[node]['Coord'])
        
      KML.writeFile('%s/%s' % (filePath, sectionFile[section][0]))
      gLogger.verbose('%s KML created: %s/%s' % (section, filePath, sectionFile[section][0]))
      
    ##############################    
    elif section == 'JobSummary':
      KML = KMLData()
      
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
      for i in range(dataSize):
        if numCat:
          scaleValue = (float(i) // sectionSize) + 1
          scaleValue /= numCat
        else:
          scaleValue = float(i + 1) / dataSize
        scaleDict[str(numJobs[i])] = (float(scaleValue) * (maxScale - minScale)) + minScale
        
      # Now actually generate the KML  
      for node in self.siteData:
        if 'JobSummary' not in self.siteData[node]:
          continue
        
        # Now calculate the total number of jobs (used for relative scaling)  
        total = 0
        for state in self.siteData[node]['JobSummary']:
          total += self.siteData[node]['JobSummary'][state]
          
        # Generate node style
        KML.addNodeStyle('%s-%s' % (sectionTag[section],node), '%s%s-%s.png' % (iconPath,sectionTag[section],node), scaleDict[str(total)])#1)#scaleData[self.siteData[node]['Cat']])
        # Generate name (widgets and stuff, too!)
        name = '<img src="%s%sw1-%s.png" width=200/>' % (iconPath, sectionTag[section], node)
        # Generate description
        description = 'Done: %d<br/>Running: %d<br/>Stalled: %d<br/>Waiting: %d<br/>Failed: %d' %\
                      (self.siteData[node]['JobSummary']['Done'], self.siteData[node]['JobSummary']['Running'], self.siteData[node]['JobSummary']['Stalled'],\
                      self.siteData[node]['JobSummary']['Waiting'], self.siteData[node]['JobSummary']['Failed'])
        # Add the node
        KML.addNode(tagStyleNodeName + node + tagStyleClose + name, tagStyleNodeName + node + tagStyleClose + tagStyleNodeDescription + description + tagStyleClose, '%s-%s' % (sectionTag[section],node), self.siteData[node]['Coord'])

      KML.writeFile('%s/%s' % (filePath, sectionFile[section][0]))
      gLogger.verbose('%s KML created: %s/%s' % (section, filePath, sectionFile[section][0]))
      
    ##############################
    elif section == 'PilotSummary':
      KML = KMLData()
      for node in self.siteData:
        if 'PilotSummary' not in self.siteData[node]:
          continue
          
        # Generate node style
        KML.addNodeStyle('%s-%s' % (sectionTag[section],node), '%s%s-%s.png' % (iconPath,sectionTag[section],node), scaleData[self.siteData[node]['Cat']])
        
        # Generate description
        description = ''
        for child in self.siteData[node]['PilotSummary']:
          description += child + '</br>'
          
        # Write the node
        KML.addNode(tagStyleNodeName + node + tagStyleClose, tagStyleNodeName + node + tagStyleClose + tagStyleNodeDescription + description + tagStyleClose, '%s-%s' % (sectionTag[section],node), self.siteData[node]['Coord'])

      KML.writeFile('%s/%s' % (filePath, sectionFile[section][0]))
      gLogger.verbose('%s KML created: %s/%s' % (section, filePath, sectionFile[section][0]))
      
    ##############################
    elif section == 'DataStorage':
      KML = KMLData()
      for node in self.siteData:
        if 'DataStorage' not in self.siteData[node]:
          continue
          
        KML.addNodeStyle('%s-%s' % (sectionTag[section],node), '%s%s-%s.png' % (iconPath,sectionTag[section],node), 2.0)
        
        description = '<img src="%s%s-%s-large.png"/><br/>' % (iconPath,sectionTag[section],node)
        for se in self.siteData[node]['DataStorage']:
          description += '%s: %.3f TB (%s files)<br/>' % (se, float(self.siteData[node]['DataStorage'][se]['Size']) / 1024**4, self.siteData[node]['DataStorage'][se]['Files'])
        
        KML.addNode(tagStyleNodeName + node + tagStyleClose, tagStyleNodeName + node + tagStyleClose + tagStyleNodeDescription + description + tagStyleClose, '%s-%s' % (sectionTag[section],node), self.siteData[node]['Coord'])
        
      KML.writeFile('%s/%s' % (filePath, sectionFile[section][0]))
      gLogger.verbose('%s KML created: %s/%s' % (section, filePath, sectionFile[section][0]))
        
    ##############################
    elif section == 'Animated':
    
      # We have multiple KML files to generate.
      # The reason for this is so that the data can be loaded at intervals
      # instead of all at once. This makes it look more 'animated.'
      KML = {'green' : KMLData(), 'yellow' : KMLData(), 'gray' : KMLData()}
      linkKML = KMLData()
      
      outFile = {}
      
      # Kill two birds with one stone--add node styles and generate file names
      for color in KML:
        if color != 'gray':
          KML[color].addNodeStyle('%s-%sup' % (sectionTag[section], color), '%s%s-%sup.gif' % (iconPath, sectionTag[section], color), 1.0)
          KML[color].addNodeStyle('%s-%sdown' % (sectionTag[section], color), '%s%s-%sdown.gif' % (iconPath, sectionTag[section], color), 1.0)
        KML[color].addNodeStyle('%s-%s' % (sectionTag[section], color), '%s%s-%s.gif' % (iconPath, sectionTag[section], color), 1.0)
        
        for f in sectionFile[section]:
          if f.find(color) != -1:
            outFile[color] = f
            break
            
      #DEBUG_COUNT = {'green' : 0, 'yellow' : 0, 'gray' : 0}
      
      for node in self.siteData:
        # Double-check that the data is actually here
        if node not in self.timeSeries:
          continue
        if 'TotalJobs' not in self.timeSeries[node]:
          continue
        
        # Check which range it is in
        #avg = self.timeSeries[node]['TotalJobs'].avg(False, 'Seconds')
        avg = self.timeSeries[node]['TotalJobs'].avg(False, False)
        if avg > animatedRanges['green']:
          color = 'green'
        elif avg > animatedRanges['yellow']:
          color = 'yellow'
        else:
          color = 'gray'
        
        # Calculate the trend
        if color == 'gray':
          trend = 0
        else:
          trend = self.timeSeries[node]['TotalJobs'].trend()
          
        if trend > animatedRanges['up']:
          state = 'up'
        elif trend < animatedRanges['down']:
          state = 'down'
        else:
          state = ''
          
        #DEBUG_COUNT[color] += 1
          
        #print '----------- DEBUG TAG: Animated'
        #print 'Node: %s\nAvg: %s\nTrend: %s\nLen: %s\nData: %s\n\n' % (node, avg, trend, len(self.timeSeries[node]['TotalJobs']), self.timeSeries[node]['TotalJobs'])
        
        KML[color].addNode(node, '', '%s-%s%s' % (sectionTag[section], color, state), self.siteData[node]['Coord'])
        
      #print '----------- DEBUG TAG: Animated Summary'
      #print 'Colors: %s' % DEBUG_COUNT
        
      # Write the KML file and reset it
      for color in KML:
        KML[color].writeFile('%s/%s' % (filePath, outFile[color]))
        gLogger.verbose('%s KML created (color: %s): %s/%s' % (section, color, filePath, outFile[color]))
        
      # Obviously we need to generate links to, but that hasn't been implemented yet.
      
    ##############################
    else:
      return S_ERROR('Invalid generation section %s' % section)
    
    return S_OK()       
    
  #############################################################################
  def generateIcons(self, section, filePath, dataDict):
   
    sectionFile = dataDict['kml']
    sectionTag = dataDict['img']
      
    gLogger.verbose('Icon generation request received. Section: %s' % section)
    
    ##############################
    if section == 'SiteMask':
      # We made these icons static
      gLogger.verbose('SiteMask icon generation -- nothing to do.')
    
    ##############################  
    elif section == 'JobSummary':
      # Done, Running, Stalled, Waiting, Failed, respectively
      colorList = ('#00ff00', '#ff7f00', '#0000ff', '#ffff00', '#ff0000')
      
      # Here's our handle for generating widget plots
      reportsClient = ReportsClient()
      # And this calculates the time period over which the widget plots will be shown
      timeNow = datetime.datetime.utcnow()
      plotFrom = timeNow - datetime.timedelta(days=7)
      
      # Generate icons
      for node in self.siteData:
        if 'JobSummary' not in self.siteData[node]:
          continue
        
        fileName = '%s/%s-%s.png' % (filePath,sectionTag[section],node)
        
        data = (self.siteData[node]['JobSummary']['Done'], self.siteData[node]['JobSummary']['Running'],\
                self.siteData[node]['JobSummary']['Stalled'], self.siteData[node]['JobSummary']['Waiting'],\
                self.siteData[node]['JobSummary']['Failed'])
        
        pylab.close()
        pylab.figure(figsize=(0.6,0.6))
        pylab.gcf().figurePatch.set_alpha(0)   
        pylab.pie(data, colors=colorList)
        pylab.savefig(fileName)
        gLogger.verbose('%s image created: %s' % (section, fileName))
                
        # Generate widget plot things :)
        result = reportsClient.generatePlot('Job', 'NumberOfJobs', plotFrom, timeNow, {'Site' : [node]}, 'JobType')
        if result['OK']:
          reportsClient.getPlotToDirectory(result['Value'], filePath)
          widgetPath = '%s/%sw1-%s.png' % (filePath, sectionTag[section], node)
          os.rename('%s/%s' % (filePath, result['Value']), widgetPath)
          gLogger.verbose('%s image created: %s' % (section, widgetPath))
    
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
        pylab.close()
        pylab.figure(figsize=(0.6,0.6))
        pylab.gcf().figurePatch.set_alpha(0)
        pylab.pie(data, colors=colorList)
        pylab.savefig(fileName)
        gLogger.verbose('%s image created: %s' % (section, fileName))
        
    ##############################    
    elif section == 'DataStorage':
      # Elements here are [name, nickname, color], and order matters
      masterList = [['RAW', 'RAW', '#0000ff'],\
                    ['RDST', 'RDST', '#7fff7f'],\
                    ['M-DST', 'MDST', '#007f00'],\
                    ['DST', 'DST', '#00ff00'],\
                    ['FAILOVER', 'FAIL', '#ffff00'],\
                    ['USER', 'USER', '#ffffff'],\
                    ['LOG', 'LOG', '#ff7f00'],\
                    ['DISK', 'DISK', '#ff0000'],\
                    ['TAPE', 'TAPE', '#7f0000']]
                    
      typeList = []
      labelList = []
      for element in masterList:
        typeList.append(element[0])
        labelList.append(element[1])
              
      # Produce a data dictionary of processed (percentage) data
      processedSize = self.processDataStorage(typeList)
      
      # This is to eliminate tick marks and labels in the thumbnail version
      zeroList1 = [0 for i in range(len(masterList))]
      zeroList2 = ['' for i in range(len(masterList))]
      
      # These are the parameters for aligning the tick marks / labels in the large version
      largeTickX = [(x*1.5 + 0.5 + 0.45) for x in range(len(masterList))]
      largeTickY = [(x*10) for x in range(0,11,2)]
      largeLabelY = [('%d%%' % (x*10)) for x in range(0,11,2)]
      
      for node in self.siteData:
        if 'DataStorage' not in self.siteData[node]:
          continue
          
        fileName = '%s/%s-%s.png' % (filePath,sectionTag[section],node)
        
        # First generate the small plot  
        pylab.close()
        pylab.figure(figsize=(0.6,0.6))
        pylab.gcf().figurePatch.set_alpha(0)
        pylab.gca().axesPatch.set_alpha(0.2)
          
        for i in range(len(masterList)):
          pylab.bar(i+0.25, processedSize[node][masterList[i][0]], color=masterList[i][2], alpha=0.8)
        
        pylab.xticks(zeroList1, zeroList2)
        pylab.yticks(zeroList1, zeroList2)
        
        pylab.savefig(fileName)
        gLogger.verbose('%s image created: %s' % (section, fileName))
        
        # Now generate the enlarged plot
        fileName = '%s/%s-%s-large.png' % (filePath,sectionTag[section],node)
          
        pylab.close()
        pylab.figure(figsize=(3,1.5))
        pylab.gcf().figurePatch.set_alpha(0)
        pylab.gca().axesPatch.set_alpha(0)
          
        for i in range(len(masterList)):
          pylab.bar(i*1.5 + 0.4, processedSize[node][masterList[i][0]], width=1.1, color=masterList[i][2], alpha=1)
        
        pylab.xticks(largeTickX, labelList, size='xx-small')
        pylab.yticks(largeTickY, largeLabelY, size='xx-small')
        
        pylab.savefig(fileName)
        gLogger.verbose('%s image created: %s' % (section, fileName))

    ##############################  
    elif section == 'Animated':
      # Um, don't do anything.
      gLogger.verbose('Animated icon generation -- nothing to do.')
      
    ##############################  
    else:
      return S_ERROR('Invalid icon generation section %s' % section)
      
    gLogger.verbose('Icon generation complete.')
            
    return S_OK('%s icons generated in %s.' % (section, filePath))
    
  #############################################################################
  def processDataStorage(self, typeList):
    """ Process data storage sizes in order to produce
        percentages which are relative within a given
        storage element type. typeList should be a list of
        storage types to process.
    """
    data = {}
    for node in self.siteData:
      if 'DataStorage' not in self.siteData[node]:
        continue
        
      data[node] = {}
      
      # Extract sizes for each type of storage device in each SE
      for se in typeList:
        if se not in self.siteData[node]['DataStorage']:
          data[node][se] = 0
        else:
          data[node][se] = self.siteData[node]['DataStorage'][se]['Size']
          
    # Now that we have the sizes, we can go through each type and calculate maximum sizes
    for se in typeList:
      maxSize = 0
      # Compute maximum
      for node in data:
        if data[node][se] > maxSize and node != 'LCG.CERN.ch':
          maxSize = data[node][se]
      # Just in case...
      if maxSize == 0:
        maxSize = 1
      # Store value as an integer percentage
      for node in data:
        if node == 'LCG.CERN.ch':
          data[node][se] = 100
        else:
          data[node][se] = int(float(data[node][se] * 100) / maxSize)
                  
    return data
        
  #EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
