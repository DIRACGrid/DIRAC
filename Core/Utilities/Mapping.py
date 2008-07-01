########################################################################
# $Id: Mapping.py,v 1.2 2008/07/01 10:17:55 asypniew Exp $
########################################################################

""" All of the data collection and handling procedures for the SiteMappingHandler
"""

from DIRAC.Core.Utilities.KMLData import KMLData
from DIRAC.Core.Utilities.MappingTable import MappingTable
from DIRAC import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.WorkloadManagementSystem.DB.JobDB import JobDB

from DIRAC.MonitoringSystem.Agent.pychart import *

jobDB = JobDB()

tier0 = ['LCG.CERN.ch']
tier1 = ['LCG.GRIDKA.de', 'LCG.CNAF.it', 'LCG.PIC.es', 'LCG.RAL.uk', 'LCG.IN2P3.fr', 'LCG.NIKHEF.nl']

class Mapping:

  ###########################################################################
  def __init__(self):
    self.siteData = {}
    
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
      mask = self.getSiteMask()
      #mask = wmsAdmin.getSiteMask()
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
  def generateKML(self, section, filePath, fileCache, sectionDict):
  
    gLogger.verbose('KML generation request received. Section: %s' % section)
     
    scaleData = {'T0' : 1, 'T1' : 0.9, 'T2' : 0.6}
    KML = KMLData()
    #fileName = ''
    
    sectionFile = sectionDict['kml']
    sectionTag = sectionDict['png']
    
    ##############################
    if section == 'SiteMask':
      #fileName = 'sitemask.kml'
      KML.addMultipleScaledStyles(('%s-green' % sectionTag[section], '%s-red' % sectionTag[section], '%s-gray' % sectionTag[section]), scaleData, '.png')	
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
      for node in self.siteData:
        if 'JobSummary' not in self.siteData[node]:
          continue
        # Generate node style
        KML.addNodeStyle('%s-%s' % (sectionTag[section],node), '%s-%s.png' % (sectionTag[section],node), scaleData[self.siteData[node]['Cat']])
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
        KML.addNodeStyle('%s-%s' % (sectionTag[section],node), '%s-%s.png' % (sectionTag[section],node), scaleData[self.siteData[node]['Cat']])
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
  def generateIcons(self, section, filePath, fileCache, sectionDict):
  
    sectionFile = sectionDict['kml']
    sectionTag = sectionDict['png']
  
    gLogger.verbose('Icon generation request received. Section: %s' % section)
      
    plotSize = (60,60)
    plotRadius = 30
        
    # Initialize PyChart plotting methods
    theme.output_format = 'png'
    theme.use_color = True
    theme.reinitialize()
    greenFill = fill_style.Plain(bgcolor=color.T(r=0,g=1,b=0))
    redFill = fill_style.Plain(bgcolor=color.T(r=1,g=0,b=0))
    orangeFill = fill_style.Plain(bgcolor=color.T(r=1,g=0.5,b=0))
    yellowFill = fill_style.Plain(bgcolor=color.T(r=1,g=1,b=0))
    blueFill = fill_style.Plain(bgcolor=color.T(r=0,g=0,b=1))
    grayFill = fill_style.Plain(bgcolor=color.T(r=0.7,g=0.7,b=0.7))
    
    ##############################
    if section == 'SiteMask':
      maskDict = {'green' : greenFill, 'red' : redFill, 'gray' : grayFill}
      for mask in maskDict:
        fileName = '%s/%s-%s.png' % (filePath, sectionTag[section], mask)
        imgFile = open(fileName, 'wb')
        can = canvas.init(imgFile)
        
        data = [('', 100)]
        
        ar = area.T(size=plotSize,legend=None,x_grid_style=None,y_grid_style=None)
        ar.add_plot(pie_plot.T(fill_styles=[maskDict[mask]],radius=plotRadius,arc_offsets=[0],data=data,label_offset=25))
        ar.draw(can)
        can.close()
        imgFile.close()
        fileCache.addToCache(fileName)
    
    ##############################  
    elif section == 'JobSummary':
      # Done, Running, Stalled, Waiting, Failed, respectively
      plotFills = [greenFill, orangeFill, blueFill, yellowFill, redFill]
      
      for node in self.siteData:
        if 'JobSummary' not in self.siteData[node]:
          continue
          
        total = 0
        for state in self.siteData[node]['JobSummary']:
          total += self.siteData[node]['JobSummary'][state]
          
        # I don't really know how this could happen, but why risk it?
        if not total:
          continue
          
        percent = {}
        for state in self.siteData[node]['JobSummary']:
          percent[state] = int((100 * self.siteData[node]['JobSummary'][state]) // total)
          
        fileName = '%s/%s-%s.png' % (filePath,sectionTag[section],node)
        imgFile = open(fileName, 'wb')
        can = canvas.init(imgFile)
        
        data = [('', percent['Done']), ('', percent['Running']),\
                ('', percent['Stalled']), ('', percent['Waiting']), ('', percent['Failed'])]
              
        ar = area.T(size=plotSize,legend=None,x_grid_style=None,y_grid_style=None)
        ar.add_plot(pie_plot.T(fill_styles=plotFills,radius=plotRadius,arc_offsets=[0,0,0,0,0],data=data,label_offset=25))
        ar.draw(can)
        can.close()
        imgFile.close()
        fileCache.addToCache(fileName)
    
    ##############################    
    elif section == 'PilotSummary':
      # Done + Cleared, Aborted
      plotFills = [greenFill, redFill]
      
      for node in self.siteData:
        if 'PilotSummary' not in self.siteData[node]:
          continue
          
        doneCleared = 0
        aborted = 0
        for child in self.siteData[node]['PilotSummary']:
          doneCleared += self.siteData[node]['PilotSummary'][child]['Done'] + self.siteData[node]['PilotSummary'][child]['Cleared']
          aborted += self.siteData[node]['PilotSummary'][child]['Aborted']
        total = doneCleared + aborted
        
        # I don't really know how this could happen, but why risk it?
        if not total:
          continue
  
        percent = {'DoneCleared' : int((100 * doneCleared) // total),\
                   'Aborted' : int((100 * aborted) // total)}
                   
        # Generate plot icon for node
        fileName = '%s/%s-%s.png' % (filePath,sectionTag[section],node)
        imgFile = open(fileName, 'wb')
        can = canvas.init(imgFile)
        
        data = [('', percent['DoneCleared']), ('', percent['Aborted'])]
              
        ar = area.T(size=plotSize,legend=None,x_grid_style=None,y_grid_style=None)
        ar.add_plot(pie_plot.T(fill_styles=plotFills,radius=plotRadius,arc_offsets=[0,0],data=data,label_offset=25))
        ar.draw(can)
        can.close()
        imgFile.close()
        fileCache.addToCache(fileName)
      
    ##############################  
    else:
      return S_ERROR('Invalid icon generation section %s' % section)
      
    gLogger.verbose('Icon generation complete.')
            
    return S_OK('%s icons generated in %s.' % (section, filePath))
    
  #############################################################################
  def getSiteMask(self):
    """ Return the site mask
    """
    return jobDB.getSiteMask('Active')


  #EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
