########################################################################
# $Id: Mapping.py,v 1.14 2008/10/08 12:33:19 rgracian Exp $
########################################################################

""" All of the data collection and handling procedures for the SiteMappingHandler
"""

from DIRAC.Core.Utilities.KMLData import KMLData
from DIRAC import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.AccountingSystem.Client.ReportsClient import ReportsClient
from DIRAC.Core.Utilities.SiteCEMapping import getCESiteMapping,getSiteCEMapping,getSiteForCE
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
  def resetData(self):
    self.siteData = {}
    #self.timeSeries = {}  # We don't want to reset this; otherwise, we may not generate a full data set
    return S_OK()

  ###########################################################################
  def updateData(self, section, dataDict):

    gLogger.verbose('Update request received. Section: %s' % section)

    wmsAdmin = RPCClient('WorkloadManagement/WMSAdministrator',useCertificates=True,timeout=120)
    jobMon = RPCClient('WorkloadManagement/JobMonitoring',useCertificates=True,timeout=120)
    storUse = RPCClient('DataManagement/StorageUsage',useCertificates=True,timeout=120)

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

            # Well, it's a hard call, but I suppose it would be better to continually add data
            #   to the dictionary rather than overwrite it every time we do a site update.
            # If we reset it, we are guaranteed to eliminate obsolete data.
            # However, if we keep old data, then we will better be able to allow section to reference
            #   each other (e.g., allow SiteMask to use JobSummary data to scale its icons).
            # The worst that happens is that you end up with a site on the map that should have been removed.
            #   In that case, just restart the agent.

            if site not in self.siteData:
              self.siteData[site] = {'Coord' : (float(coord[0]), float(coord[1])), 'Cat' : cat, 'Mask' : 'Unknown'}

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
      ceSiteMapping = getCESiteMapping()
      if not ceSiteMapping['OK']:
        return S_ERROR('Could not get CE site mapping')
      children = ceSiteMapping['Value'].keys()

      # Iterate through every pilot
      for child in pilotSummary['Value']:

        # If the pilot is not in the Resources/GridSites/LCG list, then
        # we won't be able to detect its parent, making it orphaned (we don't want that)
        if child not in children:
          continue

        # Ah, but it does have a parent!
        # Make sure it is one we recognize
        #parent = gConfig.getValue('Resources/Sites/LCG/' + child)
        parent = getSiteForCE(child)
        if not parent['OK']:
          continue
        parent = parent['Value']
        if parent not in self.siteData.keys():
          continue

        # Yes, it is. Add it to the site database
        if 'PilotSummary' not in self.siteData[parent]:
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

    # Obsolete

    #elif section == 'Animated':
    #  # Um, don't do anything.
    #  gLogger.verbose('Animated section update -- nothing to do.')

    ##############################
    else:
      return S_ERROR('Invalid update section %s' % section)

    return S_OK(self.siteData)

  ###########################################################################
  def generateKML(self, section, filePath, dataDict):

    gLogger.verbose('KML generation request received. Section: %s' % section)

    # At the moment, KMLData() is configured to only output scaling factors to one decimal place.
    #   If you need more precision, make the appropriate changes in KMLData.py
    scaleData = {'SiteMask' : {'max' : 0.5, 'min' : 0.2},\
                 'JobSummary' : {'max' : 3.0, 'min' : 1.7},\
                 'PilotSummary' : {'T0' : 4.0, 'T1' : 4.0, 'T2' : 4.0},\
                 'DataStorage' : 2.0}
    #             'Animated' : 0.8}

    # These are block-level tags for styling names/descriptions
    # They references class in infostyles.css
    #tagStyleNodeName = '<h6 class=\"nodeNameSM\">'
    #tagStyleNodeDescription = '<h6 class=\"nodeDescriptionSM\">'
    #tagStyleNodeHeading = '<h6 class=\"nodeHeadingSM\">'
    tagNodeName = lambda x:('<h6 class="nodeNameSM">%s</h6>' % x)
    tagNodeDetails = lambda x:('<h6 class="nodeDescriptionSM">%s</h6>' % x)
    tagNodeHeading = lambda x:('<h6 class="nodeHeadingSM">%s</h6>' % x)
    tagLink = lambda x:('<h6 class="nodeLinkSM" onclick="siteControl(\'%s\')">More Information</h6>' % x)
    tagColor = lambda x, c:('<div style="margin: 0px; padding: 0px; color: %s">%s</div>' % (c, x))

    makeDescription = lambda name, heading, details:(tagNodeName(name) + tagNodeHeading(heading) + tagNodeDetails(details) + tagLink(name))
    #tagColorGreen = '<div style=\"margin: 0px; padding: 0px; color: #00cf00\">'
    #tagColorRed = '<div style=\"margin: 0px; padding: 0px; color: #cf0000\">'
    #tagColorClose = '</div>'
    #tagStyleClose = '</h6>'

    sectionFile = dataDict['kml']
    sectionTag = dataDict['img']
    animatedRanges = dataDict['Animated']

    iconPath = dataDict['IconPath']

    ##############################
    if section == 'SiteMask':
      KML = KMLData()

      #KML.addMultipleScaledStyles(iconPath, ('%s-green' % sectionTag[section], '%s-red' % sectionTag[section], '%s-gray' % sectionTag[section]), scaleData[section], '.png')

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

      # Prepare the scalars
      maxScale = scaleData[section]['max']
      minScale = scaleData[section]['min']

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
      scaleDict = {'0' : minScale} # This prevents key errors in the case that there is no JobSummary data
      for i in range(dataSize):
        if numCat:
          scaleValue = (float(i) // sectionSize) + 1
          scaleValue /= numCat
        else:
          scaleValue = float(i + 1) / dataSize
        scaleDict[str(numJobs[i])] = (float(scaleValue) * (maxScale - minScale)) + minScale

      # Now actually generate the KML
      for node in self.siteData:
        if 'Mask' not in self.siteData[node]:
          continue

        # Now calculate the total number of jobs (used for relative scaling)
        total = 0
        if 'JobSummary' in self.siteData[node]:
          for state in self.siteData[node]['JobSummary']:
            total += self.siteData[node]['JobSummary'][state]

        # An example format for a file name, with all modifiers, would be:
        #   SM-green_halo_pulse_white.gif

        #print 'Node: %s' % node

        # Calculate the site mask part of things
        if self.siteData[node]['Mask'] == 'Allowed':
          icon = 'green'
        elif self.siteData[node]['Mask'] == 'Banned':
          icon = 'red'
        else:
          icon = 'gray'

        modifier = ''
        imgType = 'png'

        # Add a halo modifier for T0/T1 nodes
        if self.siteData[node]['Cat'] == 'T0':
          modifier += '_halo'
        elif self.siteData[node]['Cat'] == 'T1':
          modifier += '_halo'

        # Calculate any changes in job trends--use an animation graphically
        if node in self.timeSeries:
          if 'TotalJobs' in self.timeSeries[node]:
            # Calculate the trend
            trend = self.timeSeries[node]['TotalJobs'].trend()
            #print 'Data: %s' % self.timeSeries[node]['TotalJobs']
            #print 'Trend: %s' % trend
            if trend > animatedRanges['up']:
              modifier += '_pulse_white'
              imgType = 'gif'
            elif trend < animatedRanges['down']:
              modifier += '_pulse_black'
              imgType = 'gif'

        #print 'File: %s-%s%s.%s' % (sectionTag[section], icon, modifier, imgType)
        #print '\n'

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

        # Generate node style
        KML.addNodeStyle('%s-%s' % (sectionTag[section],node), '%s%s-%s%s.%s' % (iconPath,sectionTag[section],icon, modifier, imgType), scaleDict[str(total)])
        #KML.addNodeStyle('%s-%s' % (sectionTag[section],node), '%s%s-%s.png' % (iconPath,sectionTag[section],node), scaleDict[str(total)], (0.04, 0.5), (120, 10))
        # Generate description
        details = 'Status: %s<br/>Location: %s, %s<br/>Category: %s' % (self.siteData[node]['Mask'], west_east, north_south, self.siteData[node]['Cat'])
        description = makeDescription(node, 'Site Info', details)
        # Add the node
        KML.addNode(node, description, '%s-%s' % (sectionTag[section],node), self.siteData[node]['Coord'])
        #KML.addNode(tagStyleNodeName + node + tagStyleClose, tagStyleNodeName + node + tagStyleClose + tagStyleNodeHeading + 'Site Info' + tagStyleClose + tagStyleNodeDescription + description + tagStyleClose, '%s-%s' % (sectionTag[section],node), self.siteData[node]['Coord'])
        #KML.addNode(tagStyleNodeName + node + tagStyleClose, tagStyleNodeName + node + tagStyleClose + tagStyleNodeHeading + 'Site Info' + tagStyleClose + tagStyleNodeDescription + description + tagStyleClose, icon, self.siteData[node]['Coord'])

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

      # Prepare the scalars
      maxScale = scaleData[section]['max']
      minScale = scaleData[section]['min']

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
      scaleDict = {'0' : minScale} # This prevents key errors in the case that there is no JobSummary data
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
        KML.addNodeStyle('%s-%s' % (sectionTag[section],node), '%s%s-%s.png' % (iconPath,sectionTag[section],node), scaleDict[str(total)], (0.125, 0.5), (40, 10))
        # Generate name (widgets and stuff, too!)
        #name = '<h6 style="margin-left: -30px; margin-top: -10px; padding: 0px;"><img src="%s%sw1-%s.png"/></h6>' % (iconPath, sectionTag[section], node)
        # Generate description
        details = 'Done: %d<br/>Running: %d<br/>Stalled: %d<br/>Waiting: %d<br/>Failed: %d' %\
                      (self.siteData[node]['JobSummary']['Done'], self.siteData[node]['JobSummary']['Running'], self.siteData[node]['JobSummary']['Stalled'],\
                      self.siteData[node]['JobSummary']['Waiting'], self.siteData[node]['JobSummary']['Failed'])
        description = makeDescription(node, 'Number of Jobs', details)
        # Add the node
        #KML.addNode(tagStyleNodeName + node + tagStyleClose + name, tagStyleNodeName + node + tagStyleClose + tagStyleNodeHeading + 'Number of Jobs' + tagStyleClose + tagStyleNodeDescription + description + tagStyleClose, '%s-%s' % (sectionTag[section],node), self.siteData[node]['Coord'])
        KML.addNode(node,  description, '%s-%s' % (sectionTag[section],node), self.siteData[node]['Coord'])

      KML.writeFile('%s/%s' % (filePath, sectionFile[section][0]))
      gLogger.verbose('%s KML created: %s/%s' % (section, filePath, sectionFile[section][0]))

    ##############################
    elif section == 'PilotSummary':
      KML = KMLData()
      for node in self.siteData:
        if 'PilotSummary' not in self.siteData[node]:
          continue

        # Generate node style
        KML.addNodeStyle('%s-%s' % (sectionTag[section],node), '%s%s-%s.png' % (iconPath,sectionTag[section],node), scaleData[section][self.siteData[node]['Cat']], (0.125, 0.5), (40, 10))

        # Generate description
        details = ''
        for child in self.siteData[node]['PilotSummary']:
          doneCleared = self.siteData[node]['PilotSummary'][child]['Done'] + self.siteData[node]['PilotSummary'][child]['Cleared']
          aborted = self.siteData[node]['PilotSummary'][child]['Aborted']
          total = aborted + doneCleared
          if not total:
            percentGood = 0
          else:
            percentGood = (float(doneCleared) / total) * 100
          if percentGood > dataDict['CEGreenPercent']:
            color = '#00cf00'
          else:
            color = '#cf0000'
          #description += colorTag + child + tagColorClose
          details += tagColor(child, color)

        description = makeDescription(node, 'Computing Elements', details)

        # Write the node
        KML.addNode(node, description, '%s-%s' % (sectionTag[section],node), self.siteData[node]['Coord'])
        #KML.addNode(tagStyleNodeName + node + tagStyleClose, tagStyleNodeName + node + tagStyleClose + tagStyleNodeHeading + 'Computing Elements' + tagStyleClose + tagStyleNodeDescription + description + tagStyleClose, '%s-%s' % (sectionTag[section],node), self.siteData[node]['Coord'])

      KML.writeFile('%s/%s' % (filePath, sectionFile[section][0]))
      gLogger.verbose('%s KML created: %s/%s' % (section, filePath, sectionFile[section][0]))

    ##############################
    elif section == 'DataStorage':
      KML = KMLData()
      for node in self.siteData:
        if 'DataStorage' not in self.siteData[node]:
          continue

        KML.addNodeStyle('%s-%s' % (sectionTag[section],node), '%s%s-%s.png' % (iconPath,sectionTag[section],node), scaleData[section])

        #details = '<img src="%s%s-%s-large.png"/><br/>' % (iconPath,sectionTag[section],node)
        details = ''
        for se in self.siteData[node]['DataStorage']:
          if self.siteData[node]['DataStorage'][se]['Files'] > 1:
            plural = 's'
          else:
            plural = ''
          details += '%s: %.3f TB (%s file%s)<br/>' % (se, float(self.siteData[node]['DataStorage'][se]['Size']) / 1024**4, self.siteData[node]['DataStorage'][se]['Files'], plural)

        description = makeDescription(node, 'Disk Usage', details)

        KML.addNode(node, description, '%s-%s' % (sectionTag[section],node), self.siteData[node]['Coord'])
        #KML.addNode(tagStyleNodeName + node + tagStyleClose, tagStyleNodeName + node + tagStyleClose + tagStyleNodeHeading + 'Disk Usage' + tagStyleClose + tagStyleNodeDescription + description + tagStyleClose, '%s-%s' % (sectionTag[section],node), self.siteData[node]['Coord'])

      KML.writeFile('%s/%s' % (filePath, sectionFile[section][0]))
      gLogger.verbose('%s KML created: %s/%s' % (section, filePath, sectionFile[section][0]))

    ##############################
#    elif section == 'Animated':
#
#      # We have multiple KML files to generate.
#      # The reason for this is so that the data can be loaded at intervals
#      # instead of all at once. This makes it look more 'animated.'
#      KML = {'green' : KMLData(), 'yellow' : KMLData(), 'gray' : KMLData()}
#      linkKML = KMLData()
#
#      outFile = {}
#
#      variations = ['', '_banned']
#
#      # Kill two birds with one stone--add node styles and generate file names
#      for color in KML:
#        if color != 'gray':
#          # If it's not gray, generate all variations for up/down
#          for v in variations:
#            KML[color].addNodeStyle('%s-%sup%s' % (sectionTag[section], color, v), '%s%s-%sup%s.gif' % (iconPath, sectionTag[section], color, v), scaleData[section])
#            KML[color].addNodeStyle('%s-%sdown%s' % (sectionTag[section], color, v), '%s%s-%sdown%s.gif' % (iconPath, sectionTag[section], color, v), scaleData[section])
#        # Everyone needs the fundamental variations, though
#        for v in variations:
#          KML[color].addNodeStyle('%s-%s%s' % (sectionTag[section], color, v), '%s%s-%s%s.gif' % (iconPath, sectionTag[section], color, v), scaleData[section])
#
#        for f in sectionFile[section]:
#          if f.find(color) != -1:
#            outFile[color] = f
#            break
#
#      #DEBUG_COUNT = {'green' : 0, 'yellow' : 0, 'gray' : 0}
#
#      for node in self.siteData:
#        # Double-check that the data is actually here
#        if node not in self.timeSeries:
#          continue
#        if 'TotalJobs' not in self.timeSeries[node]:
#          continue
#
#        # Check which range it is in
#        #avg = self.timeSeries[node]['TotalJobs'].avg(False, 'Seconds')
#        avg = self.timeSeries[node]['TotalJobs'].avg(False, False)
#        if avg > animatedRanges['green']:
#          color = 'green'
#        elif avg > animatedRanges['yellow']:
#          color = 'yellow'
#        else:
#          color = 'gray'
#
#        # Calculate the trend
#        if color == 'gray':
#          trend = 0
#        else:
#          trend = self.timeSeries[node]['TotalJobs'].trend()
#
#        if trend > animatedRanges['up']:
#          state = 'up'
#        elif trend < animatedRanges['down']:
#          state = 'down'
#        else:
#          state = ''
#
#        if self.siteData[node]['Mask'] == 'Allowed':
#          variation = ''
#        else:
#          variation = '_banned'
#
#        #DEBUG_COUNT[color] += 1
#
#        #print '----------- DEBUG TAG: Animated'
#        #print 'Node: %s\nAvg: %s\nTrend: %s\nLen: %s\nData: %s\n\n' % (node, avg, trend, len(self.timeSeries[node]['TotalJobs']), self.timeSeries[node]['TotalJobs'])
#
#        KML[color].addNode(node, '', '%s-%s%s%s' % (sectionTag[section], color, state, variation), self.siteData[node]['Coord'])
#
#      #print '----------- DEBUG TAG: Animated Summary'
#      #print 'Colors: %s' % DEBUG_COUNT
#
#      # Write the KML file and reset it
#      for color in KML:
#        KML[color].writeFile('%s/%s' % (filePath, outFile[color]))
#        gLogger.verbose('%s KML created (color: %s): %s/%s' % (section, color, filePath, outFile[color]))
#
#      # Obviously we need to generate links to, but that hasn't been implemented yet.

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
      #for node in self.siteData:
      #  if 'Mask' not in self.siteData[node]:
      #    continue

      #  if self.siteData[node]['Mask'] == 'Allowed':
      #    icon = 'green'
      #  elif self.siteData[node]['Mask'] == 'Banned':
      #    icon = 'red'
      #  else:
      #    icon = 'gray'

      #  fileName = '%s/%s-%s.png' % (filePath, sectionTag[section], node)

      #  width = 6.0
      #  height = 0.5
      #  pie_size = 1.0
      #  pylab.figure(figsize=(width,height))
      #  pylab.gcf().figurePatch.set_alpha(0)
      #  pylab.figtext(height/width, height/2, node, fontsize=36, color='#ffffff', weight='ultrabold')
      #  pylab.axes([0, 0, height/width*pie_size, 1.0*pie_size])
      #  pylab.imshow(pylab.imread('%s/%s-%s.png' % (filePath, sectionTag[section], icon)))
      #  pylab.axis('off')
      #  pylab.savefig(fileName)
      #  gLogger.verbose('%s image created: %s' % (section, fileName))

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
        if data == (0, 0, 0, 0, 0):
          # If there is absolutely no data, let's just say it failed
          data = (0, 0, 0, 0, 1)

        pylab.close()
        #pylab.figure(figsize=(0.6,0.6))
        #pylab.gcf().figurePatch.set_alpha(0)
        width = 2.0
        height = 0.5
        pie_size = 1.0
        pylab.figure(figsize=(width,height))
        pylab.gcf().figurePatch.set_alpha(0)
        pylab.figtext(height/width, height/2, node, fontsize=12, color='#ffffff', weight='ultrabold')
        pylab.axes([0, 0, height/width*pie_size, 1.0*pie_size])
        pylab.pie(data, colors=colorList)
        pylab.savefig(fileName)
        gLogger.verbose('%s image created: %s' % (section, fileName))

        # Generate widget plot things :)
        #result = reportsClient.generatePlot('Job', 'NumberOfJobs', plotFrom, timeNow, {'Site' : [node]}, 'JobType', {'thumbnail' : True, 'width' : 250, 'height' : 250, 'thb_width' : 200, 'thb_height' : 200})
        #if result['OK']:
        #  reportsClient.getPlotToDirectory(result['Value'], filePath)
        #  widgetPath = '%s/%sw1-%s.png' % (filePath, sectionTag[section], node)
        #  os.rename('%s/%s' % (filePath, result['Value']), widgetPath)
        #  gLogger.verbose('%s image created: %s' % (section, widgetPath))

    ##############################
    elif section == 'PilotSummary':
      # Done + Cleared, Aborted
      colorList = ('#00ff00', '#ff0000')

      for node in self.siteData:
        if 'PilotSummary' not in self.siteData[node]:
          continue

        totalDoneCleared = 0
        totalAborted = 0
        for child in self.siteData[node]['PilotSummary']:
          doneCleared = self.siteData[node]['PilotSummary'][child]['Done'] + self.siteData[node]['PilotSummary'][child]['Cleared']
          aborted = self.siteData[node]['PilotSummary'][child]['Aborted']
          totalDoneCleared += doneCleared
          totalAborted += aborted

          # This code creates small pie charts for each CE
          #fileName = '%s/%s-%s-ce%s.png' % (filePath, sectionTag[section], node, child)
          #data = (doneCleared, aborted)
          #pylab.close()
          #pylab.figure(figsize=(0.2,0.2))
          #pylab.gcf().figurePatch.set_alpha(0)
          #pylab.pie(data, colors=colorList)
          #pylab.savefig(fileName)
          #gLogger.verbose('%s image created: %s' % (section, fileName))

        width = 2.0
        height = 0.5

        # Generate plot icon for node
        fileName = '%s/%s-%s.png' % (filePath,sectionTag[section],node)
        data = (totalDoneCleared, totalAborted)
        if data == (0, 0):
          # If there is absolutely no data, let's just say it failed
          data = (0, 1)
        pylab.close()
        pylab.figure(figsize=(width,height))
        pylab.gcf().figurePatch.set_alpha(0)
        pylab.figtext(height/width, height/2, node, fontsize=8, color='#ffffff', weight='bold')
        pylab.axes([0, 0, height/width, 1.0])
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
#    elif section == 'Animated':
#      # Um, don't do anything.
#      gLogger.verbose('Animated icon generation -- nothing to do.')

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
