# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/MonitoringSystem/Service/SiteMapHandler.py,v 1.1 2009/09/10 15:30:34 acasajus Exp $
__RCSID__ = "$Id: SiteMapHandler.py,v 1.1 2009/09/10 15:30:34 acasajus Exp $"
import types
import os
from DIRAC import gLogger, gConfig, rootPath, S_OK, S_ERROR
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.Core.Utilities.SiteCEMapping import getCESiteMapping
from DIRAC.Core.Utilities.SiteSEMapping import getSESiteMapping
from DIRAC.ConfigurationSystem.Client import PathFinder
from DIRAC.Core.Utilities.ThreadScheduler import gThreadScheduler

gSiteData = False

def initializeSiteMapHandler( serviceInfo ):
  global gSiteData
  gSiteData = SiteMapData()
  gThreadScheduler.addPeriodicTask( 300, gSiteData.refresh )
  return S_OK()

class SiteMapHandler( RequestHandler ):

  types_getSiteMask = []
  def export_getSiteMask( self ):
    """
    Get the site mask
    """
    return S_OK( gSiteData.getSiteMaskStatus() )
  
  types_getSitesData = []
  def export_getSitesData( self ):
    """
    Get all the sites data
    """
    return S_OK( gSiteData.getSitesData() )
    
    
class SiteMapData:
  
  def __init__( self, csSection = False ):
    if csSection:
      self.csSection = csSection
    else:
      self.csSection = PathFinder.getServiceSection( "Monitoring/SiteMap" )
    self.gridsToMap = []
    self.allSites = {}
    self.sitesData = {}
    self.refresh()
    
  def getSiteMaskStatus( self ):
    return self.siteMaskStatus
  
  def getSitesData( self ):
    return self.sitesData
    
  def refresh(self):
    self._updateSiteList()
    self._updateSiteMask()
    self._updateJobSummary()
    self._updatePilotSummary()
    self._updateDataStorage()
    
  def _getCSValue( self, option, defVal = None ):
    return gConfig.getValue( "%s/%s" % ( self.csSection, option ), defVal )
    
  def _updateSiteList( self ):
    self.allSites = {}
    ceSection = "/Resources/Sites"
    self.gridsToMap = self._getCSValue( "GridsToMap", [ "LCG" ] )
    for grid in self.gridsToMap:
      gridSection = "%s/%s" % ( ceSection, grid )
      result = gConfig.getSections( gridSection )
      if not result[ 'OK' ]:
        gLogger.error( "Cannot get a list of sites for grid", "%s :%s" % ( grid, result[ 'Message' ] ) )
        continue
      for site in result[ 'Value' ]:
        coords = gConfig.getValue( "%s/%s/Coordinates" % ( gridSection, site ), "" )
        coords = coords.split( ":" )
        if not coords or len( coords ) != 2:
          gLogger.warn( "Site %s has coordinates incorrectly defined: %s" % ( site, coords ) )
          continue
        name = gConfig.getValue( "%s/%s/Name" % ( gridSection, site ), "" )
        if not name:
          gLogger.warn( "Site %s no name defined" % site )
          continue
        tier = gConfig.getValue( "%s/%s/MoUTierLevel" % ( gridSection, site ), "" )
        if not tier or tier.lower() == "none":
          tier = 2
        siteData = { 'longlat' : coords,
                     'name' : name,
                     'tier' : tier }
        self.allSites[ site ] = siteData
        self.sitesData[ site ] = dict( siteData )
        
  def _updateSiteMask( self ):
    result = RPCClient( "WorkloadManagement/WMSAdministrator" ).getSiteMask()
    if not result[ 'OK' ]:
      gLogger.error( "Cannot get the site mask", result['Message'] )
      return result
    self.siteMask = result[ 'Value' ]
    self.siteMaskStatus = dict( self.allSites )
    for site in self.siteMaskStatus:
      if site in self.siteMask:
        self.siteMaskStatus[ site ][ 'siteMaskStatus' ] = 'Allowed'
      else:
        self.siteMaskStatus[ site ][ 'siteMaskStatus' ] = 'Banned'
      self.sitesData[ site ][ 'siteMaskStatus' ] = self.siteMaskStatus[ site ][ 'siteMaskStatus' ]
    return self.siteMaskStatus
        
  def _updateJobSummary( self ):
    result = RPCClient( 'WorkloadManagement/JobMonitoring' ).getSiteSummary()
    if not result[ 'OK' ]:
      gLogger.error( "Cannot get the site job summary", result['Message'] )
      return result
    summaryData = result[ 'Value' ]
    self.jobSummary = {}
    for site in self.allSites:
      if site in summaryData:
        self.jobSummary[ site ] = summaryData[ site ]
    for site in self.sitesData:
      if site in self.jobSummary:
        self.sitesData[ site ][ 'jobSummary' ] = self.jobSummary[ site ]
    return S_OK( self.jobSummary )
        
  def _updatePilotSummary( self ):
    result = RPCClient( "WorkloadManagement/WMSAdministrator" ).getPilotSummary()
    if not result[ 'OK' ]:
      gLogger.error( "Cannot get the pilot summary", result['Message'] )
      return result
    summaryData = result[ 'Value' ]
    result = getCESiteMapping()
    if not result['OK']:
      return S_ERROR( 'Could not get CE site mapping' )
    ceMapping = result[ 'Value' ]
    self.pilotSummary = {}
    for ce in summaryData:
      if ce not in ceMapping:
        continue
      ceData = summaryData[ ce ]
      siteName = ceMapping[ ce ]
      if siteName not in self.pilotSummary:
        self.pilotSummary[ siteName ] = {}
      for status in ceData:
        if status not in self.pilotSummary[ siteName ]:
          self.pilotSummary[ siteName ][ status ] = 0
        self.pilotSummary[ siteName ][ status ] += ceData[ status ]
    for site in self.sitesData:
      if site in self.pilotSummary:
        self.sitesData[ site ][ 'pilotSummary' ] = self.pilotSummary[ site ]
    return S_OK( self.pilotSummary )
    
  def _updateDataStorage( self ):
    result = RPCClient('DataManagement/StorageUsage' ).getStorageSummary()
    if not result[ 'OK' ]:
      gLogger.error( "Cannot get the data storage summary", result['Message'] )
      return result
    storageSummary = result[ 'Value' ]
    result = getSESiteMapping()
    if not result['OK']:
      return S_ERROR( 'Could not get SE site mapping' )
    seMapping = result[ 'Value' ]
    self.storageUsage = {}
    for element in storageSummary:
      if element not in seMapping:
        continue
      for siteName in seMapping[ element ]:
        valid = True
        for grid in self.gridsToMap:
          if siteName.find( grid ) != 0:
            valid = False
            break
        if not valid:
          continue
        if siteName not in self.storageUsage:
          self.storageUsage[ siteName ] = {}
        if element.find( "_" ) != -1:
          splitter = "_"
        else:
          splitter = "-"
        storageElements = element.split( splitter )[1:]
        if siteName not in self.storageUsage:
          self.storageUsage[ siteName ] = {}
        for storageType in storageElements:
          storageType = storageType.upper()
          for attKey in storageSummary[ element ]:
            siteData = self.storageUsage[ siteName ]
            if attKey not in siteData:
              siteData[ attKey ] = {}
            attData = siteData[ attKey ]
            if storageType not in attData:
              attData[ storageType ] = 0
            attData[ storageType ] += storageSummary[ element ][ attKey ]
    for site in self.sitesData:
      if site in self.storageUsage:
        self.sitesData[ site ][ 'storageSummary' ] = self.storageUsage[ site ]
    return S_OK( self.storageUsage )