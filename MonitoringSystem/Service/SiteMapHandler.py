# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/MonitoringSystem/Service/SiteMapHandler.py,v 1.3 2009/09/14 14:47:50 acasajus Exp $
__RCSID__ = "$Id: SiteMapHandler.py,v 1.3 2009/09/14 14:47:50 acasajus Exp $"
import types
import os
import threading
import time
from DIRAC import gLogger, gConfig, rootPath, S_OK, S_ERROR
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.Core.Utilities.SiteCEMapping import getCESiteMapping
from DIRAC.Core.Utilities.SiteSEMapping import getSESiteMapping
from DIRAC.ConfigurationSystem.Client import PathFinder

gSiteData = False

def initializeSiteMapHandler( serviceInfo ):
  global gSiteData
  gSiteData = SiteMapData()
  gSiteData.autoUpdate()
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
    
    
class SiteMapData( threading.Thread ):
  
  def __init__( self, csSection = False ):
    threading.Thread.__init__( self )
    if csSection:
      self.csSection = csSection
    else:
      self.csSection = PathFinder.getServiceSection( "Monitoring/SiteMap" )
    self.refreshPeriod = self._getCSValue( "RefreshPeriod", 300 )
    self.gridsToMap = []
    self.sitesData = {}
    self.refresh()
    self.setDaemon( 1 )
    
  def autoUpdate( self ):
    self.start()
  
  def getSitesData( self ):
    return self.sitesData
  
  def run( self ):
    while True:
      time.sleep( self.refreshPeriod )
      gLogger.info( "Refreshing data..." )
      start = time.time()
      self.refresh()
      gLogger.info( "Refresh done. Took %d secs" % int( time.time() - start ) )
    
  def refresh(self):
    self.gridsToMap = self._getCSValue( "GridsToMap", [ "LCG" ] )
    self.refreshPeriod = self._getCSValue( "RefreshPeriod", 300 )
    sitesData = {}
    for func in ( self._updateSiteList, self._updateSiteMask, self._updateJobSummary,
                  self._updatePilotSummary, self._updateDataStorage, self._separateSites ):
#    for func in ( self._updateSiteList, self._separateSites ):
      start = time.time()      
      result = func( sitesData )
      gLogger.info( "Function %s took %.2f secs" % ( func.__name__, time.time() - start ) )
      if not result[ 'OK' ]:
        gLogger.error( "Error while executing %s" % func.__name__, result[ 'Message' ] )
      else:
        sitesData = result[ 'Value' ]
    #We save the data
    self.sitesData = sitesData
    gLogger.info( "There are %s sites" % len( self.sitesData ) )
    
  def _getCSValue( self, option, defVal = None ):
    return gConfig.getValue( "%s/%s" % ( self.csSection, option ), defVal )
    
  def _updateSiteList( self, sitesData ):
    ceSection = "/Resources/Sites"
    for grid in self.gridsToMap:
      gridSection = "%s/%s" % ( ceSection, grid )
      result = gConfig.getSections( gridSection )
      if not result[ 'OK' ]:
        gLogger.error( "Cannot get a list of sites for grid", "%s :%s" % ( grid, result[ 'Message' ] ) )
        continue
      for site in result[ 'Value' ]:
        coords = gConfig.getValue( "%s/%s/Coordinates" % ( gridSection, site ), "" )
        try:
          coords = [ float( "%.4f" % float( c.strip() ) ) for c in coords.split( ":" ) if c.strip() ]
        except Exception, e:
          print e
          gLogger.warn( "Site %s has coordinates incorrectly defined: %s" % ( site, coords ) )
          continue
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
        sitesData[ site ] = siteData
    return S_OK( sitesData )
        
  def _updateSiteMask( self, sitesData ):
    result = RPCClient( "WorkloadManagement/WMSAdministrator" ).getSiteMask()
    if not result[ 'OK' ]:
      gLogger.error( "Cannot get the site mask", result['Message'] )
      return result
    siteMask = result[ 'Value' ]
    siteMaskStatus = dict( sitesData )
    for site in siteMaskStatus:
      if site in siteMask:
        siteMaskStatus[ site ][ 'siteMaskStatus' ] = 'Allowed'
      else:
        siteMaskStatus[ site ][ 'siteMaskStatus' ] = 'Banned'
      sitesData[ site ][ 'siteMaskStatus' ] = siteMaskStatus[ site ][ 'siteMaskStatus' ]
    return S_OK( sitesData )
        
  def _updateJobSummary( self, sitesData ):
    result = RPCClient( 'WorkloadManagement/JobMonitoring' ).getSiteSummary()
    if not result[ 'OK' ]:
      gLogger.error( "Cannot get the site job summary", result['Message'] )
      return result
    summaryData = result[ 'Value' ]
    jobSummary = {}
    for site in sitesData:
      if site in summaryData:
        jobSummary[ site ] = summaryData[ site ]
    for site in sitesData:
      if site in jobSummary:
        sitesData[ site ][ 'jobSummary' ] = jobSummary[ site ]
    return S_OK( sitesData )
        
  def _updatePilotSummary( self, sitesData ):
    result = RPCClient( "WorkloadManagement/WMSAdministrator" ).getPilotSummary()
    if not result[ 'OK' ]:
      gLogger.error( "Cannot get the pilot summary", result['Message'] )
      return result
    summaryData = result[ 'Value' ]
    result = getCESiteMapping()
    if not result['OK']:
      return S_ERROR( 'Could not get CE site mapping' )
    ceMapping = result[ 'Value' ]
    pilotSummary = {}
    for ce in summaryData:
      if ce not in ceMapping:
        continue
      ceData = summaryData[ ce ]
      siteName = ceMapping[ ce ]
      if siteName not in pilotSummary:
        pilotSummary[ siteName ] = {}
      for status in ceData:
        if status not in pilotSummary[ siteName ]:
          pilotSummary[ siteName ][ status ] = 0
        pilotSummary[ siteName ][ status ] += ceData[ status ]
    for site in sitesData:
      if site in pilotSummary:
        sitesData[ site ][ 'pilotSummary' ] = pilotSummary[ site ]
    return S_OK( sitesData )
    
  def _updateDataStorage( self, sitesData ):
    result = RPCClient('DataManagement/StorageUsage' ).getStorageSummary()
    if not result[ 'OK' ]:
      gLogger.error( "Cannot get the data storage summary", result['Message'] )
      return result
    storageSummary = result[ 'Value' ]
    result = getSESiteMapping()
    if not result['OK']:
      return S_ERROR( 'Could not get SE site mapping' )
    seMapping = result[ 'Value' ]
    storageUsage = {}
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
        if siteName not in storageUsage:
          storageUsage[ siteName ] = {}
        if element.find( "_" ) != -1:
          splitter = "_"
        else:
          splitter = "-"
        storageElements = element.split( splitter )[1:]
        if siteName not in storageUsage:
          storageUsage[ siteName ] = {}
        for storageType in storageElements:
          storageType = storageType.upper()
          for attKey in storageSummary[ element ]:
            siteData = storageUsage[ siteName ]
            if attKey not in siteData:
              siteData[ attKey ] = {}
            attData = siteData[ attKey ]
            if storageType not in attData:
              attData[ storageType ] = 0
            attData[ storageType ] += storageSummary[ element ][ attKey ]
    for site in sitesData:
      if site in storageUsage:
        sitesData[ site ][ 'storageSummary' ] = storageUsage[ site ]
    return S_OK( sitesData )
  
  def _separateSites( self, siteData ):
    nearSites = {}
    siteList = siteData.keys()
    siteList.sort()
    for iS in range( len( siteList ) ):
      site = siteList[ iS ]
      for jS in range( iS + 1, len( siteList ) ):
        nSite = siteList[ jS ]
        ll1 = siteData[ site ][ 'longlat' ]
        ll2 = siteData[ nSite ][ 'longlat' ]
        v = ( ll2[0] - ll1[0], ll2[1] - ll1[1] )
        dist = abs( v[0] + v[1] )
        if dist < 0.3:
          print site, nSite, dist
          if site not in nearSites:
            nearSites[ site ] = []
          nearSites[ site ].append( nSite )