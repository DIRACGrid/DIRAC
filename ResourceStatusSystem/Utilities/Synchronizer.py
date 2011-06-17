"""
This module contains a class to synchronize the content of the DataBase with what is the CS
"""

import socket

import datetime

from DIRAC import gLogger, S_OK
from DIRAC.Core.Utilities.SiteCEMapping import getSiteCEMapping
from DIRAC.Core.Utilities.SiteSEMapping import getSiteSEMapping
from DIRAC.Core.Utilities.SitesDIRACGOCDBmapping import getGOCSiteName, getDIRACSiteName

from DIRAC.ResourceStatusSystem.Utilities.CS import getSites, getSiteTier, getSENodes, getLFCSites, getLFCNode, getFTSSites, getVOMSEndpoints, getFTSEndpoint, getCEType, getStorageElements
from DIRAC.ResourceStatusSystem.Utilities.Exceptions import RSSException
from DIRAC.ResourceStatusSystem.PolicySystem.Configurations import ValidStatus, \
    ValidSiteType, ValidServiceType, ValidResourceType
from DIRAC.Core.LCG.GOCDBClient import GOCDBClient

class Synchronizer:

#############################################################################

  def __init__( self, rsDBin = None ):

    self.rsDB = rsDBin

    if self.rsDB == None:
      from DIRAC.ResourceStatusSystem.DB.ResourceStatusDB import ResourceStatusDB
      self.rsDB = ResourceStatusDB()

    self.GOCDBClient = GOCDBClient()

#############################################################################

#  def sync(self, thingsToSync = None, fake_param = None):
  def sync( self, a, b ):
    """
    :params:
      :attr:`thingsToSync`: list of things to sync
    """

#    if thingsToSync == None:
#      thingsToSync = ['Utils', 'Sites', 'VOBOX', 'Resources', 'StorageElements'],

    thingsToSync = ['Utils', 'Sites', 'VOBOX', 'Resources', 'StorageElements']

    gLogger.info( "!!! Sync DB content with CS content for %s !!!" % ( ' '.join( x for x in thingsToSync ) ) )

    for thing in thingsToSync:
      getattr( self, '_sync' + thing )()

    return S_OK()

#############################################################################

  def _syncUtils( self ):
    """
    Sync DB content with what is in :mod:`DIRAC.ResourceStatusSystem.Utilities.Utils`
    """

    statusIn = self.rsDB.getStatusList()
    #delete status not more in Utils
    for stIn in statusIn:
      if stIn not in ValidStatus:
        self.rsDB.removeStatus( stIn )
    #Add new status
    for s in ValidStatus:
      if s not in statusIn:
        self.rsDB.addStatus( s )

    for g in ( 'Site', 'Service', 'Resource' ):
      typeIn = self.rsDB.getTypesList( g )
      if g == 'Site':
        typesList = ValidSiteType
      elif g == 'Service':
        typesList = ValidServiceType
      if g == 'Resource':
        typesList = ValidResourceType
      #delete types not more in Utils
      for tIn in typeIn:
        if tIn not in typesList:
          self.rsDB.removeType( g, tIn )
      #Add new types
      for t in typesList:
        if t not in typeIn:
          self.rsDB.addType( g, t )

#############################################################################

  def _syncSites( self ):
    """
    Sync DB content with sites that are in the CS
    """

    # sites in the DB now
    sitesIn = self.rsDB.getMonitoredsList( 'Site', paramsList = ['SiteName'] )
    sitesIn = [s[0] for s in sitesIn]

    # sites in CS now
    sitesList = getSites()['Value']

    try:
      sitesList.remove( 'LCG.Dummy.ch' )
    except ValueError:
      pass

    # remove sites from the DB not more in the CS
    for site in sitesIn:
      if site not in sitesList:
        self.rsDB.removeSite( site )

    # add to DB what is in CS now and wasn't before
    for site in sitesList:
      if site not in sitesIn:
        # DIRAC Tier
        tier = getSiteTier( site )['Value'][0]
        if tier == 0 or tier == '0':
          t = 'T0'
        elif tier == 1 or tier == '1':
          t = 'T1'
        elif tier == 3 or tier == '3':
          t = 'T3'
        else:
          t = 'T2'

        #Grid Name of the site
        gridSiteName = getGOCSiteName( site )
        if not gridSiteName['OK']:
          raise RSSException, gridSiteName['Message']
        gridSiteName = gridSiteName['Value']

        #Grid Tier (with a workaround!)
        DIRACSitesOfGridSites = getDIRACSiteName( gridSiteName )
        if not DIRACSitesOfGridSites['OK']:
          raise RSSException, DIRACSitesOfGridSites['Message']
        DIRACSitesOfGridSites = DIRACSitesOfGridSites['Value']
        if len( DIRACSitesOfGridSites ) == 1:
          gt = t
        else:
          gt = self.__getGOCTier( DIRACSitesOfGridSites )

        self.rsDB.addOrModifySite( site, t, gridSiteName, 'Active', 'init',
                                  datetime.datetime.utcnow().replace( microsecond = 0 ), 'RS_SVC',
                                  datetime.datetime( 9999, 12, 31, 23, 59, 59 ) )

        self.rsDB.addOrModifyGridSite( gridSiteName, gt )

        sitesIn.append( site )

#############################################################################

  def _syncVOBOX( self ):
    """
    Sync DB content with VOBoxes
    """

    # services in the DB now
    servicesIn = self.rsDB.getMonitoredsList( 'Service', paramsList = ['ServiceName'] )
    servicesIn = [s[0] for s in servicesIn]

    for site in ['LCG.CNAF.it', 'LCG.IN2P3.fr', 'LCG.PIC.es',
                 'LCG.RAL.uk', 'LCG.GRIDKA.de', 'LCG.NIKHEF.nl']:
      service = 'VO-BOX@' + site
      if service not in servicesIn:
        self.rsDB.addOrModifyService( service, 'VO-BOX', site, 'Active', 'init',
                                     datetime.datetime.utcnow().replace( microsecond = 0 ), 'RS_SVC',
                                     datetime.datetime( 9999, 12, 31, 23, 59, 59 ) )


#############################################################################

  def _syncResources( self ):

    # resources in the DB now
    resourcesIn = self.rsDB.getMonitoredsList( 'Resource', paramsList = ['ResourceName'] )
    resourcesIn = [r[0] for r in resourcesIn]

    # services in the DB now
    servicesIn = self.rsDB.getMonitoredsList( 'Service', paramsList = ['ServiceName'] )
    servicesIn = [s[0] for s in servicesIn]

    # Site-CE mapping in CS now
    siteCE = getSiteCEMapping( 'LCG' )['Value']
    # Site-SE mapping in CS now
    siteSE = getSiteSEMapping( 'LCG' )['Value']

    # CEs in CS now
    CEList = []
    for i in siteCE.values():
      for ce in i:
        if ce is None:
          continue
        CEList.append( ce )

    # SEs in CS now
    SEList = []
    for i in siteSE.values():
      for x in i:
        SEList.append( x )

    # SE Nodes in CS now
    SENodeList = []
    for SE in SEList:
      node = getSENodes( SE )['Value'][0]
      if node is None:
        continue
      if node not in SENodeList:
        SENodeList.append( node )

    # LFC Nodes in CS now
    LFCNodeList_L = []
    LFCNodeList_C = []
    for site in getLFCSites()['Value']:
      for readable in ( 'ReadOnly', 'ReadWrite' ):
        LFCNode = getLFCNode( site, readable )['Value']
        if LFCNode is None or LFCNode == []:
          continue
        LFCNode = LFCNode[0]
        if readable == 'ReadWrite':
          if LFCNode not in LFCNodeList_C:
            LFCNodeList_C.append( LFCNode )
        elif readable == 'ReadOnly':
          if LFCNode not in LFCNodeList_L:
            LFCNodeList_L.append( LFCNode )

    # FTS Nodes in CS now
    FTSNodeList = []
    sitesWithFTS = getFTSSites()
    for site in sitesWithFTS['Value']:
      fts = getFTSEndpoint( site )['Value']
      if fts is None or fts == []:
        continue
      fts = fts[0]
      if fts not in FTSNodeList:
        FTSNodeList.append( fts )

    # VOMS Nodes in CS now
    VOMSNodeList = getVOMSEndpoints()['Value']


    # complete list of resources in CS now
    resourcesList = CEList + SENodeList + LFCNodeList_L + LFCNodeList_C + FTSNodeList + VOMSNodeList

    # list of services in CS now (to be done)
    servicesList = []

    #remove resources no more in the CS
    for res in resourcesIn:
      if res not in resourcesList:
        self.rsDB.removeResource( res )
        self.rsDB.removeStorageElement( resourceName = res )


    # add to DB what is in CS now and wasn't before

    # CEs
    for site in siteCE.keys():
      if site == 'LCG.Dummy.ch':
        continue
      for ce in siteCE[site]:
        if ce is None:
          continue
        siteInGOCDB = self.GOCDBClient.getServiceEndpointInfo( 'hostname', ce )
        if not siteInGOCDB['OK']:
          raise RSSException, siteInGOCDB['Message']
        if siteInGOCDB['Value'] == []:
          trueName = socket.gethostbyname_ex( ce )[0]
          siteInGOCDB = self.GOCDBClient.getServiceEndpointInfo( 'hostname', trueName )
        try:
          siteInGOCDB = siteInGOCDB['Value'][0]['SITENAME']
        except IndexError:
          continue
        serviceType = 'Computing'
        service = serviceType + '@' + site

        if service not in servicesList:
          servicesList.append( service )
        if service not in servicesIn:
          self.rsDB.addOrModifyService( service, serviceType, site, 'Active', 'init',
                                       datetime.datetime.utcnow().replace( microsecond = 0 ), 'RS_SVC',
                                       datetime.datetime( 9999, 12, 31, 23, 59, 59 ) )
          servicesIn.append( service )

        if ce not in resourcesIn:
          CEType = getCEType( site, ce )['Value']
          ceType = 'CE'
          if CEType == 'CREAM':
            ceType = 'CREAMCE'
          self.rsDB.addOrModifyResource( ce, ceType, serviceType, site, siteInGOCDB, 'Active', 'init',
                                        datetime.datetime.utcnow().replace( microsecond = 0 ), 'RS_SVC',
                                        datetime.datetime( 9999, 12, 31, 23, 59, 59 ) )
          resourcesIn.append( ce )

    # SRMs
    for srm in SENodeList:
      siteInGOCDB = self.GOCDBClient.getServiceEndpointInfo( 'hostname', srm )
      if not siteInGOCDB['OK']:
        raise RSSException, siteInGOCDB['Message']
      if siteInGOCDB['Value'] == []:
        trueName = socket.gethostbyname_ex( srm )[0]
        siteInGOCDB = self.GOCDBClient.getServiceEndpointInfo( 'hostname', trueName )
      try:
        siteInGOCDB = siteInGOCDB['Value'][0]['SITENAME']
      except IndexError:
        continue
      siteInDIRAC = getDIRACSiteName( siteInGOCDB )
      if not siteInDIRAC['OK']:
        raise RSSException, siteInDIRAC['Message']
      sites = siteInDIRAC['Value']
      serviceType = 'Storage'
      for site in sites:
        service = serviceType + '@' + site
        if service not in servicesList:
          servicesList.append( service )
        if service not in servicesIn:
          self.rsDB.addOrModifyService( service, serviceType, site, 'Active', 'init',
                                       datetime.datetime.utcnow().replace( microsecond = 0 ), 'RS_SVC',
                                       datetime.datetime( 9999, 12, 31, 23, 59, 59 ) )
          servicesIn.append( service )

      if srm not in resourcesIn and srm is not None:
        self.rsDB.addOrModifyResource( srm, 'SE', serviceType, 'NULL', siteInGOCDB, 'Active', 'init',
                                      datetime.datetime.utcnow().replace( microsecond = 0 ), 'RS_SVC',
                                      datetime.datetime( 9999, 12, 31, 23, 59, 59 ) )
        resourcesIn.append( srm )

    # LFC_C
    for lfc in LFCNodeList_C:
      siteInGOCDB = self.GOCDBClient.getServiceEndpointInfo( 'hostname', lfc )
      if not siteInGOCDB['OK']:
        raise RSSException, siteInGOCDB['Message']
      if siteInGOCDB['Value'] == []:
        trueName = socket.gethostbyname_ex( lfc )[0]
        siteInGOCDB = self.GOCDBClient.getServiceEndpointInfo( 'hostname', trueName )
      try:
        siteInGOCDB = siteInGOCDB['Value'][0]['SITENAME']
      except IndexError:
        continue
      siteInDIRAC = getDIRACSiteName( siteInGOCDB )
      if not siteInDIRAC['OK']:
        raise RSSException, siteInDIRAC['Message']
      sites = siteInDIRAC['Value']
      serviceType = 'Storage'
      for site in sites:
        service = serviceType + '@' + site
        if service not in servicesList:
          servicesList.append( service )
        if service not in servicesIn:
          self.rsDB.addOrModifyService( service, serviceType, site, 'Active', 'init',
                                       datetime.datetime.utcnow().replace( microsecond = 0 ), 'RS_SVC',
                                       datetime.datetime( 9999, 12, 31, 23, 59, 59 ) )
          servicesIn.append( service )
      if lfc not in resourcesIn and lfc is not None:
        self.rsDB.addOrModifyResource( lfc, 'LFC_C', serviceType, 'NULL', siteInGOCDB, 'Active', 'init',
                                      datetime.datetime.utcnow().replace( microsecond = 0 ), 'RS_SVC',
                                      datetime.datetime( 9999, 12, 31, 23, 59, 59 ) )
        resourcesIn.append( lfc )

    # LFC_L
    for lfc in LFCNodeList_L:
      siteInGOCDB = self.GOCDBClient.getServiceEndpointInfo( 'hostname', lfc )
      if not siteInGOCDB['OK']:
        raise RSSException, siteInGOCDB['Message']
      if siteInGOCDB['Value'] == []:
        trueName = socket.gethostbyname_ex( lfc )[0]
        siteInGOCDB = self.GOCDBClient.getServiceEndpointInfo( 'hostname', trueName )
      try:
        siteInGOCDB = siteInGOCDB['Value'][0]['SITENAME']
      except IndexError:
        continue
      siteInDIRAC = getDIRACSiteName( siteInGOCDB )
      if not siteInDIRAC['OK']:
        raise RSSException, siteInDIRAC['Message']
      sites = siteInDIRAC['Value']
      serviceType = 'Storage'
      for site in sites:
        service = serviceType + '@' + site
        if service not in servicesList:
          servicesList.append( service )
        if service not in servicesIn:
          self.rsDB.addOrModifyService( service, serviceType, site, 'Active', 'init',
                                       datetime.datetime.utcnow().replace( microsecond = 0 ), 'RS_SVC',
                                       datetime.datetime( 9999, 12, 31, 23, 59, 59 ) )
          servicesIn.append( service )
      if lfc not in resourcesIn and lfc is not None:
        self.rsDB.addOrModifyResource( lfc, 'LFC_L', serviceType, 'NULL', siteInGOCDB, 'Active', 'init',
                                      datetime.datetime.utcnow().replace( microsecond = 0 ), 'RS_SVC',
                                      datetime.datetime( 9999, 12, 31, 23, 59, 59 ) )
        resourcesIn.append( lfc )


    # FTSs
    for fts in FTSNodeList:
      siteInGOCDB = self.GOCDBClient.getServiceEndpointInfo( 'hostname', fts )
      if not siteInGOCDB['OK']:
        raise RSSException, siteInGOCDB['Message']
      if siteInGOCDB['Value'] == []:
        trueName = socket.gethostbyname_ex( fts )[0]
        siteInGOCDB = self.GOCDBClient.getServiceEndpointInfo( 'hostname', trueName )
      try:
        siteInGOCDB = siteInGOCDB['Value'][0]['SITENAME']
      except IndexError:
        continue
      siteInDIRAC = getDIRACSiteName( siteInGOCDB )
      if not siteInDIRAC['OK']:
        raise RSSException, siteInDIRAC['Message']
      sites = siteInDIRAC['Value']
      serviceType = 'Storage'
      for site in sites:
        service = serviceType + '@' + site
        if service not in servicesList:
          servicesList.append( service )
        if service not in servicesIn:
          self.rsDB.addOrModifyService( service, serviceType, site, 'Active', 'init',
                                       datetime.datetime.utcnow().replace( microsecond = 0 ), 'RS_SVC',
                                       datetime.datetime( 9999, 12, 31, 23, 59, 59 ) )
          servicesIn.append( service )
      if fts not in resourcesIn and fts is not None:
        self.rsDB.addOrModifyResource( fts, 'FTS', serviceType, 'NULL', siteInGOCDB, 'Active', 'init',
                                      datetime.datetime.utcnow().replace( microsecond = 0 ), 'RS_SVC',
                                      datetime.datetime( 9999, 12, 31, 23, 59, 59 ) )
        resourcesIn.append( fts )

    # VOMSs
    for voms in VOMSNodeList:
      siteInGOCDB = self.GOCDBClient.getServiceEndpointInfo( 'hostname', voms )
      if not siteInGOCDB['OK']:
        raise RSSException, siteInGOCDB['Message']
      if siteInGOCDB['Value'] == []:
        trueName = socket.gethostbyname_ex( voms )[0]
        siteInGOCDB = self.GOCDBClient.getServiceEndpointInfo( 'hostname', trueName )
      try:
        siteInGOCDB = siteInGOCDB['Value'][0]['SITENAME']
      except IndexError:
        continue
      siteInDIRAC = getDIRACSiteName( siteInGOCDB )
      if not siteInDIRAC['OK']:
        raise RSSException, siteInDIRAC['Message']
      site = siteInDIRAC['Value']
      serviceType = 'VOMS'
      for site in sites:
        service = serviceType + '@' + site
        if service not in servicesList:
          servicesList.append( service )
        if service not in servicesIn:
          self.rsDB.addOrModifyService( service, serviceType, site, 'Active', 'init',
                                       datetime.datetime.utcnow().replace( microsecond = 0 ), 'RS_SVC',
                                       datetime.datetime( 9999, 12, 31, 23, 59, 59 ) )
          servicesIn.append( service )
      if voms not in resourcesIn and voms is not None:
        self.rsDB.addOrModifyResource( voms, 'VOMS', serviceType, 'NULL', siteInGOCDB, 'Active', 'init',
                                      datetime.datetime.utcnow().replace( microsecond = 0 ), 'RS_SVC',
                                      datetime.datetime( 9999, 12, 31, 23, 59, 59 ) )
        resourcesIn.append( voms )

    #remove services no more in the CS
    for ser in servicesIn:
      if ser not in servicesList:
        serType = ser.split( '@' )[0]
        if serType != 'VO-BOX':
          self.rsDB.removeService( ser )
          self.rsDB.removeResource( serviceName = ser )
          site = ser.split( '@' )[1]
          if serType == 'Storage':
            self.rsDB.removeStorageElement( siteName = site )


#############################################################################

  def _syncStorageElements( self ):

    # Get StorageElements from the CS
    SEs = getStorageElements()
    if not SEs['OK']:
      raise RSSException, SEs['Message']
    SEs = SEs['Value']

    for access in ( 'Read', 'Write' ):

      storageElementsIn = self.rsDB.getMonitoredsList( 'StorageElement' + access,
                                                       paramsList = [ 'StorageElementName' ] )
      try:
        storageElementsIn = [ x[ 0 ] for x in storageElementsIn ]
      except IndexError:
        pass

      #remove storageElements no more in the CS
      for se in storageElementsIn:
        if se not in SEs:
          self.rsDB.removeStorageElement( storageElementName = se, access = access )

      #Add new storage Elements
      for SE in SEs:
        srm = getSENodes( SE )[ 'Value' ][ 0 ]
        if srm == None:
          continue
        siteInGOCDB = self.GOCDBClient.getServiceEndpointInfo( 'hostname', srm )
        if not siteInGOCDB[ 'OK' ]:
          raise RSSException, siteInGOCDB[ 'Message' ]
        if siteInGOCDB[ 'Value' ] == []:
          continue
        siteInGOCDB = siteInGOCDB[ 'Value' ][ 0 ][ 'SITENAME' ]

        if SE not in storageElementsIn:
          self.rsDB.addOrModifyStorageElement( SE, srm, siteInGOCDB, 'Active', 'init',
                                               datetime.datetime.utcnow().replace( microsecond = 0 ),
                                              'RS_SVC', datetime.datetime( 9999, 12, 31, 23, 59, 59 ),
                                               access = access )
          storageElementsIn.append( SE )

#############################################################################

  def __getGOCTier( self, sitesList ):

    gridTier = 3

    for site in sitesList:

      tier = getSiteTier( site )['Value'][0]

      if tier == 0 or tier == '0':
        tn = 0
      elif tier == 1 or tier == '1':
        tn = 1
      elif tier == 3 or tier == '3':
        tn = 3
      else:
        tn = 2

      if tn < gridTier:
        gridTier = tn

    if gridTier == 0:
      gt = 'T0'
    elif gridTier == 1:
      gt = 'T1'
    elif gridTier == 3:
      gt = 'T3'
    else:
      gt = 'T2'

    return gt

#############################################################################
