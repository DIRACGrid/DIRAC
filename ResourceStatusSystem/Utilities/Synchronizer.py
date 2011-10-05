"""
This module contains a class to synchronize the content of the DataBase with what is the CS
"""

import socket

#import datetime

from DIRAC import gLogger, S_OK
from DIRAC.Core.Utilities.SiteCEMapping import getSiteCEMapping
from DIRAC.Core.Utilities.SiteSEMapping import getSiteSEMapping
from DIRAC.Core.Utilities.SitesDIRACGOCDBmapping import getGOCSiteName, getDIRACSiteName

from DIRAC.ResourceStatusSystem.Utilities.CS import getSites, getSiteTier, getSENodes, getLFCSites, getLFCNode, getFTSSites, getVOMSEndpoints, getFTSEndpoint, getCEType, getStorageElements
from DIRAC.ResourceStatusSystem.Utilities.Exceptions import RSSException, unpack
#from DIRAC.ResourceStatusSystem.DB.ResourceStatusDB import RSSDBException
#from DIRAC.ResourceStatusSystem import ValidStatus, ValidSiteType, ValidServiceType, ValidResourceType
from DIRAC.Core.LCG.GOCDBClient import GOCDBClient

class Synchronizer(object):

#############################################################################

  def __init__( self, rsClient = None, rmDBin = None ):

    self.rsClient    = rsClient
    self.rmDB        = rmDBin
    self.GOCDBClient = GOCDBClient()

    if self.rsClient == None:
      from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient import ResourceStatusClient
      self.rsClient = ResourceStatusClient()

    if self.rmDB == None:
      from DIRAC.ResourceStatusSystem.DB.ResourceManagementDB import ResourceManagementDB
      self.rmDB = ResourceManagementDB()



#############################################################################
  def sync( self, _a, _b ):
    """
    :params:
      :attr:`thingsToSync`: list of things to sync
    """

    # FIXME: VOBOX not generic
    # FIXME: Add DIRACSites
    # FIXME: Add CONDDB

    thingsToSync = [ 'Sites', 'VOBOX', 'Resources', 'StorageElements', 'RegistryUsers' ]
    gLogger.info( "!!! Sync DB content with CS content for %s !!!" % ( ", ".join(thingsToSync) ) )

    for thing in thingsToSync:
      getattr( self, '_sync' + thing )()

    return S_OK()

#############################################################################

  def _syncSites( self ):
    """
    Sync DB content with sites that are in the CS
    """
    def getGOCTier(sitesList):
      return "T" + str(min([int(v) for v in unpack(getSiteTier(sitesList))]))

    # sites in the DB now
    sitesDB = unpack(self.rsClient.getSites())[0]

    # sites in CS now
    sitesCS = unpack(getSites())

    # remove sites from the DB that are not in the CS
    sitesToDelete = set(sitesDB) - set(sitesCS)
    for s in sitesToDelete:
      self.rsClient.deleteSites(s)

    # add to DB what is missing
    for site in set(sitesCS) - set(sitesDB):
      # DIRAC Tier
      tier = "T" + str(unpack(getSiteTier( site )))

      # Grid Name of the site
      gridSiteName = unpack(getGOCSiteName(site))

      # Grid Tier (with a workaround!)
      DIRACSitesOfGridSites = unpack(getDIRACSiteName(gridSiteName))
      if len( DIRACSitesOfGridSites ) == 1:
        gt = tier
      else:
        gt = getGOCTier( DIRACSitesOfGridSites )

      self.rsClient.addOrModifyGridSite( gridSiteName, gt )
      self.rsClient.addOrModifySite( site, tier, gridSiteName )
      sitesDB.append( site )

#############################################################################

  def _syncVOBOX( self ):
    """
    Sync DB content with VOBoxes
    LHCb specific
    """

    # services in the DB now
    #servicesIn = self.rsClient.getMonitoredsList( 'Service', paramsList = ['ServiceName'] )
    kwargs = { 'columns' : [ 'ServiceName' ]}
    servicesIn = self.rsClient.getServicesPresent( **kwargs )#paramsList = ['ServiceName'] )
    servicesIn = [ s[0] for s in servicesIn ]

    for site in ['LCG.CNAF.it', 'LCG.IN2P3.fr', 'LCG.PIC.es',
                 'LCG.RAL.uk', 'LCG.GRIDKA.de', 'LCG.NIKHEF.nl']:

      service = 'VO-BOX@' + site
      if service not in servicesIn:
        self.rsClient.addOrModifyService( service, 'VO-BOX', site )

#############################################################################

  def _syncResources( self ):

    # resources in the DB now
    #resourcesIn = self.rsClient.getMonitoredsList( 'Resource', paramsList = ['ResourceName'] )
    kwargs = { 'columns' : [ 'ResourceName' ]}
    resourcesIn = self.rsClient.getResourcesPresent( **kwargs )#paramsList = ['ServiceName'] )
    resourcesIn = [r[0] for r in resourcesIn]

    # services in the DB now
    kwargs = { 'columns' : [ 'ServiceName' ]}
    servicesIn = self.rsClient.getServicesPresent( **kwargs )#paramsList = ['ServiceName'] )
    #servicesIn = self.rsClient.getMonitoredsList( 'Service', paramsList = ['ServiceName'] )
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
        self.rsClient.deleteResources( res )
        kwargs = { 'columns' : [ 'StorageElementName' ] }
        sesToBeDel = self.rsClient.getStorageElementsPresent( resourceName = res, **kwargs )
        #sesToBeDel = self.rsClient.getMonitoredsList( 'StorageElement', ['StorageElementName'], resourceName = res )
        if sesToBeDel[ 'OK' ]:
          for seToBeDel in sesToBeDel[ 'Value' ]:
            self.rsClient.deleteStorageElements( seToBeDel[ 0 ] )

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
          try:
            trueName = socket.gethostbyname_ex( ce )[0]
            siteInGOCDB = self.GOCDBClient.getServiceEndpointInfo( 'hostname', trueName )
          except socket.gaierror:
            gLogger.info( '%s returns socket.gaiaerror' % ce )
            print '%s returns socket.gaiaerror' % ce
        try:
          siteInGOCDB = siteInGOCDB['Value'][0]['SITENAME']
        except IndexError:
          continue
        serviceType = 'Computing'
        service = serviceType + '@' + site

        if service not in servicesList:
          servicesList.append( service )
        if service not in servicesIn:
          self.rsClient.addOrModifyService( service, serviceType, site )
          servicesIn.append( service )

        if ce not in resourcesIn:
          CEType = getCEType( site, ce )['Value']
          ceType = 'CE'
          if CEType == 'CREAM':
            ceType = 'CREAMCE'
          self.rsClient.addOrModifyResource( ce, ceType, serviceType, site, siteInGOCDB )
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
          self.rsClient.addOrModifyService( service, serviceType, site )
          servicesIn.append( service )

      if srm not in resourcesIn and srm is not None:

        self.rsClient.addOrModifyResource( srm, 'SE', serviceType, 'NULL', siteInGOCDB )
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
          self.rsClient.addOrModifyService( service, serviceType, site )
          servicesIn.append( service )
      if lfc not in resourcesIn and lfc is not None:

        self.rsClient.addOrModifyResource( lfc, 'LFC_C', serviceType, 'NULL', siteInGOCDB )
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
          self.rsClient.addOrModifyService( service, serviceType, site )
          servicesIn.append( service )
      if lfc not in resourcesIn and lfc is not None:

        self.rsClient.addOrModifyResource( lfc, 'LFC_L', serviceType, 'NULL', siteInGOCDB )
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
          self.rsClient.addOrModifyService( service, serviceType, site )
          servicesIn.append( service )
      if fts not in resourcesIn and fts is not None:
        self.rsClient.addOrModifyResource( fts, 'FTS', serviceType, 'NULL', siteInGOCDB )
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
          self.rsClient.addOrModifyService( service, serviceType, site )
          servicesIn.append( service )

      if voms not in resourcesIn and voms is not None:

        self.rsClient.addOrModifyResource( voms, 'VOMS', serviceType, 'NULL', siteInGOCDB )
        resourcesIn.append( voms )

    #remove services no more in the CS
    for ser in servicesIn:
      if ser not in servicesList:
        serType = ser.split( '@' )[0]
        if serType != 'VO-BOX':
          self.rsClient.deleteServices( ser )
          #resToBeDel = self.rsClient.getMonitoredsList('Resource', ['ResourceName'], serviceName = ser )
          #if resToBeDel[ 'OK' ]:
          #  for reToBeDel in resToBeDel[ 'Value' ]:
          #    self.rsClient.deleteResources( reToBeDel[ 0 ] )
          try:
            site = ser.split( '@' )[1]
          except:
            print ( ser,site )

          if serType == 'Storage':
            kwargs = { 'columns' : [ 'StorageElementName' ] }
            sesToBeDel = self.rsClient.getStorageElementsPresent( gridSiteName = site, **kwargs )
            #sesToBeDel = self.rsClient.getMonitoredsList('StorageElement', ['StorageElementName'], gridSiteName = site )
            if sesToBeDel[ 'OK' ]:
              for seToBeDel in sesToBeDel[ 'Value' ]:
                self.rsClient.deleteStorageElements( seToBeDel )


#############################################################################

  def _syncStorageElements( self ):

    # Get StorageElements from the CS
    SEs = getStorageElements()
    if not SEs['OK']:
      raise RSSException, SEs['Message']
    SEs = SEs['Value']

    kwargs = { 'columns' : [ 'StorageElementName' ] }
    storageElementsIn = self.rsClient.getStorageElementsPresent( **kwargs )
    #storageElementsIn = self.rsClient.getMonitoredsList( 'StorageElement',
    #                                                   paramsList = [ 'StorageElementName' ] )
    try:
      storageElementsIn = [ x[ 0 ] for x in storageElementsIn ]
    except IndexError:
      pass

    #remove storageElements no more in the CS
    for se in storageElementsIn:
      if se not in SEs:
        #self.rsClient.removeStorageElement( storageElementName = se, resourceName = None )
        self.rsClient.deleteStorageElements( se )

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
        self.rsClient.addOrModifyStorageElement( SE, srm, siteInGOCDB )
        storageElementsIn.append( SE )

#############################################################################


#############################################################################

  def _syncRegistryUsers(self):
    from DIRAC.ResourceStatusSystem.Utilities import CS
    users = CS.getTypedDictRootedAt("Users", root= "/Registry")
    for u in users:
      if type(users[u]['DN']) == list:
        users[u]['DN'] = users[u]['DN'][0]
      if type(users[u]['Email']) == list:
        users[u]['Email'] = users[u]['Email'][0]

      users[u]['DN'] = users[u]['DN'].split('=')[-1]
      self.rmDB.registryAddUser(u, users[u]['DN'].lower(), users[u]['Email'].lower())
