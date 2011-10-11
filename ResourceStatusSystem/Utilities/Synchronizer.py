"""
This module contains a class to synchronize the content of the DataBase with what is the CS
"""

from DIRAC                                           import gLogger, S_OK
from DIRAC.Core.Utilities.SiteCEMapping              import getSiteCEMapping
from DIRAC.Core.Utilities.SiteSEMapping              import getSiteSEMapping
from DIRAC.Core.Utilities.SitesDIRACGOCDBmapping     import getGOCSiteName, getDIRACSiteName

from DIRAC.ResourceStatusSystem.Utilities            import CS, Utils
from DIRAC.Core.LCG.GOCDBClient                      import GOCDBClient

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
      return "T" + str(min([int(v) for v in Utils.unpack(CS.getSiteTier(sitesList))]))

    # sites in the DB now
    sitesDB = Utils.unpack(self.rsClient.getSites())
    sitesDB = set([s[0] for s in sitesDB])

    # sites in CS now
    sitesCS = set(Utils.unpack(CS.getSites()))
    print "%d sites in CS, %d sites in DB\n" % (len(sitesCS), len(sitesDB))

    # remove sites from the DB that are not in the CS
    for s in sitesDB - sitesCS:
      self.rsClient.deleteSites(s)

    # add to DB what is missing
    for site in sitesCS - sitesDB:
      # DIRAC Tier
      tier = "T" + str(Utils.unpack(CS.getSiteTier( site )))

      # Grid Name of the site
      gridSiteName = Utils.unpack(getGOCSiteName(site))

      # Grid Tier (with a workaround!)
      DIRACSitesOfGridSites = Utils.unpack(getDIRACSiteName(gridSiteName))
      if len( DIRACSitesOfGridSites ) == 1:
        gt = tier
      else:
        gt = getGOCTier( DIRACSitesOfGridSites )

      Utils.protect2(self.rsClient.addOrModifyGridSite, gridSiteName, gt)
      Utils.protect2(self.rsClient.addOrModifySite, site, tier, gridSiteName )

#############################################################################

  def _syncVOBOX( self ):
    """
    Sync DB content with VOBoxes
    LHCb specific
    """

    # services in the DB now
    servicesInDB = Utils.unpack(self.rsClient.getServicesPresent( columns="ServiceName" ))
    servicesInDB = set([ s[0] for s in servicesInDB ])

    for site in CS.getT1s():
      service = 'VO-BOX@' + site
      if service not in servicesInDB:
        Utils.protect2(self.rsClient.addOrModifyService, service, 'VO-BOX', site )

#############################################################################
# _syncResources HELPER functions

  def __updateService(self, site, type_, servicesInCS, servicesInDB):
    service = type_ + '@' + site
    servicesInCS.add( service )
    Utils.protect2(self.rsClient.addOrModifyService, service, type_, site )
    servicesInDB.add( service )

  def __getServiceEndpointInfo(self, node):
    res = Utils.unpack(self.GOCDBClient.getServiceEndpointInfo( 'hostname', node ))
    if res == []:
      res = Utils.unpack(self.GOCDBClient.getServiceEndpointInfo('hostname', Utils.canonicalURL(node)))
    return res

  def __syncNode(self, NodeInCS, servicesInCS, servicesInDB,
                 resourcesInDB, resourceType, serviceType, site = "NULL"):

    print "syncNode"
    print NodeInCS

    nodesToUpdate = NodeInCS - resourcesInDB
    print nodesToUpdate
    print ""
    # Update Service table
    siteInGOCDB = [self.__getServiceEndpointInfo(node) for node in nodesToUpdate]
    siteInGOCDB = Utils.list_sanitize(siteInGOCDB)
    sites = [Utils.unpack(getDIRACSiteName(s[0]['SITENAME'])) for s in siteInGOCDB]
    sites = Utils.list_sanitize(Utils.list_flatten(sites))
    _ = [self.__updateService(s, serviceType, servicesInCS, servicesInDB) for s in sites]

    # Update Resource table
    for node in NodeInCS:
      if serviceType == "Computing":
        resourceType = CS.getCEType(site, node)
      if node not in resourcesInDB and node is not None:
        try:
          siteInGOCDB = self. __getServiceEndpointInfo(node)[0]['SITENAME']
        except IndexError: # No INFO in GOCDB: Node does not exist
          continue

        assert(type(siteInGOCDB) == str)
        Utils.protect2(self.rsClient.addOrModifyResource, node, resourceType, serviceType, site, siteInGOCDB )
        resourcesInDB.add( node )
############################################################################

  def _syncResources( self ):
    gLogger.info("Starting sync of Resources")

    # resources in the DB now
    resourcesInDB = Utils.unpack(self.rsClient.getResourcesPresent( columns="ResourceName" ))
    resourcesInDB = set([r[0] for r in resourcesInDB])

    # services in the DB now
    servicesInDB = Utils.unpack(self.rsClient.getServicesPresent( columns="ServiceName" ))
    servicesInDB = set([s[0] for s in servicesInDB])

    # Site-CE / Site-SE mapping in CS now
    siteCE = Utils.unpack(getSiteCEMapping( 'LCG' ))
    siteSE = Utils.unpack(getSiteSEMapping( 'LCG' ))

    # All CEs in CS now
    CEInCS = Utils.set_sanitize([CE for celist in siteCE.values() for CE in celist])

    # All SEs in CS now
    SEInCS = Utils.set_sanitize([SE for selist in siteSE.values() for SE in selist])

    # All SE Nodes in CS now
    SENodeInCS = Utils.set_sanitize([Utils.unpack(CS.getSENodes( SE )) for SE in SEInCS])

    # LFC Nodes in CS now
    LFCSites = Utils.unpack(CS.getLFCSites())
    sitesRO = [(s, "ReadOnly") for s in LFCSites]
    sitesRW = [(s, "ReadWrite") for s in LFCSites]
    LFCNodesRO = Utils.set_sanitize([Utils.unpack(CS.getLFCNode(*s)) for s in sitesRO])
    LFCNodesRW = Utils.set_sanitize([Utils.unpack(CS.getLFCNode(*s)) for s in sitesRW])
    LFCNodeInCS_L = set((e[0] for e in LFCNodesRO))
    LFCNodeInCS_C = set((e[0] for e in LFCNodesRW))

    # FTS Nodes in CS now
    FTSNodeInCS = Utils.unpack(CS.getFTSSites())
    FTSNodeInCS = Utils.set_sanitize([Utils.unpack(CS.getFTSEndpoint(site)) for site in FTSNodeInCS])
    FTSNodeInCS = set((e[0] for e in FTSNodeInCS))

    # VOMS Nodes in CS now
    VOMSNodeInCS = set(Utils.unpack(CS.getVOMSEndpoints()))

    # complete list of resources in CS now
    resourcesInCS = CEInCS | SENodeInCS | LFCNodeInCS_L | LFCNodeInCS_C | FTSNodeInCS | VOMSNodeInCS

    # list of services in CS now (to be done)
    servicesInCS = set()

    # Remove resources that are not in the CS anymore
    for res in set(resourcesInDB) - set(resourcesInCS):
      self.rsClient.deleteResources( res )
      sesToBeDel = Utils.unpack(self.rsClient.getStorageElementsPresent(
          resourceName = res,
          columns="StorageElementName" ))
      _ = [Utils.protect2(self.rsClient.deleteStorageElements, s[0]) for s in sesToBeDel]

    # Add to DB what is in CS now and wasn't before

    # CEs
    for site in siteCE:
      self.__syncNode(set(siteCE[site]), servicesInCS, servicesInDB, resourcesInDB, "", "Computing", site)

    # SRMs
    self.__syncNode(SENodeInCS, servicesInCS, servicesInDB, resourcesInDB, "SE", "Storage")

    # LFC_C
    self.__syncNode(LFCNodeInCS_C, servicesInCS, servicesInDB, resourcesInDB, "LFC_C", "Storage")

    # LFC_L
    self.__syncNode(LFCNodeInCS_L, servicesInCS, servicesInDB, resourcesInDB, "LFC_L", "Storage")

    # FTSs
    self.__syncNode(FTSNodeInCS, servicesInCS, servicesInDB, resourcesInDB, "FTS", "Storage")

    # VOMSs
    self.__syncNode(VOMSNodeInCS, servicesInCS, servicesInDB, resourcesInDB, "VOMS", "VOMS")

    # Remove services that are not in the CS anymore
    for ser in servicesInDB - servicesInCS:
      serType = ser.split( '@' )[0]
      if serType != 'VO-BOX':
        Utils.protect2(self.rsClient.deleteServices, ser )
        try:
          site = ser.split( '@' )[1]
        except:
          print ( ser,site )

        if serType == 'Storage':
          sesToBeDel = self.rsClient.getStorageElementsPresent( gridSiteName = site, columns="StorageElementName" )
            #sesToBeDel = self.rsClient.getMonitoredsList('StorageElement', ['StorageElementName'], gridSiteName = site )
          if sesToBeDel[ 'OK' ]:
            for seToBeDel in sesToBeDel[ 'Value' ]:
              Utils.protect2(self.rsClient.deleteStorageElements, seToBeDel )


#############################################################################

  def _syncStorageElements( self ):

    # Get StorageElements from the CS and the DB
    CSSEs = set(Utils.unpack(CS.getStorageElements()))
    DBSEs = set((s[0] for s in Utils.unpack(self.rsClient.getStorageElementsPresent( columns="StorageElementName" ))))

    print "%d SEs in CS, %d SEs in DB\n" % (len(CSSEs), len(DBSEs))

    # Remove storageElements that are in DB but not in CS
    for se in DBSEs - CSSEs:
      Utils.protect2(self.rsClient.deleteStorageElements, se )

    # Add new storage Elements
    for SE in CSSEs - DBSEs:
      srm = Utils.unpack(CS.getSENodes( SE ))
      if srm == None:
        print "Warning! %s has no srm URL in CS!!!" % SE
        continue
      siteInGOCDB = Utils.unpack(self.GOCDBClient.getServiceEndpointInfo( 'hostname', srm ))
      if siteInGOCDB == []:
        print "Warning! %s is not in GOCDB!!!" % srm
        continue
      siteInGOCDB = siteInGOCDB[ 0 ][ 'SITENAME' ]
      Utils.protect2(self.rsClient.addOrModifyStorageElement, SE, srm, siteInGOCDB )

#############################################################################

  def _syncRegistryUsers(self):
    users = CS.getTypedDictRootedAt("Users", root= "/Registry")
    for u in users:
      if type(users[u]['DN']) == list:
        users[u]['DN'] = users[u]['DN'][0]
      if type(users[u]['Email']) == list:
        users[u]['Email'] = users[u]['Email'][0]

      users[u]['DN'] = users[u]['DN'].split('=')[-1]
      self.rmDB.registryAddUser(u, users[u]['DN'].lower(), users[u]['Email'].lower())
