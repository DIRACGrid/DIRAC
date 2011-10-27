################################################################################
# $HeadURL $
################################################################################
__RCSID__  = "$Id$"

"""
  This module contains a class to synchronize the content of the DataBase with what is the CS
"""

from DIRAC                                           import gLogger, S_OK
from DIRAC.Core.Utilities.SiteCEMapping              import getSiteCEMapping
from DIRAC.Core.Utilities.SitesDIRACGOCDBmapping     import getGOCSiteName, getDIRACSiteName

from DIRAC.ResourceStatusSystem.Utilities            import CS, Utils
from DIRAC.Core.LCG.GOCDBClient                      import GOCDBClient

class Synchronizer(object):

  def __init__( self, rsAPI = None, rmAPI = None ):

    self.rsAPI       = rsAPI
    self.rmAPI       = rmAPI
    self.GOCDBClient = GOCDBClient()

    if self.rsAPI == None:
      from DIRAC.ResourceStatusSystem.API.ResourceStatusAPI import ResourceStatusAPI
      self.rsAPI = ResourceStatusAPI()

    if self.rmAPI == None:
      from DIRAC.ResourceStatusSystem.API.ResourceManagementAPI import ResourceManagementAPI
      self.rmAPI = ResourceManagementAPI()

################################################################################

  def sync( self, _a, _b ):
    """
    :params:
      :attr:`thingsToSync`: list of things to sync
    """

    # FIXME: VOBOX not generic

    thingsToSync = [ 'Sites', 'VOBOX', 'Resources', 'StorageElements', "Services", "CondDBs", 'RegistryUsers' ]
    gLogger.info( "!!! Sync DB content with CS content for %s !!!" % ( ", ".join(thingsToSync) ) )

    for thing in thingsToSync:
      getattr( self, '_sync' + thing )()

    return S_OK()

################################################################################

  def _syncSites( self ):
    """
    Sync DB content with sites that are in the CS
    """
    def getGOCTier(sitesList):
      return "T" + str(min([int(v) for v in Utils.unpack(CS.getSiteTier(sitesList))]))

    # sites in the DB now
    sitesDB = set(Utils.list_flatten(Utils.unpack(self.rsAPI.getSite())))

    # sites in CS now
    sitesCS = set(Utils.unpack(CS.getSites()))

    # remove sites from the DB that are not in the CS
    for s in sitesDB - sitesCS:
      self.rsAPI.removeElement( 'Site', s )

    # add to DB what is missing
    print "Updating %d Sites in DB" % len(sitesCS - sitesDB)
    for site in sitesCS - sitesDB:
      siteType = site.split(".")[0]
      # DIRAC Tier
      tier = "T" + str(Utils.unpack(CS.getSiteTier( site )))
      if siteType == "LCG":
        # Grid Name of the site
        gridSiteName = Utils.unpack(getGOCSiteName(site))

        # Grid Tier (with a workaround!)
        DIRACSitesOfGridSites = Utils.unpack(getDIRACSiteName(gridSiteName))
        if len( DIRACSitesOfGridSites ) == 1:
          gt = tier
        else:
          gt = getGOCTier( DIRACSitesOfGridSites )

        Utils.protect2(self.rsAPI.addOrModifyGridSite, gridSiteName, gt)
        Utils.protect2(self.rsAPI.addOrModifySite, site, tier, gridSiteName )

      elif siteType == "DIRAC":
        Utils.protect2(self.rsAPI.addOrModifySite, site, tier, "NULL" )

################################################################################

  def _syncVOBOX( self ):
    """
    Sync DB content with VOBoxes
    LHCb specific
    """

    # services in the DB now
    VOBOXesInCS = set(Utils.unpack(CS.getT1s()))
    VOBOXesInDB = set(Utils.list_flatten(Utils.unpack(self.rsAPI.getServicePresent(
          serviceType = "VO-BOX", columns = "SiteName" ))))

    print "Updating %d VOBOXes on DB" % len(VOBOXesInCS - VOBOXesInDB)
    for site in VOBOXesInCS - VOBOXesInDB:
      service = 'VO-BOX@' + site
      Utils.protect2(self.rsAPI.addOrModifyService, service, 'VO-BOX', site )

  def _syncCondDBs(self):
    CondDBinCS = set(Utils.unpack(CS.getCondDBs()))
    CondDBinDB = set(Utils.list_flatten(Utils.unpack(self.rsAPI.getServicePresent(
            serviceType = "CondDB", columns = "SiteName"))))

    print "Updating %d CondDBs on DB" % len (CondDBinCS - CondDBinDB)
    for site in CondDBinCS - CondDBinDB:
      service = "CondDB@" + site
      Utils.protect2(self.rsAPI.addOrModifyService, service, 'CondDB', site )

################################################################################
# _syncResources HELPER functions

  def __updateService(self, site, type_):
    service = type_ + '@' + site
    Utils.protect2(self.rsAPI.addOrModifyService, service, type_, site )

  def __getServiceEndpointInfo(self, node):
    res = Utils.unpack(self.GOCDBClient.getServiceEndpointInfo( 'hostname', node ))
    if res == []:
      res = Utils.unpack(self.GOCDBClient.getServiceEndpointInfo('hostname', Utils.canonicalURL(node)))
    return res

  def __syncNode(self, NodeInCS, resourcesInDB, resourceType, serviceType, site = "NULL"):

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
    _ = [self.__updateService(s, serviceType) for s in sites]

    # Update Resource table
    for node in NodeInCS:
      if serviceType == "Computing":
        resourceType = CS.getCEType(site, node)
      if node not in resourcesInDB and node is not None:
        try:
          siteInGOCDB = self. __getServiceEndpointInfo(node)[0]['SITENAME']
        except IndexError: # No INFO in GOCDB: Node does not exist
          print "Node %s is not in GOCDB!! Considering that it does not exists!" % node
          continue

        assert(type(siteInGOCDB) == str)
        Utils.protect2(self.rsAPI.addOrModifyResource, node, resourceType, serviceType, site, siteInGOCDB )
        resourcesInDB.add( node )

################################################################################

  def _syncResources( self ):
    gLogger.info("Starting sync of Resources")

    # resources in the DB now
    resourcesInDB = set(Utils.list_flatten(Utils.unpack(self.rsAPI.getResourcePresent( columns="ResourceName" ))))

    # Site-CE / Site-SE mapping in CS now
    CEinCS = Utils.unpack(getSiteCEMapping( 'LCG' ))

    # All CEs in CS now
    CEInCS = Utils.set_sanitize([CE for celist in CEinCS.values() for CE in celist])

    # All SEs in CS now
    SEInCS = CS.getSpaceTokens()

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

    # Remove resources that are not in the CS anymore
    for res in set(resourcesInDB) - set(resourcesInCS):
      self.rsAPI.removeElement( 'Resource', res )
      sesToBeDel = Utils.unpack(self.rsAPI.getStorageElementPresent(
          resourceName = res,
          columns="StorageElementName" ))
      _ = [Utils.protect2(self.rsAPI.removeElement, 'StorageElement', s[0]) for s in sesToBeDel]

    # Add to DB what is in CS now and wasn't before

    # CEs
    for site in CEinCS:
      self.__syncNode(set(CEinCS[site]), resourcesInDB, "", "Computing", site)

    # SRMs
    self.__syncNode(SENodeInCS, resourcesInDB, "SE", "Storage")

    # LFC_C
    self.__syncNode(LFCNodeInCS_C, resourcesInDB, "LFC_C", "Storage")

    # LFC_L
    self.__syncNode(LFCNodeInCS_L, resourcesInDB, "LFC_L", "Storage")

    # FTSs
    self.__syncNode(FTSNodeInCS, resourcesInDB, "FTS", "Storage")

    # VOMSs
    self.__syncNode(VOMSNodeInCS, resourcesInDB, "VOMS", "VOMS")

################################################################################

  def _syncStorageElements( self ):

    # Get StorageElements from the CS and the DB
    CSSEs = set(CS.getSpaceTokens())
    DBSEs = set(Utils.list_flatten(Utils.unpack(self.rsAPI.getStorageElementPresent(
            columns="StorageElementName" ))))

    # Remove storageElements that are in DB but not in CS
    for se in DBSEs - CSSEs:
      Utils.protect2(self.rsAPI.removeElement, 'StorageElement', se )

    # Add new storage Elements
    print "Updating %d StorageElements in DB (%d on CS vs %d on DB)" % (len(CSSEs - DBSEs), len(CSSEs), len(DBSEs))
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
      Utils.protect2(self.rsAPI.addOrModifyStorageElement, SE, srm, siteInGOCDB )

################################################################################

  def _syncServices(self):
    """This function is in charge of cleaning the Service table in DB
    in case of obsolescence."""
    # services in the DB now
    servicesInDB = set(Utils.list_flatten(Utils.unpack(self.rsAPI.getServicePresent( columns="ServiceName" ))))
    # TODO: Write the code.

  def _syncRegistryUsers(self):
    users = CS.getTypedDictRootedAt("Users", root= "/Registry")
    for u in users:
      if type(users[u]['DN']) == list:
        users[u]['DN'] = users[u]['DN'][0]
      if type(users[u]['Email']) == list:
        users[u]['Email'] = users[u]['Email'][0]

      users[u]['DN'] = users[u]['DN'].split('=')[-1]
      #self.rmClient.registryAddUser(u, users[u]['DN'].lower(), users[u]['Email'].lower())
      self.rmAPI.addOrModifyUserRegistryCache( u, users[u]['DN'].lower(), users[u]['Email'].lower() )
################################################################################
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #  
################################################################################

'''
  HOW DOES THIS WORK.
    
    will come soon...
'''
            
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
