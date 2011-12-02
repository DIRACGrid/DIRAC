################################################################################
# $HeadURL $
################################################################################
"""
  This module contains a class to synchronize the content of the DataBase with what is the CS
"""

from DIRAC                                                      import gLogger, S_OK
from DIRAC.Core.Utilities.SiteCEMapping                         import getSiteCEMapping
from DIRAC.Core.Utilities.SitesDIRACGOCDBmapping                import getGOCSiteName, getDIRACSiteName

from DIRAC.ResourceStatusSystem.Utilities                       import CS, Utils
from DIRAC.Core.LCG.GOCDBClient                                 import GOCDBClient

from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient     import ResourceStatusClient
from DIRAC.ResourceStatusSystem.Client.ResourceManagementClient import ResourceManagementClient

class Synchronizer(object):

  def __init__( self, rsClient = None, rmClient = None ):

    self.GOCDBClient = GOCDBClient()
    self.rsClient = ResourceStatusClient()     if rsClient == None else rsClient
    self.rmClient = ResourceManagementClient() if rmClient == None else rmClient

    self.synclist = [ 'Sites', 'Resources', 'StorageElements', 'Services', 'RegistryUsers' ]

################################################################################

  def sync( self, _a, _b ):
    """
    :params:
      :attr:`thingsToSync`: list of things to sync
    """
    gLogger.info( "!!! Sync DB content with CS content for %s !!!" % ( ", ".join(self.synclist) ) )

    for thing in self.synclist:
      getattr( self, '_sync' + thing )()

    return S_OK()

################################################################################
  def __purge_resource(self, resourceName):
    # Maybe remove attached SEs
    SEs = Utils.unpack(self.rsClient.getStorageElement(resourceName=resourceName))
    Utils.unpack(self.rsClient.removeElement("StorageElement", [s[0] for s in SEs]))
    # Remove resource itself.
    Utils.unpack(self.rsClient.removeElement("Resource", resourceName))

  def __purge_site(self, siteName):
    # Remove associated resources and services
    resources = Utils.unpack(self.rsClient.getResource(siteName=siteName))
    services  = Utils.unpack(self.rsClient.getService(siteName=siteName))
    _ = [self.__purge_resource(r[0]) for r in resources]
    Utils.unpack(self.rsClient.removeElement("Service", [s[0] for s in services]))
    # Remove site itself
    Utils.unpack(self.rsClient.removeElement("Site", siteName))

  def _syncSites( self ):
    """
    Sync DB content with sites that are in the CS
    """
    def getGOCTier(sitesList):
      return "T" + str(min([int(v) for v in CS.getSiteTier(sitesList)]))

    # sites in the DB now
    sitesDB = set((s[0] for s in Utils.unpack(self.rsClient.getSite())))

    # sites in CS now
    sitesCS = set(CS.getSites())

    print "Syncing Sites from CS: %d sites in CS, %d sites in DB" % (len(sitesCS), len(sitesDB))

    # remove sites and associated resources, services, and storage
    # elements from the DB that are not in the CS:
    for s in sitesDB - sitesCS:
      gLogger.info("Purging Site %s (not in CS anymore)" % s)
      self.__purge_site(s)

    # add to DB what is missing
    print "Updating %d Sites in DB" % len(sitesCS - sitesDB)
    for site in sitesCS - sitesDB:
      siteType = site.split(".")[0]
      # DIRAC Tier
      tier = "T" + str(CS.getSiteTier( site ))
      if siteType == "LCG":
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

      elif siteType == "DIRAC":
        Utils.protect2(self.rsClient.addOrModifySite, site, tier, "NULL" )

################################################################################
# _syncResources HELPER functions

  def __updateService(self, site, type_):
    service = type_ + '@' + site
    Utils.protect2(self.rsClient.addOrModifyService, service, type_, site )

  def __getServiceEndpointInfo(self, node):
    res = Utils.unpack(self.GOCDBClient.getServiceEndpointInfo( 'hostname', node ))
    if res == []:
      res = Utils.unpack(self.GOCDBClient.getServiceEndpointInfo('hostname', Utils.canonicalURL(node)))
    return res

  def __syncNode(self, NodeInCS, resourcesInDB, resourceType, serviceType, site = "NULL"):

    nodesToUpdate = NodeInCS - resourcesInDB
    if len(nodesToUpdate) > 0:
      print NodeInCS, nodesToUpdate

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
        Utils.protect2(self.rsClient.addOrModifyResource, node, resourceType, serviceType, site, siteInGOCDB )
        resourcesInDB.add( node )

################################################################################

  def _syncResources( self ):
    print "Starting sync of Resources"

    # resources in the DB now
    resourcesInDB = set((r[0] for r in Utils.unpack(self.rsClient.getResource())))

    # Site-CE / Site-SE mapping in CS now
    CEinCS = Utils.unpack(getSiteCEMapping( 'LCG' ))

    # All CEs in CS now
    CEInCS = Utils.set_sanitize([CE for celist in CEinCS.values() for CE in celist])

    # All SE Nodes in CS now
    SENodeInCS = set(CS.getSENodes())

    # LFC Nodes in CS now
    LFCNodeInCS_L = set(CS.getLFCNode(readable = "ReadOnly"))
    LFCNodeInCS_C = set(CS.getLFCNode(readable = "ReadWrite"))

    # FTS Nodes in CS now
    FTSNodeInCS = set([v.split("/")[2][0:-5] for v in CS.getTypedDictRootedAt(root="/Resources/FTSEndpoints").values()])

    # VOMS Nodes in CS now
    VOMSNodeInCS = set(CS.getVOMSEndpoints())

    # complete list of resources in CS now
    resourcesInCS = CEInCS | SENodeInCS | LFCNodeInCS_L | LFCNodeInCS_C | FTSNodeInCS | VOMSNodeInCS

    print "  %d resources in CS, %s resources in DB, updating %d resources" % (len(resourcesInCS), len(resourcesInDB), len(resourcesInCS)-len(resourcesInDB))

    # Remove resources that are not in the CS anymore
    for res in resourcesInDB - resourcesInCS:
      gLogger.info("Purging resource %s. Reason: not in CS anywore." % res)
      self.__purge_resource(res)

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
    CSSEs = set(CS.getSEs())
    DBSEs = set((s[0] for s in Utils.unpack(self.rsClient.getStorageElement())))

    # Remove storageElements that are in DB but not in CS
    for se in DBSEs - CSSEs:
      Utils.protect2(self.rsClient.removeElement, 'StorageElement', se )

    # Add new storage elements
    print "Updating %d StorageElements in DB (%d on CS vs %d on DB)" % (len(CSSEs - DBSEs), len(CSSEs), len(DBSEs))
    for SE in CSSEs - DBSEs:
      srm = CS.getSEHost( SE )
      if not srm:
        print "Warning! %s has no srm URL in CS!!!" % SE
        continue
      siteInGOCDB = Utils.unpack(self.GOCDBClient.getServiceEndpointInfo( 'hostname', srm ))
      if siteInGOCDB == []:
        print "Warning! %s is not in GOCDB!!!" % srm
        continue
      siteInGOCDB = siteInGOCDB[ 0 ][ 'SITENAME' ]
      Utils.protect2(self.rsClient.addOrModifyStorageElement, SE, srm, siteInGOCDB )

################################################################################

  def _syncServices(self):
    """This function is in charge of cleaning the Service table in DB
    in case of obsolescence."""
    # services in the DB now
    servicesInDB = Utils.unpack(self.rsClient.getService())
    for service_name, service_type, site_name in servicesInDB:
      if Utils.unpack(self.rsClient.getResource(siteName=site_name, serviceType=service_type)) == [] \
      and service_type not in ["VO-BOX", "CondDB"]:
        print "Deleting Service %s since it has no corresponding resources." % service_name
        Utils.protect2(self.rsClient.removeElement, "Service", service_name)

  def _syncRegistryUsers(self):
    users = CS.getTypedDictRootedAt("Users", root= "/Registry")
    usersInCS = set(users.keys())
    usersInDB = set((u[0] for u in Utils.unpack(self.rmClient.getUserRegistryCache())))
    usersToAdd = usersInCS - usersInDB
    usersToDel = usersInDB - usersInCS

    print "Updating Registry Users: + %d, - %d" % (len(usersToAdd), len(usersToDel))
    if len(usersToAdd) > 0:
      print usersToAdd
    if len(usersToDel) > 0:
      print usersToDel

    for u in usersToAdd:
      if type(users[u]['DN']) == list:
        users[u]['DN'] = users[u]['DN'][0]
      if type(users[u]['Email']) == list:
        users[u]['Email'] = users[u]['Email'][0]
      users[u]['DN'] = users[u]['DN'].split('=')[-1]
      Utils.unpack(self.rmClient.addOrModifyUserRegistryCache( u, users[u]['DN'], users[u]['Email'].lower()))

    for u in usersToDel:
      Utils.protect2(self.rmClient.deleteUserRegistryCache, u)
################################################################################
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
################################################################################

'''
  HOW DOES THIS WORK.

    will come soon...
'''

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
