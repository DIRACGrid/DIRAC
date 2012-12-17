## $HeadURL $
#''' Synchronizer
#
#  Module that keeps in sync the CS and the RSS database.
#
#'''
#
#from DIRAC                                                      import gLogger, S_OK
#from DIRAC.Core.LCG.GOCDBClient                                 import GOCDBClient
#from DIRAC.Core.Utilities.SiteCEMapping                         import getSiteCEMapping
#from DIRAC.Core.Utilities.SitesDIRACGOCDBmapping                import getGOCSiteName, getDIRACSiteName
#from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient     import ResourceStatusClient
#from DIRAC.ResourceStatusSystem.Client.ResourceManagementClient import ResourceManagementClient
#from DIRAC.ResourceStatusSystem.Utilities                       import CS, Utils
#
#__RCSID__ = '$Id: $'
#
#class Synchronizer( object ):
#
#  def __init__( self, rsClient = None, rmClient = None ):
#
#    self.GOCDBClient = GOCDBClient()
#    self.rsClient = ResourceStatusClient()     if rsClient == None else rsClient
#    self.rmClient = ResourceManagementClient() if rmClient == None else rmClient
#
#    self.synclist = [ 'Sites', 'Resources', 'StorageElements', 'Services', 'RegistryUsers' ]
#
#################################################################################
#
#  def sync( self, _a, _b ):
#    """
#    :params:
#      :attr:`thingsToSync`: list of things to sync
#    """
#    gLogger.info( "!!! Sync DB content with CS content for %s !!!" % ( ", ".join(self.synclist) ) )
#
#    for thing in self.synclist:
#      getattr( self, '_sync' + thing )()
#
#    return S_OK()
#
#################################################################################
#  def __purge_resource( self, resourceName ):
#    # Maybe remove attached SEs
#    
#    #SEs = Utils.unpack(self.rsClient.getStorageElement(resourceName=resourceName))
#    SEs = self.rsClient.getStorageElement( resourceName = resourceName )
#    if not SEs[ 'OK' ]:
#      gLogger.error( SEs[ 'Message' ] )
#      return SEs
#    
#    #Utils.unpack(self.rsClient.removeElement("StorageElement", [s[0] for s in SEs]))   
#    SEs = [ se[0] for se in SEs ]  
#    res = self.rsClient.removeElement( 'StorageElement', SEs )
#    if not res[ 'OK' ]:
#      gLogger.error( res[ 'Message' ] )
#      return res
#    
#    # Remove resource itself.
#    #Utils.unpack(self.rsClient.removeElement("Resource", resourceName))
#    res = self.rsClient.removeElement( 'Resource', resourceName )
#    if not res[ 'OK' ]:
#      gLogger.error( res[ 'Message' ] ) 
#    
#    return res
#    
#  def __purge_site( self, siteName ):
#    # Remove associated resources and services
#    
#    #resources = Utils.unpack(self.rsClient.getResource(siteName=siteName))
#    resources = self.rsClient.getResource( siteName = siteName )
#    if not resources[ 'OK' ]:
#      gLogger.error( resources[ 'Message' ] )
#      return resources
#    
#    #services  = Utils.unpack(self.rsClient.getService(siteName=siteName))
#    services = self.rsClient.getService( siteName = siteName )
#    if not services[ 'OK' ]:
#      gLogger.error( services[ 'Message' ] )
#      return services
#    
#    #_ = [self.__purge_resource(r[0]) for r in resources]
#    for resource in resources:
#      res = self.__purge_resource( resource[ 0 ] )
#      if not res[ 'OK' ]:
#        gLogger.error( res[ 'Message' ] )
#        return res
#       
#    #Utils.unpack(self.rsClient.removeElement("Service", [s[0] for s in services]))
#    services = [ service[ 0 ] for service in services[ 'Value' ] ]
#    res      = self.rsClient.removeElement( 'Service', services )
#    if not res[ 'OK' ]:
#      gLogger.error( res[ 'Message' ] )
#      return res  
#    
#    # Remove site itself
#    #Utils.unpack(self.rsClient.removeElement("Site", siteName))
#    res = self.rsClient.removeElement( 'Site', siteName )
#    if not res[ 'OK' ]:
#      gLogger.info( res[ 'Message' ] )
#    
#    return res
#    
#  def _syncSites( self ):
#    """
#    Sync DB content with sites that are in the CS
#    """
#    def getGOCTier(sitesList):
#      return "T" + str(min([int(v) for v in CS.getSiteTiers(sitesList)]))
#
#    # sites in the DB now
#    #sitesDB = set((s[0] for s in Utils.unpack(self.rsClient.getSite())))
#    
#    sites = self.rsClient.getSite()
#    if not sites[ 'OK' ]:
#      gLogger.error( sites[ 'Message' ] )
#      return sites
#    sitesDB = set( [ site[0] for site in sites[ 'Value' ] ] )
#
#    # sites in CS now
#    sitesCS = set( CS.getSites() )
#
#    gLogger.info("Syncing Sites from CS: %d sites in CS, %d sites in DB" % (len(sitesCS), len(sitesDB)))
#
#    # remove sites and associated resources, services, and storage
#    # elements from the DB that are not in the CS:
#    for s in sitesDB - sitesCS:
#      gLogger.info("Purging Site %s (not in CS anymore)" % s)
#      self.__purge_site(s)
#
#    # add to DB what is missing
#    gLogger.info("Updating %d Sites in DB" % len(sitesCS - sitesDB))
#    for site in sitesCS - sitesDB:
#      siteType = site.split(".")[0]
#      # DIRAC Tier
#      tier = "T" + str(CS.getSiteTier( site ))
#      if siteType == "LCG":
#        # Grid Name of the site
#        #gridSiteName = Utils.unpack(getGOCSiteName(site))
#        gridSiteName = getGOCSiteName( site )
#        if not gridSiteName[ 'OK' ]:
#          gLogger.error( gridSiteName[ 'Message' ] )
#          return gridSiteName
#        gridSiteName = gridSiteName[ 'Value' ]
#
#        # Grid Tier (with a workaround!)
#        #DIRACSitesOfGridSites = Utils.unpack(getDIRACSiteName(gridSiteName))
#        DIRACSitesOfGridSites = getDIRACSiteName( gridSiteName )
#        if not DIRACSitesOfGridSites[ 'OK' ]:
#          gLogger.error( DIRACSitesOfGridSites[ 'Message' ] )
#          return DIRACSitesOfGridSites
#        DIRACSitesOfGridSites = DIRACSitesOfGridSites[ 'Value' ]
#        
#        if len( DIRACSitesOfGridSites ) == 1:
#          gt = tier
#        else:
#          gt = getGOCTier( DIRACSitesOfGridSites )
#
#        #Utils.protect2(self.rsClient.addOrModifyGridSite, gridSiteName, gt)
#        res = self.rsClient.addOrModifyGridSite( gridSiteName, gt )
#        if not res[ 'OK' ]:
#          gLogger.error( res[ 'Message' ] )
#          return res
#        
#        #Utils.protect2(self.rsClient.addOrModifySite, site, tier, gridSiteName )
#        res = self.rsClient.addOrModifySite( site, tier, gridSiteName )
#        if not res[ 'OK' ]:
#          gLogger.error( res[ 'Message' ] )
#          return res
#
#      elif siteType == "DIRAC":
#        #Utils.protect2(self.rsClient.addOrModifySite, site, tier, "NULL" )
#        res = self.rsClient.addOrModifySite( site, tier, "NULL" )
#        if not res[ 'OK' ]:
#          gLogger.error( res[ 'Message' ] )
#          return res
#
#################################################################################
## _syncResources HELPER functions
#
#  def __updateService(self, site, type_):
#    service = type_ + '@' + site
#    #Utils.protect2(self.rsClient.addOrModifyService, service, type_, site )
#    res = self.rsClient.addOrModifyService( service, type_, site )
#    if not res[ 'OK' ]:
#      gLogger.error( res[ 'Message' ] )
#      return res
#
#  def __getServiceEndpointInfo(self, node):
#    #res = Utils.unpack( self.GOCDBClient.getServiceEndpointInfo( 'hostname', node ) )
#    res = self.GOCDBClient.getServiceEndpointInfo( 'hostname', node )
#    if res['OK']:
#      res = res[ 'Value' ]
#    else:
#      gLogger.warn( 'Error getting hostname info for %s' % node )
#      return []
#        
#    if res == []:
#      #res = Utils.unpack( self.GOCDBClient.getServiceEndpointInfo('hostname', Utils.canonicalURL(node)) )
#      url = Utils.canonicalURL(node)
#      res = self.GOCDBClient.getServiceEndpointInfo('hostname', url )
#      if res['OK']:
#        res = res[ 'Value' ]
#      else:
#        gLogger.warn( 'Error getting canonical hostname info for %s' % node )
#        res = []
#      
#    return res
#
#  def __syncNode(self, NodeInCS, resourcesInDB, resourceType, serviceType, site = "NULL"):
#
#    nodesToUpdate = NodeInCS - resourcesInDB
#    if len(nodesToUpdate) > 0:
#      gLogger.debug(str(NodeInCS))
#      gLogger.debug(str(nodesToUpdate))
#
#    # Update Service table
#    siteInGOCDB = [self.__getServiceEndpointInfo(node) for node in nodesToUpdate]
#    siteInGOCDB = Utils.list_sanitize(siteInGOCDB)
#    #sites = [Utils.unpack(getDIRACSiteName(s[0]['SITENAME'])) for s in siteInGOCDB]
#    
#    sites = []
#    for sInGOCDB in siteInGOCDB:
#      siteName = getDIRACSiteName( sInGOCDB[ 0 ][ 'SITENAME' ] )
#      if not siteName[ 'OK' ]:
#        gLogger.error( siteName[ 'Message' ] )
#        return siteName
#      sites.append( siteName[ 'Value' ] )
#    
#    sites = Utils.list_sanitize( Utils.list_flatten( sites ) )
#    _     = [ self.__updateService(s, serviceType) for s in sites ]
#
#    # Update Resource table
#    for node in NodeInCS:
#      if serviceType == "Computing":
#        resourceType = CS.getCEType(site, node)
#      if node not in resourcesInDB and node is not None:
#        try:
#          siteInGOCDB = self.__getServiceEndpointInfo(node)[0]['SITENAME']
#        except IndexError: # No INFO in GOCDB: Node does not exist
#          gLogger.warn("Node %s is not in GOCDB!! Considering that it does not exists!" % node)
#          continue
#
#        assert(type(siteInGOCDB) == str)
#        #Utils.protect2(self.rsClient.addOrModifyResource, node, resourceType, serviceType, site, siteInGOCDB )
#        res = self.rsClient.addOrModifyResource( node, resourceType, serviceType, site, siteInGOCDB )
#        if not res[ 'OK' ]:
#          gLogger.error( res[ 'Message' ] )
#          return res
#        resourcesInDB.add( node )
#
#################################################################################
#
#  def _syncResources( self ):
#    gLogger.info("Starting sync of Resources")
#
#    # resources in the DB now
#    #resourcesInDB = set((r[0] for r in Utils.unpack(self.rsClient.getResource())))
#    
#    resources = self.rsClient.getResource()
#    if not resources[ 'OK' ]:
#      gLogger.error( resources[ 'Message' ] )
#      return resources
#    
#    resourcesInDB = set( [ resource[ 0 ] for resource in resources[ 'Value' ] ] )
#      
#    # Site-CE / Site-SE mapping in CS now
#    #CEinCS = Utils.unpack(getSiteCEMapping( 'LCG' ))
#    CEinCS = getSiteCEMapping( 'LCG' )
#    if not CEinCS[ 'OK' ]:
#      gLogger.error( CEinCS[ 'Message' ] )
#      return CEinCS
#    CEinCS = CEinCS[ 'Value' ]
#
#    # All CEs in CS now
#    CEInCS = Utils.set_sanitize([CE for celist in CEinCS.values() for CE in celist])
#
#    # All SE Nodes in CS now
#    SENodeInCS = set(CS.getSENodes())
#
#    # LFC Nodes in CS now
#    LFCNodeInCS_L = set(CS.getLFCNode(readable = "ReadOnly"))
#    LFCNodeInCS_C = set(CS.getLFCNode(readable = "ReadWrite"))
#
#    # FTS Nodes in CS now
#    FTSNodeInCS = set([v.split("/")[2][0:-5] for v
#                       in CS.getTypedDictRootedAt(root="/Resources/FTSEndpoints").values()])
#
#    # VOMS Nodes in CS now
#    VOMSNodeInCS = set(CS.getVOMSEndpoints())
#
#    # complete list of resources in CS now
#    resourcesInCS = CEInCS | SENodeInCS | LFCNodeInCS_L | LFCNodeInCS_C | FTSNodeInCS | VOMSNodeInCS
#
#    gLogger.info("  %d resources in CS, %s resources in DB, updating %d resources" %
#                 (len(resourcesInCS), len(resourcesInDB), len(resourcesInCS)-len(resourcesInDB)))
#
#    # Remove resources that are not in the CS anymore
#    for res in resourcesInDB - resourcesInCS:
#      gLogger.info("Purging resource %s. Reason: not in CS anywore." % res)
#      self.__purge_resource(res)
#
#    # Add to DB what is in CS now and wasn't before
#
#    # CEs
#    for site in CEinCS:
#      self.__syncNode(set(CEinCS[site]), resourcesInDB, "", "Computing", site)
#
#    # SRMs
#    self.__syncNode(SENodeInCS, resourcesInDB, "SE", "Storage")
#
#    # LFC_C
#    self.__syncNode(LFCNodeInCS_C, resourcesInDB, "LFC_C", "Storage")
#
#    # LFC_L
#    self.__syncNode(LFCNodeInCS_L, resourcesInDB, "LFC_L", "Storage")
#
#    # FTSs
#    self.__syncNode(FTSNodeInCS, resourcesInDB, "FTS", "Storage")
#
#    # VOMSs
#    self.__syncNode(VOMSNodeInCS, resourcesInDB, "VOMS", "VOMS")
#
#################################################################################
#
#  def _syncStorageElements( self ):
#
#    # Get StorageElements from the CS and the DB
#    CSSEs = set(CS.getSEs())
#    #DBSEs = set((s[0] for s in Utils.unpack(self.rsClient.getStorageElement())))
#    ses = self.rsClient.getStorageElement()
#    if not ses[ 'OK' ]:
#      gLogger.error( ses[ 'Message' ] )
#      return ses
#    
#    DBSEs = set( [ se[0] for se in ses[ 'Value' ] ] )  
#
#    # Remove storageElements that are in DB but not in CS
#    for se in DBSEs - CSSEs:
#      #Utils.protect2(self.rsClient.removeElement, 'StorageElement', se )
#      res = self.rsClient.removeElement( 'StorageElement', se )
#      if not res[ 'OK' ]:
#        gLogger.error( res[ 'Message' ] )
#        return res
#
#    # Add new storage elements
#    gLogger.info("Updating %d StorageElements in DB (%d on CS vs %d on DB)" % (len(CSSEs - DBSEs), len(CSSEs), len(DBSEs)))
#    for SE in CSSEs - DBSEs:
#      srm = CS.getSEHost( SE )
#      if not srm:
#        gLogger.warn("%s has no srm URL in CS!!!" % SE)
#        continue
#      #siteInGOCDB = Utils.unpack(self.GOCDBClient.getServiceEndpointInfo( 'hostname', srm ))
#      siteInGOCDB = self.GOCDBClient.getServiceEndpointInfo( 'hostname', srm )
#      if siteInGOCDB[ 'OK' ]:
#        siteInGOCDB = siteInGOCDB[ 'Value' ]
#      else:
#        gLogger.error("Error getting hostname for %s from GOCDB!!!" % srm)
#        continue
#      if siteInGOCDB == []:
#        gLogger.warn("%s is not in GOCDB!!!" % srm)
#        continue
#      siteInGOCDB = siteInGOCDB[ 0 ][ 'SITENAME' ]
#      #Utils.protect2(self.rsClient.addOrModifyStorageElement, SE, srm, siteInGOCDB )
#      res = self.rsClient.addOrModifyStorageElement( SE, srm, siteInGOCDB )
#      if not res[ 'OK' ]:
#        gLogger.error( res[ 'Message' ] )
#        return res
#
#################################################################################
#
#  def _syncServices(self):
#    """This function is in charge of cleaning the Service table in DB
#    in case of obsolescence."""
#    # services in the DB now
#    #servicesInDB = Utils.unpack(self.rsClient.getService())
#    servicesInDB = self.rsClient.getService()
#    if not servicesInDB[ 'OK' ]:
#      gLogger.error( servicesInDB[ 'Message' ] )
#      return servicesInDB
#    servicesInDB = servicesInDB[ 'Value' ]
#    
#    for service_name, service_type, site_name in servicesInDB:
#      if not service_type in ["VO-BOX", "CondDB", "VOMS", "Storage"]:
#        
#        #if Utils.unpack(self.rsClient.getResource(siteName=site_name, serviceType=service_type)) == []:
#        resource = self.rsClient.getResource( siteName = site_name, serviceType = service_type )
#        if not resource[ 'OK' ]:
#          gLogger.error( resource[ 'Message' ] )
#          return resource
#        if resource[ 'Value' ] == []:
#          
#          gLogger.info("Deleting Service %s since it has no corresponding resources." % service_name)
#          #Utils.protect2(self.rsClient.removeElement, "Service", service_name)
#          res = self.rsClient.removeElement( "Service", service_name )
#          if not res[ 'OK' ]:
#            gLogger.error( res[ 'Message' ] )
#            return res
#      elif service_type == "Storage":
#        res = self.rsClient.getSite( siteName = site_name, meta = { 'columns' : 'GridSiteName'} )
#        if res[ 'OK' ]:
#          res = res[ 'Value' ]
#        else:
#          res = []
#        
#        if res:
#          if self.rsClient.getResource( gridSiteName = res[0], serviceType = service_type ) == []:
#            gLogger.info("Deleting Service %s since it has no corresponding resources." % service_name)
#            #Utils.protect2(self.rsClient.removeElement, "Service", service_name)
#            res = self.rsClient.removeElement( "Service", service_name )
#            if not res[ 'OK' ]:
#              gLogger.error( res[ 'Message' ] )
#              return res
#
#  def _syncRegistryUsers(self):
#    users = CS.getTypedDictRootedAt("Users", root= "/Registry")
#    usersInCS = set(users.keys())
#    #usersInDB = set((u[0] for u in Utils.unpack(self.rmClient.getUserRegistryCache())))
#    
#    usersInCache = self.rmClient.getUserRegistryCache()
#    if not usersInCache[ 'OK' ]:
#      gLogger.error( usersInCache[ 'Message' ] )
#      return usersInCache
#    
#    usersInDB = set( [ userInCache[ 0 ] for userInCache in usersInCache[ 'Value' ] ] )
#    
#    usersToAdd = usersInCS - usersInDB
#    usersToDel = usersInDB - usersInCS
#
#    gLogger.info("Updating Registry Users: + %d, - %d" % (len(usersToAdd), len(usersToDel)))
#    if len(usersToAdd) > 0:
#      gLogger.debug(str(usersToAdd))
#    if len(usersToDel) > 0:
#      gLogger.debug(str(usersToDel))
#
#    for u in usersToAdd:
#      if type(users[u]['DN']) == list:
#        users[u]['DN'] = users[u]['DN'][0]
#      if type(users[u]['Email']) == list:
#        users[u]['Email'] = users[u]['Email'][0]
#      users[u]['DN'] = users[u]['DN'].split('=')[-1]
#      
#      #Utils.unpack(self.rmClient.addOrModifyUserRegistryCache( u, users[u]['DN'], users[u]['Email'].lower()))
#      
#      res = self.rmClient.addOrModifyUserRegistryCache( u, users[u]['DN'], users[u]['Email'].lower() )
#      if not res[ 'OK' ]:
#        gLogger.error( res[ 'Message' ] )
#        return res
#
#    for u in usersToDel:
#      #Utils.protect2(self.rmClient.deleteUserRegistryCache, u)
#      res = self.rmClient.deleteUserRegistryCache( u )
#      if not res[ 'OK' ]:
#        gLogger.error( res[ 'Message' ] )
#        return res
#
#################################################################################
##EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF