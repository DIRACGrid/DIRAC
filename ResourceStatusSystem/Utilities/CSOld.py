'''
  Old CS module. Still here to be recycled.
'''
# 
#''' CS
# 
#  This module offers "helpers" to access the CS, and do some processing.
#  
#'''
#
#import itertools
#
#from DIRAC                                               import S_OK, S_ERROR, gConfig
#from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
#from DIRAC.Core.Utilities                                import List
#from DIRAC.ResourceStatusSystem.Utilities                import Utils
#
__RCSID__  = '$Id: $'
#
#g_BaseRegistrySection   = '/Registry'
#g_BaseResourcesSection  = '/Resources'
#g_BaseConfigSection     = 'RSSConfiguration'
##g_BaseConfigSection     = '/Operations/RSSConfiguration'
#
#### CS HELPER FUNCTIONS
#
#class CSError( Exception ):
#  ''' To be removed '''
#  pass
#
#def getValue( val, default ):
#  '''Wrapper around gConfig.getValue. Returns typed values'''
#  res = gConfig.getValue( val, default )
#  if Utils.isiterable( res ):
#    return [ Utils.typedobj_of_string(e) for e in res ]
#  else:
#    return Utils.typedobj_of_string( res )
#
#def getTypedDictRootedAtOperations( relpath = "", root = g_BaseConfigSection ):
#  '''Gives the configuration rooted at path in a Python dict. The
#  result is a Python dictionnary that reflects the structure of the
#  config file.'''
#  def getTypedDictRootedAt( path ):
#    retval = {}
#
#    opts = Operations().getOptionsDict( path )
#    secs = Operations().getSections( path )
#
#    if not opts[ 'OK' ]:
#      raise CSError, opts[ 'Message' ]
#    if not secs[ 'OK' ]:
#      raise CSError, secs[ 'Message' ]
#
#    opts = opts[ 'Value' ]
#    secs = secs[ 'Value' ]
#
#    for k in opts:
#      if opts[ k ].find( "," ) > -1:
#        retval[ k ] = [ Utils.typedobj_of_string(e) for e in List.fromChar(opts[k]) ]
#      else:
#        retval[ k ] = Utils.typedobj_of_string( opts[ k ] )
#    for i in secs:
#      retval[ i ] = getTypedDictRootedAt( path + "/" + i )
#    return retval
#
#  return getTypedDictRootedAt( root + "/" + relpath )
#
#################################################################################
#
## Mail functions #######################
#
#def getOperationMails( op ):
#  ''' Get emails from Operations section'''
#  return Operations().getValue( "EMail/%s" % op ,"" )
#
## Setup functions ####################
#
#def getSetup():
#  ''' Get setup in which we are running'''
#  return gConfig.getValue( "DIRAC/Setup", "" )
#
## VOMS functions ####################
#
#def getVOMSEndpoints():
#  ''' Get VOMS endpoints '''
#  
#  endpoints = gConfig.getSections( '%s/VOMS/Servers/lhcb/' % g_BaseRegistrySection )
#  if endpoints[ 'OK' ]:
#    return endpoints[ 'Value' ]
#  return [] 
#  #return Utils.unpack( gConfig.getSections( "%s/VOMS/Servers/lhcb/" % g_BaseRegistrySection ) )
#
## Sites functions ###################
#
#def getSites( grids = ( 'LCG', 'DIRAC' ) ):
#  ''' Get sites from CS '''
#  if isinstance( grids, basestring ):
#    grids = ( grids, )
#    
#  sites = []  
#    
#  for grid in grids:
#    
#    gridSites = gConfig.getSections( '%s/Sites/%s' % ( g_BaseResourcesSection, grid ), True )
#    if gridSites[ 'OK' ]:
#      sites.extend( gridSites[ 'Value' ] )      
# 
#  return sites 
##  sites = [Utils.unpack(gConfig.getSections('%s/Sites/%s'
##                                      % ( g_BaseResourcesSection, grid ), True))
##           for grid in grids]
##  return Utils.list_flatten( sites )
#
#def getSiteTiers( sites ):
#  ''' Get tiers from CS '''
#  return [ getValue("%s/Sites/%s/%s/MoUTierLevel"
#                    % (g_BaseResourcesSection, site.split(".")[0], site), 2) for site in sites ]
#
#def getSiteTier( site ):
#  ''' Get tier from site '''
#  return getSiteTiers( [ site ] )[ 0 ]
#
#def getT1s( grids = 'LCG' ):
#  ''' Get Tier 1s '''
#  sites = getSites( grids )
#  tiers = getSiteTiers( sites )
#  pairs = itertools.izip( sites, tiers )
#  return [ s for (s, t) in pairs if t == 1 ]
#
## LFC functions #####################
#
#def getLFCSites():
#  ''' Get LFC sites '''
#  
#  lfcSites = gConfig.getSections( '%s/FileCatalogs/LcgFileCatalogCombined' % g_BaseResourcesSection, True )
#  if lfcSites[ 'OK' ]:
#    return lfcSites[ 'Value' ]
#  return []
##  return Utils.unpack(gConfig.getSections('%s/FileCatalogs/LcgFileCatalogCombined'
##                             % g_BaseResourcesSection, True))
#
#def getLFCNode( sites = None, readable = ( 'ReadOnly', 'ReadWrite' ) ):
#  ''' Get LFC node '''
#  
#  if sites is None:
#    sites = getLFCSites()
#  
#  def getLFCURL(site, mode):
#    return gConfig.getValue("%s/FileCatalogs/LcgFileCatalogCombined/%s/%s"
#                            % ((g_BaseResourcesSection, site, mode)), "")
#
#  if isinstance(sites, basestring)   : sites    = [sites]
#  if isinstance(readable, basestring): readable = [readable]
#
#  node = [[getLFCURL(site, r) for r in readable] for site in sites]
#  node = [url for urlgroup in node for url in urlgroup] # Flatten the list
#  return [n for n in node if n != ""]                   # Filter empty string
#
## Storage Elements functions ########
#def getSENodes():
#  ''' Get StorageElement nodes '''
#  nodes = [getSEHost(SE) for SE in getSEs()]
#  return [n for n in nodes if n != ""]
#
#def getSEStatus( SE, accessType ):
#  ''' Get StorageElement status '''
#  return gConfig.getValue("%s/StorageElements/%s/%s" %
#                           (g_BaseResourcesSection, SE, accessType), "")
#
## CE functions ######################
#
#def getCEType( site, ce, grid = 'LCG' ):
#  ''' Get CE types '''
#  res = gConfig.getValue('%s/Sites/%s/%s/CEs/%s/CEType'
#                          % (g_BaseResourcesSection, grid, site, ce), "CE")
#  return "CREAMCE" if res == "CREAM" else "CE"
#
## CondDB functions ##################
#
#def getCondDBs():
#  ''' Get CondDB'''
#  
#  condDBs = gConfig.getSections( '%s/CondDB' % g_BaseResourcesSection )
#  if condDBs[ 'OK' ]:
#    return condDBs[ 'Value' ]
#  return condDBs
##  return Utils.unpack(gConfig.getSections("%s/CondDB" % g_BaseResourcesSection))
#
#################################################################################
##EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF