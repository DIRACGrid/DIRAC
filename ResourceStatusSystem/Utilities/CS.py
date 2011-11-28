################################################################################
# $HeadURL $
################################################################################
__RCSID__  = "$Id$"

import itertools

from DIRAC                                   import S_OK, S_ERROR, gConfig
from DIRAC.Core.Utilities                    import List

from DIRAC.ResourceStatusSystem.Utilities    import Utils

g_BaseRegistrySection   = "/Registry"
g_BaseResourcesSection  = "/Resources"
g_BaseOperationsSection = "/Operations"
g_BaseConfigSection     = "/Operations/RSSConfiguration"

### CS HELPER FUNCTIONS

class CSError(Exception):
  pass

def getValue(v):
  """Wrapper around gConfig.getValue. Returns typed values instead of
  a string value"""
  res = gConfig.getValue(v)
  if res == None: return None
  if res.find(",") > -1: # res is a list of values
    return [Utils.typedobj_of_string(e) for e in List.fromChar(res)]
  else: return Utils.typedobj_of_string(res)

def getTypedDict(sectionPath):
  """
  DEPRECATED: use getTypedDictRootedAt instead. This function does
  probably not do what you want.

  Wrapper around gConfig.getOptionsDict. Returns a dict where values are
  typed instead of a dict where values are strings."""
  def typed_dict_of_dict(d):
    for k in d:
      if type(d[k]) == dict:
        d[k] = typed_dict_of_dict(d[k])
      else:
        if d[k].find(",") > -1:
          d[k] = [Utils.typedobj_of_string(e) for e in List.fromChar(d[k])]
        else:
          d[k] = Utils.typedobj_of_string(d[k])
    return d

  res = gConfig.getOptionsDict(g_BaseConfigSection + "/" + sectionPath)
  if res['OK'] == False: raise CSError, res['Message']
  else:                  return typed_dict_of_dict(res['Value'])

def getTypedDictRootedAt(relpath = "", root = g_BaseConfigSection):
  """Gives the configuration rooted at path in a Python dict. The
  result is a Python dictionnary that reflects the structure of the
  config file."""
  def getTypedDictRootedAt(path):
    retval = {}

    opts = gConfig.getOptionsDict(path)
    secs = gConfig.getSections(path)

    if not opts['OK']:
      raise CSError, opts['Message']
    if not secs['OK']:
      raise CSError, secs['Message']

    opts = opts['Value']
    secs = secs['Value']

    for k in opts:
      if opts[k].find(",") > -1:
        retval[k] = [Utils.typedobj_of_string(e) for e in List.fromChar(opts[k])]
      else:
        retval[k] = Utils.typedobj_of_string(opts[k])
    for i in secs:
      retval[i] = getTypedDictRootedAt(path + "/" + i)
    return retval

  return getTypedDictRootedAt(root + "/" + relpath)

def getUserNames():
  return gConfig.getSections("%s/Users" % g_BaseRegistrySection)['Value']

################################################################################

# Mail functions #######################

def getOperationMails( op ):
  return gConfig.getOption("%s/EMail/%s" %(g_BaseOperationsSection, op) ,'')

def getMailForUser(users):
  from DIRAC.ResourceStatusSystem.Client.ResourceManagementClient import ResourceManagementClient
  rmAPI = ResourceManagementClient()

  if isinstance(users, basestring):
    users = [users]

  return rmAPI.getUserRegistryCache( user = users, columns= 'email' ) #      .registryGetMailFromLogin(u) for u in users])

# Setup functions ####################

def getSetup():
  return gConfig.getValue("DIRAC/Setup")

def getExtensions():
  return gConfig.getValue("DIRAC/Extensions", [])

def getExt():
  """FIXME: Write generic code for other VOs """
  try:
    return "LHCb" if "LHCb" in getExtensions() else return ""
  except KeyError:
    return ""

# VOMS functions ####################

def getVOMSEndpoints():
  return gConfig.getSections("%s/VOMS/Servers/lhcb/" %(g_BaseRegistrySection) ,'')

# Sites functions ###################

def getSites( grids = ['LCG', 'DIRAC'] ):
  if isinstance(grids, basestring):
    grids = [grids]
  sites = [Utils.unpack(gConfig.getSections('%s/Sites/%s'
                                      % ( g_BaseResourcesSection, grid ), True))
           for grid in grids]
  return S_OK(Utils.list_flatten(sites))

def getSiteTier( sitesIn ):
  def normalizeTier(val):
    # All sites that have no or invalid tier information are considered tier2
    try:               return int(val)
    except:            return 2
  sites = sitesIn
  if isinstance(sitesIn, basestring):
    sites = [sitesIn]
  tiers = [getValue("%s/Sites/%s/%s/MoUTierLevel"
                    % (g_BaseResourcesSection, site.split(".")[0], site)) for site in sites]

  tiers = [normalizeTier(t) for t in tiers]
  if isinstance(sitesIn, basestring): return S_OK(tiers[0])
  else:                               return S_OK(tiers)

def getT1s(grids = 'LCG'):
  sites = Utils.unpack(getSites(grids))
  tiers = Utils.unpack(getSiteTier(sites))
  pairs = itertools.izip(sites, tiers)
  return S_OK([s for (s, t) in pairs if t == 1])

# LFC functions #####################

def getLFCSites():
  return gConfig.getSections('%s/FileCatalogs/LcgFileCatalogCombined'
                             % g_BaseResourcesSection, True)

def getLFCNode( sites = None, readable = None ):
  def getLFCURL(site, mode):
    return gConfig.getValue("%s/FileCatalogs/LcgFileCatalogCombined/%s/%s"
                            % ((g_BaseResourcesSection, site, mode)))

  if sites == None:
    sites = Utils.unpack(getLFCSites())
  if readable == None:
    readable = ['ReadOnly', 'ReadWrite']
  if isinstance(sites, basestring):
    sites = [sites]
  if isinstance(readable, basestring):
    readable = [readable]
  node = [[getLFCURL(site, r) for r in readable] for site in sites]
  node = [url for urlgroup in node for url in urlgroup] # Flatten the list
  node = [n for n in node if n] # Filter Nones
  return S_OK(node)

# Storage Elements functions ########

from DIRAC.Core.Utilities.SiteSEMapping import getSiteSEMapping
def getSpaceTokens():
  SEinCS = Utils.unpack(getSiteSEMapping( 'LCG' ))
  return Utils.set_sanitize([SE for selist in SEinCS.values() for SE in selist])

def getSENodes( SEIn ):
  if not SEIn:
    return S_ERROR("Invalid empty argument for function getSENodes")
  if isinstance(SEIn, basestring):
    SE = [SEIn]
  node = [gConfig.getValue("%s/StorageElements/%s/AccessProtocol.1/Host"
                           %( g_BaseResourcesSection, se ) ) for se in SE]
  if isinstance(SEIn, basestring): return S_OK(node[0])
  else:                            return S_OK(node)

def getStorageElementStatus(SE, accessType):
  return gConfig.getOption("%s/StorageElements/%s/%s" %
                           (g_BaseResourcesSection, SE, accessType))

def getHostByToken(space_token):
  return gConfig.getValue('%s/StorageElements/%s/AccessProtocol.1/Host'
                          % (g_BaseResourcesSection, space_token))

# FTS functions #####################

def getFTSSites():
  return gConfig.getOptions("%s/FTSEndpoints" %g_BaseResourcesSection)

def getFTSEndpoint( sites = None ):
  if sites == None:
    sites = Utils.unpack(getFTSSites())
  if isinstance(sites, basestring):
    sites = [sites]
  ftsNode = [gConfig.getValue("%s/FTSEndpoints/%s"
                              % ( g_BaseResourcesSection, site) ).split('/')[2][0:-5]
             for site in sites]
  ftsNode = [n for n in ftsNode if n]  # Filter out Nones
  ftsNode = list(set(ftsNode))         # Filter out doublons
  return S_OK(ftsNode)

# CE functions ######################

def getCEType( site, ce, grid = 'LCG' ):
  res = gConfig.getOption('%s/Sites/%s/%s/CEs/%s/CEType'
                             % (g_BaseResourcesSection, grid, site, ce))
  try:
    res = Utils.unpack(res)
    if res == "CREAM": return "CREAMCE"
    else:              return "CE"
  except Utils.RPCError:
    return "CE"

# CondDB functions ##################

def getCondDBs():
  return gConfig.getSections("%s/CondDB" % g_BaseResourcesSection)

################################################################################
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
################################################################################

'''
  HOW DOES THIS WORK.

    will come soon...
'''

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
