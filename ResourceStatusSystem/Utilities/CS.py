################################################################################
# $HeadURL $
################################################################################
__RCSID__  = "$Id$"

import itertools

from DIRAC                                                      import S_OK, S_ERROR, gConfig
from DIRAC.Core.Utilities                                       import List

from DIRAC.ResourceStatusSystem.Utilities                       import Utils

g_BaseRegistrySection   = "/Registry"
g_BaseResourcesSection  = "/Resources"
g_BaseOperationsSection = "/Operations"
g_BaseConfigSection     = "/Operations/RSSConfiguration"

### CS HELPER FUNCTIONS

class CSError(Exception):
  pass

def getValue(v, default):
  """Wrapper around gConfig.getValue. Returns typed values"""
  res = gConfig.getValue(v, default)
  if Utils.isiterable(res):
    return [Utils.typedobj_of_string(e) for e in res]
  else:
    return Utils.typedobj_of_string(res)

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

################################################################################

# Mail functions #######################

def getOperationMails( op ):
  return gConfig.getValue("%s/EMail/%s" %(g_BaseOperationsSection, op) ,"")

# Setup functions ####################

def getSetup():
  return gConfig.getValue("DIRAC/Setup", "")

# VOMS functions ####################

def getVOMSEndpoints():
  return gConfig.getSections("%s/VOMS/Servers/lhcb/" % g_BaseRegistrySection)

# Sites functions ###################

def getSites( grids = ('LCG', 'DIRAC') ):
  if isinstance(grids, basestring):
    grids = (grids,)
  sites = [Utils.unpack(gConfig.getSections('%s/Sites/%s'
                                      % ( g_BaseResourcesSection, grid ), True))
           for grid in grids]
  return S_OK(Utils.list_flatten(sites))

def getSiteTier( sites ):
  if isinstance(sites, basestring):
    sites = (sites,)
  tiers = [getValue("%s/Sites/%s/%s/MoUTierLevel"
                    % (g_BaseResourcesSection, site.split(".")[0], site), 2) for site in sites]

  if isinstance(sites, basestring): return S_OK(tiers[0])
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

def getLFCNode( sites = Utils.unpack(getLFCSites()),
                readable = ('ReadOnly', 'ReadWrite')):
  def getLFCURL(site, mode):
    return gConfig.getValue("%s/FileCatalogs/LcgFileCatalogCombined/%s/%s"
                            % ((g_BaseResourcesSection, site, mode)), "")

  if isinstance(sites, basestring)   : sites    = [sites]
  if isinstance(readable, basestring): readable = [readable]

  node = [[getLFCURL(site, r) for r in readable] for site in sites]
  node = [url for urlgroup in node for url in urlgroup] # Flatten the list
  node = [n for n in node if n != ""]                   # Filter empty string
  return S_OK(node)

# Storage Elements functions ########

def getHostByToken(space_token):
  return gConfig.getValue('%s/StorageElements/%s/AccessProtocol.1/Host'
                          % (g_BaseResourcesSection, space_token), "")

def getSpaceTokens():
  return gConfig.getSections("/Resources/StorageElements")

def getStorageElementStatus(SE, accessType):
  return gConfig.getValue("%s/StorageElements/%s/%s" %
                           (g_BaseResourcesSection, SE, accessType), "")

def getSENodes():
  nodes = [getHostByToken(tok) for tok in getSpaceTokens()]
  return S_OK([n for n in nodes if n != ""])

# CE functions ######################

def getCEType( site, ce, grid = 'LCG' ):
  res = gConfig.getValue('%s/Sites/%s/%s/CEs/%s/CEType'
                          % (g_BaseResourcesSection, grid, site, ce), "CE")
  return "CREAMCE" if res == "CREAM" else "CE"

# CondDB functions ##################

def getCondDBs():
  return gConfig.getSections("%s/CondDB" % g_BaseResourcesSection)

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
