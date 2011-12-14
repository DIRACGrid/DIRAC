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
  return Utils.unpack(gConfig.getSections("%s/VOMS/Servers/lhcb/" % g_BaseRegistrySection))

# Sites functions ###################

def getSites( grids = ('LCG', 'DIRAC') ):
  if isinstance(grids, basestring):
    grids = (grids,)
  sites = [Utils.unpack(gConfig.getSections('%s/Sites/%s'
                                      % ( g_BaseResourcesSection, grid ), True))
           for grid in grids]
  return Utils.list_flatten(sites)

def getSiteTiers(sites):
  return [getValue("%s/Sites/%s/%s/MoUTierLevel"
                    % (g_BaseResourcesSection, site.split(".")[0], site), 2) for site in sites]

def getSiteTier(site):
  return getSiteTiers([site])[0]

def getT1s(grids = 'LCG'):
  sites = getSites(grids)
  tiers = getSiteTiers(sites)
  pairs = itertools.izip(sites, tiers)
  return [s for (s, t) in pairs if t == 1]

# LFC functions #####################

def getLFCSites():
  return Utils.unpack(gConfig.getSections('%s/FileCatalogs/LcgFileCatalogCombined'
                             % g_BaseResourcesSection, True))

def getLFCNode( sites = getLFCSites(),
                readable = ('ReadOnly', 'ReadWrite')):
  def getLFCURL(site, mode):
    return gConfig.getValue("%s/FileCatalogs/LcgFileCatalogCombined/%s/%s"
                            % ((g_BaseResourcesSection, site, mode)), "")

  if isinstance(sites, basestring)   : sites    = [sites]
  if isinstance(readable, basestring): readable = [readable]

  node = [[getLFCURL(site, r) for r in readable] for site in sites]
  node = [url for urlgroup in node for url in urlgroup] # Flatten the list
  return [n for n in node if n != ""]                   # Filter empty string


# Storage Elements functions ########

def getSEs():
  return Utils.unpack(gConfig.getSections("/Resources/StorageElements"))

def getSEHost(SE):
  return gConfig.getValue('%s/StorageElements/%s/AccessProtocol.1/Host'
                          % (g_BaseResourcesSection, SE), "")

def getSENodes():
  nodes = [getSEHost(SE) for SE in getSEs()]
  return [n for n in nodes if n != ""]

def getSEStatus(SE, accessType):
  return gConfig.getValue("%s/StorageElements/%s/%s" %
                           (g_BaseResourcesSection, SE, accessType), "")

def getSEToken(SE):
  return gConfig.getValue("/Resources/StorageElements/%s/AccessProtocol.1/SpaceToken" % SE, "")

# Space Tokens functions ############

def getSpaceTokens():
  return ["LHCb_USER", "LHCb-Disk", "LHCb-Tape"]

def getSpaceTokenEndpoints():
  return getTypedDictRootedAt(root="", relpath="/Resources/Shares/Disk")

# CE functions ######################

def getCEType( site, ce, grid = 'LCG' ):
  res = gConfig.getValue('%s/Sites/%s/%s/CEs/%s/CEType'
                          % (g_BaseResourcesSection, grid, site, ce), "CE")
  return "CREAMCE" if res == "CREAM" else "CE"

# CondDB functions ##################

def getCondDBs():
  return Utils.unpack(gConfig.getSections("%s/CondDB" % g_BaseResourcesSection))

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
