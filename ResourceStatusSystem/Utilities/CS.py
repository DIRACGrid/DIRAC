from DIRAC                                   import S_OK, S_ERROR
from DIRAC.Core.Utilities                    import List
from DIRAC.ResourceStatusSystem.Utilities    import Utils
from DIRAC                                   import gConfig

g_BaseRegistrySection   = "/Registry"
g_BaseResourcesSection  = "/Resources"
g_BaseOperationsSection = "/Operations"
g_BaseConfigSection     = "/Operations/RSSConfiguration"

class CSError(Exception):
  pass

def getValue(v):
  """Wrapper around gConfig.getValue. Returns typed values instead of
  a string value"""
  res = gConfig.getValue(v)
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

#############################################################################

def getMailForUser(users):
  from DIRAC.ResourceStatusSystem.DB.ResourceManagementDB import ResourceManagementDB
  rmDB = ResourceManagementDB()

  if isinstance(users, basestring):
    users = [users]

  return S_OK([rmDB.registryGetMailFromLogin(u) for u in users])

#############################################################################

def getVOMSEndpoints():
  return gConfig.getSections("%s/VOMS/Servers/lhcb/" %(g_BaseRegistrySection) ,'')

#############################################################################

def getOperationMails( op ):
  return gConfig.getOption("%s/EMail/%s" %(g_BaseOperationsSection, op) ,'')

#############################################################################

def getSetup():
  return gConfig.getOption("DIRAC/Setup")

#############################################################################

def getExtensions():
  return gConfig.getOption("DIRAC/Extensions")

#############################################################################

def getExt():
  """FIXME: Only works for LHCb. """
  VOExtension = ''
  ext = getExtensions()['Value']
  if 'LHCb' in ext:
    VOExtension = 'LHCb'
  return VOExtension

#############################################################################

def getStorageElementStatus(SE, accessType):
  return gConfig.getOption("%s/StorageElements/%s/%s" %
                           (g_BaseResourcesSection, SE, accessType))

def getSENodes( SEIn ):
  if not SEIn:
    return S_ERROR("Invalid empty argument for function getSENodes")
  if isinstance(SEIn, basestring):
    SE = [SEIn]
  node = [gConfig.getValue("%s/StorageElements/%s/AccessProtocol.1/Host"
                           %( g_BaseResourcesSection, se ) ) for se in SE]
  if isinstance(SEIn, basestring): return S_OK(node[0])
  else:                            return S_OK(node)

def getSites( grids = 'LCG' ):
  if isinstance(grids, basestring):
    grids = [grids]
  sites = []
  for grid in grids:
    s = gConfig.getSections('%s/Sites/%s' %( g_BaseResourcesSection, grid ), True)
    if not s['OK']:
      return s
    sites = sites + s['Value']
  return S_OK(sites)

def getSiteTier( sitesIn ):
  sites = sitesIn
  if isinstance(sitesIn, basestring):
    sites = [sitesIn]
  tiers = [gConfig.getValue("%s/Sites/LCG/%s/MoUTierLevel" % (g_BaseResourcesSection, site)) for site in sites]
  # All sites that have no tier information are considered tier2
  tiers = [t if t else "2" for t in tiers]
  if isinstance(sitesIn, basestring): return S_OK(tiers[0])
  else:                               return S_OK(tiers)

def getLFCSites():
  return gConfig.getSections('%s/FileCatalogs/LcgFileCatalogCombined' %g_BaseResourcesSection, True)

def getHostByToken(space_token):
  return gConfig.getValue('%s/StorageElements/%s/AccessProtocol.1/Host'
                          % (g_BaseResourcesSection, space_token))

def getStorageElements( hostName = None ):
  SEs = gConfig.getSections('%s/StorageElements' %g_BaseResourcesSection)
  if not SEs['OK'] or hostName == None: return SEs
  else:
    SEs = SEs['Value']
    if isinstance(hostName, basestring):
      hostName = [hostName.tolower()]
    else:
      hostName = [h.tolower() for h in hostName]

    hosts_to_SE_assoc_list = [(getHostByToken(SE), SE) for SE in SEs]
    return S_OK([token for (url, token) in hosts_to_SE_assoc_list if url.tolower() in hostName])

def getLFCNode( sites = None, readable = None ):
  if sites == None:
    sites = getLFCSites()
    if not sites['OK']:
      return sites
    sites = sites['Value']
  if readable == None:
    readable = ['ReadOnly', 'ReadWrite']
  if isinstance(sites, basestring):
    sites = [sites]
  if isinstance(readable, basestring):
    readable = [readable]
  node = []
  for site in sites:
    for r in readable:
      n = gConfig.getValue('%s/FileCatalogs/LcgFileCatalogCombined/%s/%s' %(g_BaseResourcesSection,
                                                                            site, r))
      if n != None:
        if n not in node:
          node = node + [n]
  return S_OK(node)

def getFTSSites():
  return gConfig.getOptions("%s/FTSEndpoints" %g_BaseResourcesSection)

def getFTSEndpoint( sites = None ):
  if sites == None:
    sites = getFTSSites()
    if not sites['OK']:
      return sites
    sites = sites['Value']
  if isinstance(sites, basestring):
    sites = [sites]
  ftsNode = []
  for site in sites:
    node = gConfig.getValue("%s/FTSEndpoints/%s" %( g_BaseResourcesSection, site) ).split('/')[2][0:-5]
    if node != None:
      if node not in ftsNode:
        ftsNode = ftsNode + [node]
  return S_OK(ftsNode)

def getCEType( site, ce, grid = 'LCG' ):
  return S_OK(gConfig.getValue('%s/Sites/%s/%s/CEs/%s/CEType' % (g_BaseResourcesSection, grid, site, ce)))
