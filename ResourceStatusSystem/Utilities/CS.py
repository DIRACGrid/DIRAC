from DIRAC                                   import S_OK
from DIRAC.Core.Utilities                    import List
from DIRAC.ResourceStatusSystem.Utilities    import Utils
from DIRAC import gConfig

g_BaseRegistrySection   = "/Registry"
g_BaseResourcesSection  = "/Resources"
g_BaseOperationsSection = "/Operations"
g_BaseConfigSection     = "/Operations/RSSConfiguration"

class CSError(Exception):
  pass

def getTypedDict(sectionPath):
  """Wrapper around gConfig.getOptionsDict. Returns a dict where
  values are typed instead of a dict where values are strings."""
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
    try:
      opts = gConfig.getOptionsDict(path)['Value']
      secs = gConfig.getSections(path)['Value']
    except KeyError:
      raise CSError, "Unable to access CS."
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

def getMailForName(user):
  """Try to guess the username of a user specified as a string. The
  string is normally Firstname + Lastname, but cannot guarantee it
  formally. This function relies on luck to work. You'd better be a
  lucky person :D"""

  candidates = getUserNames()
  def score(S1, S2, alpha=1., beta=1.):
    return alpha*len(Utils.LongestCommonSubstring(S1, S2)) + beta*Utils.CountCommonLetters(S1, S2)

  name = user.strip().lower().split()
  if len(name) == 1:
    # Strategy one: Lastname
    scores = [score(name[0], c) for c in candidates]
  elif len(name) == 2:
    # Stategy two: Firstname + Lastname
    name = name[0][0] + name[1][0:7]
    scores = [score(name, c) for c in candidates]
  else:
    # Strategy three: Multiple names
    scores = [score(name[-1], c) for c in candidates]

  highscore = max(scores)
  return candidates[scores.index(highscore)]

#############################################################################

def getMailForUser( users ):
  if type(users) != list:
    users = [users]
  mails = []
  for user in users:
    mail = gConfig.getValue("%s/Users/%s/Email" %(g_BaseRegistrySection, user))
    mails.append(mail)
  return S_OK(mails)

#############################################################################

def getVOMSEndpoints():
  voms_S = gConfig.getSections("%s/VOMS/Servers/lhcb/" %(g_BaseRegistrySection) ,'')
  return voms_S

#############################################################################

def getOperationMails( op ):
  mail = gConfig.getValue("%s/EMail/%s" %(g_BaseOperationsSection, op) ,'')
  return S_OK(mail)

#############################################################################

def getSetup():
  setup = gConfig.getValue("DIRAC/Setup")
  return S_OK(setup)

#############################################################################

def getExtensions():
  ext = gConfig.getValue("DIRAC/Extensions")
  return S_OK(ext)

#############################################################################

def getExt():
  VOExtension = ''

  ext = getExtensions()['Value']

  if 'LHCb' in ext:
    VOExtension = 'LHCb'

  return VOExtension

#############################################################################

def getStorageElementStatus( SE, accessType):
  status = gConfig.getValue("%s/StorageElements/%s/%s" %(g_BaseResourcesSection, SE, accessType) )
  return S_OK(status)

def getSENodes( SE ):
  if isinstance(SE, basestring):
    SE = [SE]
  node = []
  for se in SE:
    n = gConfig.getValue("%s/StorageElements/%s/AccessProtocol.1/Host" %( g_BaseResourcesSection,
                                                                          se ) )
    node = node + [n]
  return S_OK(node)

def getSites( grids = None ):
  if grids == None:
    grids = ['LCG']
  if isinstance(grids, basestring):
    grids = [grids]
  sites = []
  for grid in grids:
    s = gConfig.getSections('%s/Sites/%s' %( g_BaseResourcesSection, grid ), True)
    if not s['OK']:
      return s
    sites = sites + s['Value']
  return S_OK(sites)

def getSiteTier( sites ):
  if isinstance(sites, basestring):
    sites = [sites]
  tiers = []
  for site in sites:
    t = gConfig.getValue("%s/Sites/LCG/%s/MoUTierLevel" %( g_BaseResourcesSection, site ) )
    tiers = tiers + [t]
  return S_OK(tiers)

def getLFCSites():
  lfcL = gConfig.getSections('%s/FileCatalogs/LcgFileCatalogCombined' %g_BaseResourcesSection,
                             True)
  return lfcL

def getStorageElements( hostName = None ):
  SEs = gConfig.getSections('%s/StorageElements' %g_BaseResourcesSection)
  if not SEs['OK']:
    return SEs
  SEs = SEs['Value']
  if hostName != None:
    removeSEs = []
    if isinstance(hostName, basestring):
      hostName = [hostName]
    for SE in SEs:
      host = gConfig.getValue('%s/StorageElements/%s/AccessProtocol.1/Host' %(g_BaseResourcesSection, SE) )
      if host not in hostName:
        removeSEs.append(SE)
    for SE in removeSEs:
      SEs.remove(SE)
  return S_OK(SEs)

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
  FTS = gConfig.getOptions("%s/FTSEndpoints" %g_BaseResourcesSection)
  return FTS

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

def getCEType( site, ce, grid = None ):
  if grid == None:
    grid = 'LCG'
  ceT = gConfig.getValue('%s/Sites/%s/%s/CEs/%s/CEType' %(g_BaseResourcesSection,
                                                          grid, site, ce) )
  return S_OK(ceT)
