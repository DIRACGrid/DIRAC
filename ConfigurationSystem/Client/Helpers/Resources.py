# $HeadURL$

""" Resources helper class gives access to various computing resources descriptions

    The following naming conventions are used:
    
    Site names as input arguments can be specified in a full form, e.g. LCG.CERN.ch, or 
    a short form, e.g. CERN. Site names in the output are given in the short form. To convert
    short site names into long site names use the following utilities:
      
      getSites( sites, fullName=True ) 
      getSiteFullNamesDict( sites )

    All the resources and nodes use the following naming convention for arguments and in
    the outputs: <SiteShortName>::<ResourceName>::<NodeName> where names are the same
    as the names of corresponding sections in the CS. For example
    
      CE: CPPM::marce01
      Queue: CPPM::marce01::jobmanager-pbs-lhcb
      SE: CERN::disk
      AccessProtocol: CERN::disk::SRM
      
    Object names are guaranteed to be unique for the given object type.  

    Objects of the Resources class are constructed with a possible specification
    of the target user community. In this case, the values will be automatically
    resolved specifically for the corresponding VO. Community can be specified either by
    vo or group parameter.

    The following methods to access generic CS resources data are provided following the pattern
    of the configuration client:

      getSiteOption( site, option )
      getSiteOptionsDict( site )
      getSiteValue( site, option, default )

      getResourceOption( resourceName, resourceType, option )
      getResourceOptionsDict( resourceName, resourceType )
      getResourceValue( resourceName, resourceType, option, default )

      getNodeOption( nodeName, nodeType, option )
      getNodeOptionsDict( nodeName, nodeType )
      getNodeValue( nodeName, nodeType, option, default )

    For particular Resource and Node types the following methods can be used

      get<ResourceType>Option( resourceName, option )
      get<ResourceType>OptionsDict( resourceName )
      get<ResourceType>Value( resourceName, option, default )

      get<NodeType>Option( nodeName, option )
      get<NodeType>OptionsDict( nodeName )
      get<NodeType>Value( nodeName, option, default )

    The eligible Resource types and Node types are defined in the RESOURCE_NODE_MAPPING global
    dictionary, there is a one-to-one correspondence as we have a single node type per given
    Resource type, mostly for clarity/readability reasons

    For example,

      getComputingOption( ceName, option ) or getComputingElementOption( ceName, option )
      getQueueOptionsDict( queueName )
      getAccessProtocolValue( accessProtocol, option, default )

    The following series of methods allow to get a list of elements ( sites, resources, nodes )
    which are allowed for usage by the community specified for the Resources() object:

      getSites( selectDict )
      getResources( site, resourceType, selectDict )
      getNodes( resourceName, nodeType, selectDict )
      
      get<ResourceType>Elements( site, selectDict )
      get<NodeType>s( resourceName, selectDict )

    selectDict is an optional dictionary of resource selection criteria. For example:

      getSites( {"Name":['CERN','CPPM','RAL']} )
      getStorageElements( 'LCG.CERN.ch', {'ReadAccess':'Active'} )
      getQueues( 'CERN::ce130', { 'Platform' : ['Linux_x86_64_glibc-2.5','x86_64_ScientificSL_Boron_5.4'] } )

    The following methods allow to get a list of Resources and Nodes across sites

      getEligibleResources( resourceType, selectDict )
      getEligibleNodes( nodeType, resourceSelectDict, nodeSelectDict )
      
    A subset of sites or resources can be specified in the selection dictionaries to limit
    the choice. For example
    
      getEligibleResources( 'Storage', { 'Site':['CERN','CPPM','RAL'] } ) 
      getEligibleNodes( 'Queue', { 'Resource': 'CPPM::marce01', 'Platform':'Linux_x86_64_glibc-2.5' } 

    The following are methods specialized for particular resources:

      getEligibleStorageElements( selectDict={} )
      getStorageElementOptionsDict( seName )
      getQueueDescription( queue )
      getEligibleQueuesInfo( siteList, ceList, ceTypeList, mode ):

"""

__RCSID__ = "$Id$"

import re, os
from types import ListType
from distutils.version import LooseVersion

from DIRAC                                              import S_OK, S_ERROR, gConfig
from DIRAC.ConfigurationSystem.Client.Helpers.Path      import cfgPath
from DIRAC.ConfigurationSystem.Client.Helpers           import CSGlobals, Registry
from DIRAC.Core.Utilities.List                          import uniqueElements

############################################################################
#
#  Global constants

gBaseResourcesSection = "/Resources_new"
RESOURCE_NODE_MAPPING = {
  'Computing':'Queue',
  'Storage':'AccessProtocol',
  'Catalog':'',
  'Transfer':'Channel',
  'CommunityManagement':'',
  'DBServer':'Database'
}
RESOURCE_NAME_SEPARATOR = "::"

############################################################################
#
#  General utilities

def getSites( siteList = [], fullName = False ):
  """ Get the list of site names possibly limited by a given list of sites names
      in arbitrary form
  """
  if not siteList:
    result = gConfig.getSections( cfgPath( gBaseResourcesSection, 'Sites' ) )
    if not result['OK']:
      return result
    sList = result['Value']
  else:
    if not type( siteList ) == ListType:
      siteList = [ siteList ]
    sList = []
    for s in siteList:
      result = getSiteName( s ) 
      if not result['OK']:
        continue
      sList.append( result['Value'] )
    
  resultList = []  
  if fullName:    
    for site in sList:
      result = getSiteFullNames( site )
      if not result['OK']:
        continue
      resultList += result['Value']
  else:  
    for site in sList:
      result = getSiteName( site )
      if not result['OK']:
        continue
      resultList.append( result['Value'] )
    
  return S_OK( resultList )

def getSiteFullNames( site_ ):
  """ Get site full names in all the eligible domains
  """
  result = getSiteName( site_ )
  if not result['OK']:
    return result
  site = result['Value']
  
  result = getSiteDomain( site )
  if not result['OK']:
    return result
  domains = result['Value']
  
  result = getSitePath( site )
  if not result['OK']:
    return result
  sitePath = result['Value']
  country = gConfig.getValue( cfgPath( sitePath, 'Country' ), 'xx' )
  
  resultList = [ '.'.join( [domain,site,country] ) for domain in domains ]
  return S_OK( resultList )
  

def getSiteName( site_, verify=False ):
  """ Get the site name in its basic form
  """

  site = None
  if not '.' in site_:
    if verify:
      result = getSites()
      if not result['OK']:
        return result
      sites = result['Value']
      if site_ in sites:
        site = site_
      if site is not None:
        return S_OK( site )  
      else:
        return S_ERROR( 'Unknown site: %s' % site )
    else:
      return S_OK( site_ )  
  else:
    site = site_
  if site is None:
    return S_ERROR( 'Unknown site %s' % site_ )

  domain = ''
  if len( site.split( '.' ) ) == 2:
    siteName,country = site.split( '.' )
    if verify and len( country ) != 2:
      return S_ERROR( 'Invalid site name: %s' % site )
    else:
      result = S_OK( siteName )
      result['Country'] = country
      return result
  elif len( site.split( '.' ) ) == 3:
    domain,siteName,country = site.split( '.' )
    if verify and len( country ) != 2:
      return S_ERROR( 'Invalid site name: %s' % site )
    else:
      result = S_OK( siteName )
      result['Domain'] = domain
      result['Country'] = country
      return result
  else:
    return S_ERROR( 'Invalid site name: %s' % site )

def getSiteNamesDict( siteList_ ):
  """ Get site names in their basic form for a given list of sites.
      Returns a list of basic site names  
  """
  resultDict = {}
  siteList = siteList_
  if type( siteList_ ) != ListType:
    siteList = [siteList_] 
  
  for site in siteList:
    result = getSiteName( site )
    if not result['OK']:
      return S_ERROR( 'Illegal site name: %s' % site )
    resultDict[site] = result['Value']
    
  return S_OK( resultDict )  

def getSitePath( site ):
  """ Return path to the Site section on CS
  """
  result = getSiteName( site )
  if not result['OK']:
    return result
  siteName = result['Value']
  return S_OK( cfgPath( gBaseResourcesSection, 'Sites', siteName ) )

def getSiteDomain( site ):
  """ Get the domains to which the site participates
  """
  result = getSitePath( site )
  if not result['OK']:
    return result
  sitePath = result['Value']
  siteDomains = gConfig.getValue( cfgPath( sitePath, 'Domain' ), [] )
  if not result['OK']:
    if not "does not exist" in result['Message']:
      return result
  if not siteDomains:
    result = getDomains()
    if not result['OK']:
      return result
    siteDomains = result['Value']
  return S_OK( siteDomains )

def getDomains():
  """ Get the list of all the configured Resources Domains
  """
  result = gConfig.getSections( cfgPath( gBaseResourcesSection, 'Domains' ) )
  return result

def getResourceTypes( site ):
  """ Get existing resource types at the given site
  """
  result = getSitePath( site )
  if not result['OK']:
    return result
  sitePath = result['Value']
  return gConfig.getSections( sitePath )

def getSiteForResource( resourceName ):
  """ Get the site name for the given resource or node specified by name
  """

  result = getSites()
  if not result['OK']:
    return result
  sites = result['Value']

  names = resourceName.split( RESOURCE_NAME_SEPARATOR )
  if len( names ) in [2,3]:
    site = names[0]
    if site in sites:
      return S_OK( names[0])
    else:
      return S_ERROR( 'Unknown site %s' % site ) 
  else:
    return S_ERROR( 'Illegal Resource name %s' % resourceName )
  
#  for site in sites:
#    result = Resources().getResources( site, resourceType )
#    if not result['OK']:
#      continue
#    resources = result['Value']
#    for resource in resources:
#      if resource == resourceName:
#        return S_OK( site )
#
#  return S_ERROR( 'Resource %s of type %s is not found' % ( resourceName, resourceType ) )

####################################################################################
#
#  The Resources helper itself

class Resources( object ):
  """ Class to access Resources configuration data according to a standard three-level
      hierarchy: Site > Resource > Node
  """

  class __InnerResources( object ):
    """ Internal class providing implementation for generic Resources data access methods
    """

    def getOption( self, optionPath, optionVOPath ):
      value = None
      if optionVOPath:
        result = gConfig.getOption( optionVOPath )
        if not result['OK']:
          if not 'does not exist' in result['Message']:
            return result
        else:
          value = result['Value']
      if value is None:
        return gConfig.getOption( optionPath )
      else:
        return S_OK(value)

    def getOptionsDict( self, optionPath, optionVOPath ):
      comOptDict = {}
      if optionVOPath is not None:
        result = gConfig.getOptionsDict( optionVOPath )
        if result['OK']:
          comOptDict = result['Value']
      result = gConfig.getOptionsDict( optionPath )
      if not result['OK']:
        return result
      optDict = result['Value']
      optDict.update( comOptDict )
      return S_OK( optDict )

    def getValue( self, optionPath, optionVOPath, default ):
      
      if optionVOPath is not None:
        value = gConfig.getValue( optionVOPath )
        if value is None:
          return gConfig.getValue( optionPath, default )
        else:
          return gConfig.getValue( optionVOPath, default )
      else:
        return gConfig.getValue( optionPath, default )

    def getElements( self, typePath, community, prefix, selectDict={} ):
      
      result = gConfig.getSections( typePath )
      if not result['OK']:
        return result
      elements = result['Value']
      elementList = []
      for element in elements:
        elementPath = cfgPath( typePath, element )
        if community:
          if not self.__checkElementCommunity( elementPath, community ):
            continue
        if selectDict:
          if not self.__checkElementProperties( element, elementPath, selectDict ):
            continue
        if prefix:  
          elementList.append( RESOURCE_NAME_SEPARATOR.join( [prefix, element] ) )
        else:
          elementList.append( element )

      return S_OK( elementList )

    def __checkElementCommunity( self, elementPath, community ):
      """ Check recursively the given element for eligibility with respect to the given community
      """
      communities = gConfig.getValue( cfgPath( elementPath, 'Communities'), [] )
      if community in communities:
        return True
      if communities:
        return False
      parentPath = os.path.dirname( os.path.dirname( elementPath ) )
      if parentPath == gBaseResourcesSection:
        return True
      else:
        return self.__checkElementCommunity( parentPath, community )

    def __checkElementProperties( self, element, elementPath, selectDict ):
      """ Check the given element for the compliance with the given selection criteria
      """
      if not selectDict:
        return True
      result = gConfig.getOptionsDict( elementPath )
      if not result['OK']:
        return False
      elementDict = result['Value']
      elementDict['Name'] = element      
      finalResult = True
      for property_ in selectDict:
        if not property_ in elementDict:
          return False
        if type( selectDict[property_] ) == ListType:
          if not elementDict[property_] in selectDict[property_]:
            return False
        else:
          if elementDict[property_] != selectDict[property_]:
            return False

      return finalResult

  #########################################################################################
  #
  # The Resources class implementation itself

  def __init__( self, vo=None, group=None ):

    self.__uVO = vo
    self.__uGroup = group
    self.__vo = None
    self.__discoverSettings()
    self.__innerResources = self.__InnerResources()

  def __discoverSettings( self ):
    #Set the VO
    globalVO = CSGlobals.getVO()
    if globalVO:
      self.__vo = globalVO
    elif self.__uVO:
      self.__vo = self.__uVO
    else:
      self.__vo = Registry.getVOForGroup( self.__uGroup )
      if not self.__vo:
        self.__vo = None

  def __getResourcePath( self, resourceName, resourceType ):
    """ Get path to the resource CS section
    """
    items = resourceName.split( RESOURCE_NAME_SEPARATOR )
    return cfgPath( gBaseResourcesSection, 'Sites', items[0], resourceType, items[1] )
  
  def __getResourcePrefix( self, resourceName ):
    """ Get resource name prefix
    """
    items = resourceName.split( RESOURCE_NAME_SEPARATOR )
    return RESOURCE_NAME_SEPARATOR.join( items[:2] )

  def __getNodePath( self, nodeName, resourceType ):
    """ Get path to the resource CS section
    """
    site,resource,node = nodeName.split( RESOURCE_NAME_SEPARATOR )
    return cfgPath( gBaseResourcesSection, 'Sites', site, resourceType, resource, 
                    RESOURCE_NODE_MAPPING[resourceType]+'s', node )

  def __execute( self, *params, **kwargs ):
    """ internal redirector to the appropriate CS sections
    """

    name = self.call
    resourceType = None
    nodeType = None
    siteType = None

    args = list( params )
    args.reverse()

    opType = None
    for op in ['OptionsDict','Option','Value','Elements','s']:
      if name.endswith( op ):
        opType  = op
        break
    if opType is None:
      return S_ERROR('Illegal method name: %s' % name)  
    if opType == 's':
      opType = "Elements"
    method = getattr( self.__innerResources, 'get'+opType )

    resourceName = ''
    sitePath = ''
    if name != "getSites":
      # This is the only generic method not needing the site name as the first argument
      resourceName = args.pop()
      if RESOURCE_NAME_SEPARATOR in resourceName:
        sitePath = resourceName.split( RESOURCE_NAME_SEPARATOR )[0]
      else:
        result = getSitePath( resourceName )      
        if not result['OK']:
          return result
        sitePath = result['Value']

    if name.startswith( 'getSite' ):
      siteType = True
    elif name.startswith( 'getResource' ):
      resourceType = args.pop()
    elif name.startswith( 'getNode' ):
      nodeType = args.pop()
      for rType in RESOURCE_NODE_MAPPING:
        if nodeType == RESOURCE_NODE_MAPPING[rType]:
          resourceType = rType
    else:
      for rType in RESOURCE_NODE_MAPPING:
        if name.startswith( 'get'+rType ):
          resourceType = rType
        nType = RESOURCE_NODE_MAPPING[rType]
        if nType and name.startswith( 'get'+nType ):
          nodeType = nType
          for rType in RESOURCE_NODE_MAPPING:
            if nType == RESOURCE_NODE_MAPPING[rType]:
              resourceType = rType

    optionVOPath = None
    optionPath = None
    typePath = None
    if siteType is not None:
      typePath = cfgPath( gBaseResourcesSection, 'Sites' )
      prefix = ''
      if resourceName:
        optionPath = sitePath
        if self.__vo is not None:
          optionVOPath = cfgPath( optionPath, self.__vo )
    elif nodeType is not None:
      resourcePath = self.__getResourcePath( resourceName, resourceType )
      typePath = cfgPath( resourcePath, nodeType+'s' )
      prefix = self.__getResourcePrefix( resourceName )
      if opType != "Elements":
        nodePath = self.__getNodePath( resourceName, resourceType )
        optionPath = nodePath
        if self.__vo is not None:
          optionVOPath = cfgPath( optionPath, self.__vo )
    elif resourceType is not None:
      typePath = cfgPath( sitePath, resourceType )
      prefix = os.path.basename( sitePath)
      if opType != "Elements":
        resourcePath = self.__getResourcePath( resourceName, resourceType )
        optionPath = resourcePath
        if self.__vo is not None:
          optionVOPath = cfgPath( optionPath, self.__vo )
    else:      
      return S_ERROR( 'Illegal method name: %s' % name )

    if opType in ['Option','Value']:
      option = args.pop()
      optionPath = cfgPath( optionPath, option )
      if optionVOPath:
        optionVOPath = cfgPath( optionVOPath, option )

    if opType == 'Value':
      default = None
      if len( args ) > 0:
        default = args[0]
      elif 'default' in kwargs:
        default = kwargs['default']
      return method( optionPath, optionVOPath, default )
    elif opType == 'Elements':
      selectDict = {}
      if len( args ) > 0:
        selectDict = args[0]
      elif 'selectDict' in kwargs:
        selectDict = kwargs['selectDict']
      if typePath is None:
        return S_ERROR( 'Illegal method arguments' )  
      return method( typePath, self.__vo, prefix, selectDict )
    else:
      return method( optionPath, optionVOPath )

  def __getattr__( self, name ):
    self.call = name
    return self.__execute

  def getEligibleSites( self, selectDict={} ):
    """ Get all the sites eligible according to the selection criteria
    """
    return self.getSites( selectDict=selectDict )

  def getEligibleResources( self, resourceType, selectDict={} ):
    """ Get all the resources eligible according to the selection criteria
    """

    sites = selectDict.get( 'Sites', selectDict.get( 'Site', [] ) )
    if type( sites ) != ListType:
      sites = [sites]
    for key in ['Site','Sites']:
      if key in selectDict:
        selectDict.pop(key)

    result = getSites( sites )
    if not result['OK']:
      return result
    sites = result['Value']

    result = self.getSites( selectDict = {'Name':sites} )
    if not result['OK']:
      return result
    eligibleSites = result['Value']

    resultList = []
    for site in eligibleSites:
      result = self.getResources( site, resourceType, selectDict=selectDict )
      if not result['OK']:
        continue
      if result['Value']:
        resultList += result['Value']

    return S_OK( resultList )

  def getEligibleNodes( self, nodeType, resourceSelectDict={}, nodeSelectDict={} ):
    """ Get all the Access Points eligible according to the selection criteria
    """

    sites = resourceSelectDict.get( 'Sites', resourceSelectDict.get( 'Site', [] ) )
    if type( sites ) != ListType:
      sites = [sites]
    for key in ['Site','Sites']:
      if key in resourceSelectDict:
        resourceSelectDict.pop(key)

    result = getSites( sites )
    if not result['OK']:
      return result
    eligibleSites = result['Value']

    eligibleResources = resourceSelectDict.get( 'Resources', resourceSelectDict.get( 'Resource', [] ) )
    if type( eligibleResources ) != ListType:
      eligibleResources = [eligibleResources]
    for key in ['Resources','Resource']:
      if key in resourceSelectDict:
        resourceSelectDict.pop(key)

    resourceType = None
    for rType in RESOURCE_NODE_MAPPING:
      if RESOURCE_NODE_MAPPING[rType] == nodeType:
        resourceType = rType
    if resourceType is None:
      return S_ERROR( 'Invalid Node type %s' % nodeType )

    resultList = []

    for site in eligibleSites:
      if eligibleResources:
        siteER = eligibleResources
      else:
        result = self.getResources( site, resourceType, selectDict=resourceSelectDict )
        if not result['OK']:
          continue
        siteER = result['Value']
      for resource in siteER:
        result = self.getNodes( resource, nodeType, selectDict=nodeSelectDict )
        if not result['OK']:
          continue
        if result['Value']:
          resultList += result['Value']
          
    return S_OK( resultList )



  ####################################################################################
  #
  #  Specialized tools for particular resources

  def getEligibleStorageElements( self, selectDict={} ):
    """ Get all the eligible Storage Elements according to the selection criteria with
        the names following the SE convention.
    """
    return self.getEligibleResources( 'Storage', selectDict )

  def getCatalogOptionsDict( self, catalogName ):
    """ Get the CS Catalog Options
    """
    result = getSiteForResource( catalogName )
    if not result['OK']:
      return result
    site = result['Value']
    result = self.getResourceOptionsDict( catalogName, 'Catalog' )
    return result

  def getStorageElementOptionsDict( self, seName ):
    """ Get the CS StorageElementOptions
    """
    # Construct the SE path first
    result = getSiteForResource( seName )
    if not result['OK']:
      return result
    site = result['Value']

    result = self.getStorageOptionsDict( seName )
    if not result['OK']:
      return result
    options = result['Value']

    # Help distinguishing storage type
    diskSE = True
    tapeSE = False
    if options.has_key( 'SEType' ):
      # Type should follow the convention TXDY
      seType = options['SEType']
      diskSE = re.search( 'D[1-9]', seType ) != None
      tapeSE = re.search( 'T[1-9]', seType ) != None
    options['DiskSE'] = diskSE
    options['TapeSE'] = tapeSE

    result = self.getAccessProtocols( seName )
    if result['OK']:
      protocol = result['Value'][0]
      result = self.getAccessProtocolOptionsDict( protocol )
      if result['OK']:
        options.update( result['Value'] )

    return S_OK( options )

  def getQueueDescription( self, queue ):
    """ Get parameters of the specified queue
    """
    result = self.getComputingElementOptionsDict( queue )
    if not result['OK']:
      return result
    resultDict = result['Value']
    result = self.getQueueOptionsDict( queue )
    if not result['OK']:
      return result
    resultDict.update( result['Value'] )
    resultDict['Queue'] = queue

    return S_OK( resultDict )

  def getEligibleQueuesInfo( self, siteList = None, ceList = None, ceTypeList = None, mode = None ):
    """ Get CE/queue options according to the specified selection
    """

    ceSelectDict = {}
    if siteList:
      ceSelectDict['Sites'] = siteList
    if ceTypeList is not None:
      ceSelectDict['CEType'] = ceTypeList
    if mode is not None:
      ceSelectDict['SubmissionMode'] = mode

    result = self.getEligibleNodes( 'Queue', resourceSelectDict=ceSelectDict )
    if not result['OK']:
      return result

    queueList = result['Value']
    resultDict = {}
    for queueName in queueList:
      site,ce,queue = queueName.split( RESOURCE_NAME_SEPARATOR )
      result = self.getComputingOptionsDict( site, ce )
      if not result['OK']:
        return result
      ceDict = result['Value']
      result = self.getQueueOptionsDict( site, ce, queue )
      if not result['OK']:
        return result
      queueDict = result['Value']

      resultDict.setdefault( site, {} )
      resultDict[site].setdefault( ce, ceDict )
      resultDict[site][ce].setdefault( 'Queues', {} )
      resultDict[site][ce]['Queues'][queueName] = queueDict

    return S_OK( resultDict )

  def getSiteFullName( self, site ):
    """ Get the site full name including the domain prefix, site name and country code
    """
    # Check if the site name is already in a full form
    if '.' in site and len( site.split('.') ) == 3:
      return S_OK( site )

    result = getSiteName( site )
    if not result['OK']:
      return result
    siteShortName = result['Value']
    result = self.getSiteOption( siteShortName, 'Country' )
    if not result['OK']:
      return result
    country = result['Value']
    result = self.getSiteDomain( site )
    if not result['OK']:
      return result
    domain = result['Value']

    siteFullName = '.'.join( [domain, siteShortName, country] )
    return S_OK( siteFullName )

  def getSiteDomain( self, site ):
    """ Return Domain component from Site Name
    """
    #siteTuple = site.split( "." )
    #if len( siteTuple ) != 3:
    #  # Site does not contain the Domain, check what we can do still
    #  result = getSiteDomain( site )
    #  if not result['OK']:
    #    return S_ERROR('No domains defined for site')
    #  domains = result['Value']
    #  if domains:
    #    return S_OK( domains[0] )
    #  else:
    #    return S_ERROR('No domains defined for site')
    #else:
    #  return S_OK( siteTuple[0] )

    return self.getSiteOption( site, 'Domain' )

############################################################################################
#
#  Other methods

def getSiteTier( site ):
  """ Get the site Tier level according to the MoU agreement
  """
  return Resources().getSiteOption( site, 'MoUTierLevel' )

def getCompatiblePlatforms( originalPlatforms ):
  """ Get a list of platforms compatible with the given list
  """
  if type( originalPlatforms ) == type( ' ' ):
    platforms = [originalPlatforms]
  else:
    platforms = list( originalPlatforms )

  result = gConfig.getOptionsDict( '/Resources/Computing/OSCompatibility' )
  if not ( result['OK'] and result['Value'] ):
    return S_ERROR( "OS compatibility info not found" )

  platformsDict = dict( [( k, v.replace( ' ', '' ).split( ',' ) ) for k, v in result['Value'].iteritems()] )
  for k, v in platformsDict.iteritems():
    if k not in v:
      v.append( k )

  resultList = list( platforms )
  for p in platforms:
    tmpList = platformsDict.get( p, [] )
    for pp in platformsDict:
      if p in platformsDict[pp]:
        tmpList.append( pp )
        tmpList += platformsDict[pp]
    if tmpList:
      resultList += tmpList

  return S_OK( uniqueElements( resultList ) )

def getDIRACPlatform( OS ):
  """ Get standard DIRAC platform(s) compatible with the argument.

      NB: The returned value is a list! ordered, in reverse, using distutils.version.LooseVersion
      In practice the "highest" version (which should be the most "desirable" one is returned first)
  """
  result = gConfig.getOptionsDict( '/Resources/Computing/OSCompatibility' )
  if not ( result['OK'] and result['Value'] ):
    return S_ERROR( "OS compatibility info not found" )

  platformsDict = dict( [( k, v.replace( ' ', '' ).split( ',' ) ) for k, v in result['Value'].iteritems()] )
  for k, v in platformsDict.iteritems():
    if k not in v:
      v.append( k )

  # making an OS -> platforms dict
  os2PlatformDict = dict()
  for platform, osItems in platformsDict.iteritems():
    for osItem in osItems:
      if os2PlatformDict.get( osItem ):
        os2PlatformDict[osItem].append( platform )
      else:
        os2PlatformDict[osItem] = [platform]

  if OS not in os2PlatformDict:
    return S_ERROR( 'No compatible DIRAC platform found for %s' % OS )

  platforms = os2PlatformDict[OS]
  platforms.sort( key = LooseVersion, reverse = True )

  return S_OK( platforms )

#############################################################################

def getGOCSiteName( diracSiteName ):
  """
  Get GOC DB site name, given the DIRAC site name, as it stored in the CS

  :params:
    :attr:`diracSiteName` - string: DIRAC site name (e.g. 'LCG.CERN.ch')
  """
  resources = Resources()
  gocDBName = resources.getSiteValue( diracSiteName, "GOCName" )
  
  if not gocDBName:
    return S_ERROR( "No GOC site name for %s in CS (Not a LCG site ?)" % diracSiteName )
  else:
    return S_OK( gocDBName )

#############################################################################

def getDIRACSiteName( gocSiteName ):
  """
  Get DIRAC site name, given the GOC DB site name, as it stored in the CS

  :params:
    :attr:`gocSiteName` - string: GOC DB site name (e.g. 'CERN-PROD')
  """
  resources = Resources()
  result = resources.getEligibleSites( { "GOCName":gocSiteName } )
  if not result['OK']:
    return result
  
  diracSites = result['Value']
  if diracSites:
    return S_OK( diracSites )

  return S_ERROR( "There's no site with GOCDB name = %s in DIRAC CS" % gocSiteName )

def getFTSServersForSites( self, siteList=None ):
  """ get FTSServers for sites 
  
  :param list siteList: list of sites
  """
  siteList = siteList if siteList else None
  if not siteList:
    siteList = getSites()
    if not siteList["OK"]:
      return siteList
    siteList = siteList["Value"]
  ftsServers = dict()
  resources = Resources()
  for site in siteList:
    result = resources.getEligibleResources( "Transfer", { "Site": site } )
    if not result['OK']:
      continue
    if result['Value']:
      transferSvcs = result['Value'] 
      serv = resources.getTransferValue( transferSvcs, "FTSServer", "" )    
      if serv:
        ftsServers[site] = serv
         
  return S_OK( ftsServers )
