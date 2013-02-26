# $HeadURL$

""" Resources helper class gives access to various computing resources descriptions

    Objects of the Resources class are constructed with a possible specification
    of the target user community. In this case, the values will be automatically
    resolved for community specification. Community can be specified either by
    vo or group parameter.

    The following methods to access generic CS resources data are provided following the pattern
    of the configuration client

      getSiteOption( site, option )
      getSiteOptionsDict( site )
      getSiteValue( site, option, default ) 

      getResourceOption( site, resourceType, resource, option )
      getResourceOptionsDict( site, resourceType, resource )
      getResourceValue( site, resourceType, resource, option, default )

      getNodeOption( site, resourceType, resource, node, option )
      getNodeOptionsDict( site, resourceType, resource, node )
      getNodeValue( site, resourceType, resource, node, option, default )

    For particular Resource and Node types the following methods can be used

      get<ResourceType>Option( site, option )
      get<ResourceType>OptionsDict( site )
      get<ResourceType>Value( site, option, default )

      get<NodeType>Option( site, node, option )
      get<NodeType>OptionsDict( site, node )
      get<NodeType>Value( site, node, option, default )

    The eligible Resource types and Node types are defined in the RESOURCE_ACCESS_MAPPING global
    dictionary, there is a one-to-one correspondence as we have a single node type per given
    Resource type, mostly for clarity/readability reasons

    For example, 

      getComputingOption( site, ceName, option )
      getQueueOptionsDict( site, ceName, queueName )

    The following series of methods allow to get a list of elements ( sites, resources, access points )
    which are allowed for usage by the community specified for the Resources() object:

      getSites( selectDict )
      get<ResourceType>Elements( site, selectDict )
      get<NodeType>s( site, ceName, selectDict )

    selectDict is an optional dictionary of resource selection criteria. For example:

      getStorageElements( site, {'ReadAccess':'Active'} )
      getQueues( site, ceName, { 'Platform' : ['Linux_x86_64_glibc-2.5','x86_64_ScientificSL_Boron_5.4'] } )

    The following methods allow to get a list of Resources and Nodes across sites
      
      getEligibleResources( resourceType, selectDict )
      getEligibleNodes( nodeType, resourceSelectDict, nodeSelectDict )

    The following are methods specialized for particular resources:
    
      getEligibleStorageElements( selectDict={} )
      getStorageElementOptionsDict( seName )
      getQueueDescription( site, ce, queue )
      getEligibleQueues( siteList, ceList, ceTypeList, mode ):        

"""

__RCSID__ = "$Id$"

from DIRAC                                              import S_OK, S_ERROR, gConfig
from DIRAC.ConfigurationSystem.Client.Helpers.Path      import cfgPath
from DIRAC.ConfigurationSystem.Client.Helpers           import CSGlobals, Registry
from DIRAC.Core.Utilities.List                          import uniqueElements
import re, os  
from types import ListType, StringTypes

############################################################################
#
#  Global constants

gBaseResourcesSection = "/Resources"
RESOURCE_ACCESS_MAPPING = {
  'Computing':'Queue',
  'Storage':'AccessProtocol',
  'Catalog':'',
  'Transfer':'Channel',
  'CommunityManagement':'',
  'DBServer':'Database'
}

############################################################################
#
#  General utilities

def getSites():
  """ Get the complete list of sites
  """
  return gConfig.getSections( cfgPath( gBaseResourcesSection, 'Sites' ) )

def getSiteName( site_ ):
  """ Get the site name in a canonical form <Site>.<country>
  """
  
  site = None
  if not '.' in site_:
    result = getSites()
    if not result['OK']:
      return result
    sites = result['Value']
    for siteName in sites:
      if siteName.startswith( site_ ):
        site = siteName  
  else:
    site = site_    
  if site is None:
    return S_ERROR( 'Unknown site %s' % site_ )
  
  domain = ''
  if len( site.split( '.' ) ) == 2:
    siteName,country = site.split( '.' )
    if len( country ) != 2:
      return S_ERROR( 'Invalid site name %s' % site )
    else:
      return S_OK( site )
  elif len( site.split( '.' ) ) == 3:
    domain,siteName,country = site.split( '.' )
    if len( country ) != 2:
      return S_ERROR( 'Invalid site name %s' % site )
    else:
      result = S_OK( '.'.join( [siteName, country] ) )
      result['Domain'] = domain  
      return result
  else:
    return S_ERROR( 'Invalid site name %s' % site )    

def getSitePath( site ):
  """ Return path to the Site section on CS
  """
  result = getSiteName( site )
  if not result['OK']:
    return result
  siteName = result['Value']
  return S_OK( cfgPath( gBaseResourcesSection, 'Sites', siteName ) )

def getSiteDomain( site ):
  """ Return Domain component from Site Name
  """
  siteTuple = site.split( "." )
  if len( siteTuple ) != 3:
    # Site does not contain the Domain, check what we can do still
    result = getSiteDomains( site )
    if not result['OK']:
      return None
    domains = result['Value']
    if domains:
      return domains[0]
    else:
      return None
  else:
    return siteTuple[0]  

def getSiteDomains( site ):
  """ Get the domains to which the site participates
  """
  result = getSitePath( site )
  if not result['OK']:
    return result
  sitePath = result['Value']
  siteDomains = gConfig.getValue( cfgPath( sitePath, 'Domains' ), [] ) 
  if not result['OK']:
    if not "does not exist" in result['Message']:
      return result
  if not siteDomains:
    result = getResourceDomains()
    if not result['OK']:
      return result
    siteDomains = result['Value']
  return S_OK( siteDomains )

def getResourceDomains():
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

# def checkElementCommunity( elementPath, community ):
#   """ Check recursively the given element for eligibility with respect to the given community
#   """
#   communities = gConfig.getValue( cfgPath( elementPath, 'Communities'), [] )
#   if community in communities:
#     return True
#   if communities:
#     return False
#   parentPath = os.path.dirname( os.path.dirname( elementPath ) )
#   if parentPath == gBaseResourcesSection:
#     return True
#   else:
#     return checkElementCommunity( parentPath, community )      

def checkElementProperties( elementPath, selectDict ):
  """ Check the given element for the compliance with the given selection criteria
  """
  if not selectDict:
    return True
  result = gConfig.getOptionsDict( elementPath )
  if not result['OK']:
    return False
  elementDict = result['Value']
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

def getSiteForResource( resourceType, resourceName ):
  """ Get the site name for the given resource specified by type and name
  """
  for site in getSites():
    result = Resources().getResources( site, resourceType )
    if not result['OK']:
      continue
    resources = result['Value']
    for resource in resources:
      if resource == resourceName:
        return S_OK( site )
      
  return S_ERROR( 'Resource %s of type %s is not found' % ( resourceType, resourceName ) )    

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

    def getElements( self, typePath, community, selectDict={} ):
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
          if not self.__checkElementProperties( elementPath, selectDict ):
            continue
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

    def __checkElementProperties( self, elementPath, selectDict ):
      """ Check the given element for the compliance with the given selection criteria
      """
      if not selectDict:
        return True
      result = gConfig.getOptionsDict( elementPath )
      if not result['OK']:
        return False
      elementDict = result['Value']
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

  def __execute( self, *params, **kwargs ): 
    """ internal redirector to the appropriate CS sections
    """

    name = self.call
    resourceType = None
    accessType = None
    siteType = None

    args = list( params )
    args.reverse()

    for op in ['OptionsDict','Option','Value','Elements','s']:
      if name.endswith( op ):
        opType  = op
        break
    if opType == 's':
      opType = "Elements"    
    method = getattr( self.__innerResources, 'get'+opType )

    sitePath = None
    if len( args ) > 0:
      site = args.pop()
    else:
      site = 'Unknown.ch'  
    result = getSitePath( site )
    if not result['OK']:
      return result
    sitePath = result['Value']

    if "Site" in name:
      siteType = True
    elif "Resource" in name:
      resourceType = args.pop()
    elif "Node" in name:
      resourceType = args.pop()
      accessType = RESOURCE_ACCESS_MAPPING[resourceType]    
    else:  
      for rType in RESOURCE_ACCESS_MAPPING:
        if name.startswith( 'get'+rType ):
          resourceType = rType
        aType = RESOURCE_ACCESS_MAPPING[rType]
        if aType and name.startswith( 'get'+aType ):
          accessType = aType
          for rType in RESOURCE_ACCESS_MAPPING:
            if aType == RESOURCE_ACCESS_MAPPING[rType]:
              resourceType = rType

    optionVOPath = None
    optionPath = None 
    if siteType is not None:
      optionPath = sitePath
      typePath = os.path.dirname( sitePath )
      if self.__vo is not None:
        optionVOPath = cfgPath( sitePath, self.__vo )
    elif accessType is not None:
      resource = args.pop()
      typePath = cfgPath( sitePath, resourceType, resource, accessType+'s' ) 
      if len( args ) > 0:
        node = args.pop()
        optionPath = cfgPath( sitePath, resourceType, resource, accessType+'s', node )        
        if self.__vo is not None:
          optionVOPath = cfgPath( optionPath, self.__vo )
    elif resourceType is not None:
      typePath = cfgPath( sitePath, resourceType )
      if len( args ) > 0:
        resource = args.pop()
        optionPath = cfgPath( sitePath, resourceType, resource )
        if self.__vo is not None:
          optionVOPath = cfgPath( optionPath, self.__vo )

    if opType == 'Option':
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
      return method( typePath, self.__vo, selectDict )           
    else:
      return method( optionPath, optionVOPath )
      
  def __getattr__( self, name ):
    self.call = name 
    return self.__execute    

  def getEligibleResources( self, resourceType, selectDict={} ):
    """ Get all the resources eligible according to the selection criteria
    """
    result = self.getSites()
    if not result['OK']:
      return result
    sites = result['Value']
    
    resultDict = {}

    for site in sites:
      result = self.getResources( site, resourceType, selectDict )
      if not result['OK']:
        continue
      if result['Value']:  
        resultDict[site] = result['Value']

    return S_OK( resultDict ) 

  def getEligibleNodes( self, nodeType, resourceSelectDict={}, apSelectDict={} ):
    """ Get all the Access Points eligible according to the selection criteria
    """

    resourceType = None
    for rType in RESOURCE_ACCESS_MAPPING:
      if RESOURCE_ACCESS_MAPPING[rType] == nodeType:
        resourceType = rType
    if resourceType is None:
      return S_ERROR( 'Invalid Access Point type %s' % nodeType ) 

    result = self.getSites()
    if not result['OK']:
      return result
    sites = result['Value']
    
    resultDict = {}

    for site in sites:
      result = self.getResources( site, resourceType, resourceSelectDict )
      if not result['OK']:
        continue
      for resource in result['Value']:
        result = self.getNodes( site, resourceType, resource, nodeType, apSelectDict )
        if not result['OK']:
          continue
        if result['Value']:  
          resultDict.setdefault( site, {} )
          resultDict[site][resource] = result['Value']

    return S_OK( resultDict )
  
  

  ####################################################################################
  #
  #  Specialized tools for particular resources

  def getEligibleStorageElements( self, selectDict={} ):
    """ Get all the eligible Storage Elements according to the selection criteria with
        the names following the SE convention.
    """
    result = self.getEligibleResources( 'Storage', selectDict )
    if not result['OK']:
      return result
    seDict = result['Value']

    seList = []
    for site in seDict:
      for se in seDict[site]:
        seList.append( site[:-3] + '-' + se )

    return S_OK( seList )    

  def getStorageElementOptionsDict( self, seName ):
    """ Get the CS StorageElementOptions
    """
    # Construct the SE path first
    ind = seName.rfind('-')
    if ind == -1:
      return S_ERROR( 'Invalid SE name %s' % seName )
    seElement = seName[ind+1:]
    siteName = seName[:ind]
    result = getSiteName( siteName )
    if not result['OK']:
      return result
    site = result['Value']  

    storageConfigPath = cfgPath( gBaseResourcesSection, site, 'Storage', seElement )
    result = self.getStorageOptionsDict( site, seElement )
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

    return S_OK( options )

  def getQueueDescription( self, site, ce, queue ):
    """ Get parameters of the specified queue 
    """
    result = self.getComputingElementOptionsDict( site, ce )
    if not result['OK']:
      return result 
    resultDict = result['Value']  
    result = self.getQueueOptionsDict( site, ce, queue )
    if not result['OK']:
      return result  
    resultDict.update( result['Value'] )
    resultDict['Queue'] = queue
    
    return S_OK( resultDict )

  def getEligibleQueues( self, siteList = None, ceList = None, ceTypeList = None, mode = None ):
    """ Get CE/queue options according to the specified selection
    """
   
    ceSelectDict = {}
    if ceList is not None:
      ceSelectDict['CEType'] = ceTypeList
    if mode is not None:
      ceSelectDict['SubmissionMode'] = mode 

    result = self.getEligibleNodes( 'Queue', resourceSelectDict=ceSelectDict )
    if not result['OK']:
      return result

    siteDict = result['Value']
    resultDict = {}
    for site in siteDict:
      for ce in siteDict[site]:
        result = self.getComputingOptionsDict( site, ce )
        if not result['OK']:
          return result
        ceDict = result['Value']  
        for queue in siteDict[site][ce]:
          result = self.getQueueOptionsDict( site, ce, queue ) 
          if not result['OK']:
            return result
          queueDict = result['Value']   

          resultDict.setdefault( site, {} )
          resultDict[site].setdefault( ce, ceDict )
          resultDict[site][ce].setdefault( 'Queues', {} )
          resultDict[site][ce]['Queues'][queue] = queueDict
   
    return S_OK( resultDict )

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
    
  platformDict = {}
  result = gConfig.getOptionsDict( '/Resources/Computing/OSCompatibility' )
  if result['OK'] and result['Value']:
    platformDict = result['Value'] 
    for platform in platformDict:
      platformDict[platform] = [ x.strip() for x in platformDict[platform].split( ',' ) ]
  else:
    return S_ERROR( 'OS compatibility info not found' )        

  resultList = list( platforms )
  for p in platforms:
    tmpList = platformDict.get( p, [] )
    for pp in platformDict:
      if p in platformDict[pp]:
        tmpList.append( pp )
        tmpList += platformDict[pp]
    if tmpList:
      resultList += tmpList

  return S_OK( uniqueElements( resultList ) )

def getDIRACPlatform( platform ):
  """ Get standard DIRAC platform compatible with the argument
  """
  platformDict = {}
  result = gConfig.getOptionsDict( '/Resources/Computing/OSCompatibility' )
  if result['OK'] and result['Value']:
    platformDict = result['Value'] 
    for p in platformDict:
      if p == platform:
        return S_OK( platform ) 
      platformDict[p] = [ x.strip() for x in platformDict[p].split( ',' ) ]
  else:
    return S_ERROR( 'OS compatibility info not found' )    
      
  for p in platformDict:
    if platform in platformDict[p]:
      return S_OK( p )
  
  return S_ERROR( 'No compatible DIRAC platform found for %s' % platform )
  
def getCatalogPath( catalogName ):
  """  Return the configuration path of the description for a a given catalog
  """
  return '/Resources/FileCatalogs/%s' % catalogName
