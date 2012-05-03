########################################################################
# $HeadURL$
# File :   InputDataResolution.py
# Author : Stuart Paterson
########################################################################

""" The input data resolution module is a plugin that
    allows to define VO input data policy in a simple way using existing
    utilities in DIRAC or extension code supplied by the VO.

    The arguments dictionary from the Job Wrapper includes the file catalogue
    result and in principle has all the necessary information to resolve input data
    for applications.

"""

__RCSID__ = "$Id$"

from DIRAC.Core.Utilities.ModuleFactory                             import ModuleFactory
from DIRAC.WorkloadManagementSystem.Client.PoolXMLSlice             import PoolXMLSlice
from DIRAC                                                          import S_OK, S_ERROR, gLogger
from DIRAC.ConfigurationSystem.Client.Helpers.Operations            import Operations

import DIRAC

import types

COMPONENT_NAME = 'InputDataResolution'

class InputDataResolution:
  """ The class
  """

  #############################################################################
  def __init__( self, argumentsDict ):
    """ Standard constructor
    """
    self.arguments = argumentsDict
    # By default put input data into the current directory
    if not self.arguments.has_key( 'InputDataDirectory' ):
      self.arguments['InputDataDirectory'] = 'CWD'
    self.name = COMPONENT_NAME
    self.log = gLogger.getSubLogger( self.name )

  #############################################################################
  def execute( self ):
    """Given the arguments from the Job Wrapper, this function calls existing
       utilities in DIRAC to resolve input data according to LHCb VO policy.
    """
    result = self.__resolveInputData()
    if not result['OK']:
      self.log.error( 'InputData resolution failed with result:\n%s' % ( result ) )

    #For local running of this module we can expose an option to ignore missing files
    ignoreMissing = False
    if self.arguments.has_key( 'IgnoreMissing' ):
      ignoreMissing = self.arguments['IgnoreMissing']

    # Missing some of the input files is a fatal error unless ignoreMissing option is defined
    if result.has_key( 'Failed' ):
      failedReplicas = result['Failed']
      if failedReplicas and not ignoreMissing:
        self.log.error( 'Failed to obtain access to the following files:\n%s'
                        % ( '\n'.join( failedReplicas ) ) )
        return S_ERROR( 'Failed to access all of requested input data' )

    if not result.has_key( 'Successful' ):
      return result

    if not result['Successful']:
      return S_ERROR( 'Could not access any requested input data' )

    #TODO: Must define file types in order to pass to POOL XML catalogue.  In the longer
    #term this will be derived from file catalog metadata information but for now is based
    #on the file extension types.
    resolvedData = result['Successful']
    tmpDict = {}
    for lfn, mdata in resolvedData.items():
      tmpDict[lfn] = mdata
      tmpDict[lfn]['pfntype'] = 'ROOT_All'
      self.log.verbose( 'Adding PFN file type %s for LFN:%s' % ( 'ROOT_All', lfn ) )

    resolvedData = tmpDict
    catalogName = 'pool_xml_catalog.xml'
    if self.arguments['Configuration'].has_key( 'CatalogName' ):
      catalogName = self.arguments['Configuration']['CatalogName']

    self.log.verbose( 'Catalog name will be: %s' % catalogName )
    resolvedData = tmpDict
    appCatalog = PoolXMLSlice( catalogName )
    check = appCatalog.execute( resolvedData )
    if not check['OK']:
      return check
    return result

  #############################################################################
  def __resolveInputData( self ):
    """This method controls the execution of the DIRAC input data modules according
       to the VO policy defined in the configuration service.
    """
    if self.arguments['Configuration'].has_key( 'SiteName' ):
      site = self.arguments['Configuration']['SiteName']
    else:
      site = DIRAC.siteName()

    policy = []
    if not self.arguments.has_key( 'Job' ):
      self.arguments['Job'] = {}

    if self.arguments['Job'].has_key( 'InputDataPolicy' ):
      policy = self.arguments['Job']['InputDataPolicy']
      #In principle this can be a list of modules with the first taking precedence
      if type( policy ) in types.StringTypes:
        policy = [policy]
      self.log.info( 'Job has a specific policy setting: %s' % ( ', '.join( policy ) ) )
    else:
      self.log.verbose( 'Attempting to resolve input data policy for site %s' % site )
      inputDataPolicy = Operations().getOptionsDict( 'InputDataPolicy' )
      if not inputDataPolicy['OK']:
        return S_ERROR( 'Could not resolve InputDataPolicy from Operations InputDataPolicy' )

      options = inputDataPolicy['Value']
      if options.has_key( site ):
        policy = options[site]
        policy = [x.strip() for x in policy.split( ',' )]
        self.log.info( 'Found specific input data policy for site %s:\n%s' % ( site, '\n'.join( policy ) ) )
      elif options.has_key( 'Default' ):
        policy = options['Default']
        policy = [x.strip() for x in policy.split( ',' )]
        self.log.info( 'Applying default input data policy for site %s:\n%s' % ( site, '\n'.join( policy ) ) )

    dataToResolve = None #if none, all supplied input data is resolved
    allDataResolved = False
    successful = {}
    failedReplicas = []
    for modulePath in policy:
      if not allDataResolved:
        result = self.__runModule( modulePath, dataToResolve )
        if not result['OK']:
          self.log.warn( 'Problem during %s execution' % modulePath )
          return result

        if result.has_key( 'Failed' ):
          failedReplicas = result['Failed']

        if failedReplicas:
          self.log.info( '%s failed for the following files:\n%s'
                         % ( modulePath, '\n'.join( failedReplicas ) ) )
          dataToResolve = failedReplicas
        else:
          self.log.info( 'All replicas resolved after %s execution' % ( modulePath ) )
          allDataResolved = True

        successful.update( result['Successful'] )
        self.log.verbose( successful )

    result = S_OK()
    result['Successful'] = successful
    result['Failed'] = failedReplicas
    return result

  #############################################################################
  def __runModule( self, modulePath, remainingReplicas ):
    """This method provides a way to run the modules specified by the VO that
       govern the input data access policy for the current site. Using the 
       InputDataPolicy section from Operations different modules can be defined for 
       particular sites or for InputDataPolicy defined in the JDL of the jobs.
    """
    self.log.info( 'Attempting to run %s' % ( modulePath ) )
    moduleFactory = ModuleFactory()
    moduleInstance = moduleFactory.getModule( modulePath, self.arguments )
    if not moduleInstance['OK']:
      return moduleInstance

    module = moduleInstance['Value']
    result = module.execute( remainingReplicas )
    return result

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
