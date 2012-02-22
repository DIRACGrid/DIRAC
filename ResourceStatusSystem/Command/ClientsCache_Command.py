################################################################################
# $HeadURL $
################################################################################
__RCSID__ = "$Id:  $"

""" 
  The ClientsCache_Command class is a command module to know about collective clients results 
  (to be cached)
"""

import datetime

from DIRAC                                        import gLogger, S_OK, S_ERROR
from DIRAC.Core.Utilities.SitesDIRACGOCDBmapping  import getGOCSiteName, getDIRACSiteName

from DIRAC.ResourceStatusSystem.Command.Command   import *
from DIRAC.ResourceStatusSystem.Command.knownAPIs import initAPIs
from DIRAC.ResourceStatusSystem.Utilities.Utils   import where

################################################################################
################################################################################

class JobsEffSimpleEveryOne_Command( Command ):

  __APIs__ = [ 'ResourceStatusClient', 'JobsClient', 'WMSAdministrator' ]

  def doCommand( self, sites = None ):
    """ 
    Returns simple jobs efficiency for all the sites in input.
        
    :params:
      :attr:`sites`: list of site names (when not given, take every site)
    
    :returns:
      {'SiteName': {'JE_S': 'Good'|'Fair'|'Poor'|'Idle'|'Bad'}, ...}
    """

    self.APIs = initAPIs( self.__APIs__, self.APIs )

    try:

      if sites is None:
        sites = self.APIs[ 'ResourceStatusClient' ].getSite( meta = { 'columns' : 'SiteName' } )
        
        if not sites['OK']:
          return { 'Result' : sites }
         
        sites = [ si[ 0 ] for si in sites[ 'Value' ] ]

      res = self.APIs[ 'JobsClient' ].getJobsSimpleEff( sites, self.APIs[ 'WMSAdministrator' ] )
      if res is None:
        res = []

      resToReturn = {}
      for site in res:
        resToReturn[ site ] = { 'JE_S' : res[ site ] }

      res = S_OK( resToReturn )

    except Exception, e:
      _msg = '%s (%s): %s' % ( self.__class__.__name__, self.args, e )
      gLogger.exception( _msg )
      return { 'Result' : S_ERROR( _msg ) }

    return { 'Result' : res } 

  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__

################################################################################
################################################################################

class PilotsEffSimpleEverySites_Command( Command ):

  __APIs__ = [ 'ResourceStatusClient', 'PilotsClient', 'WMSAdministrator' ]

  def doCommand( self, sites = None ):
    """ 
    Returns simple pilots efficiency for all the sites and resources in input.
        
    :params:
      :attr:`sites`: list of site names (when not given, take every site)
    
    :returns:
      {'SiteName':  {'PE_S': 'Good'|'Fair'|'Poor'|'Idle'|'Bad'} ...}
    """

    self.APIs = initAPIs( self.__APIs__, self.APIs )

    try:

      if sites is None:
        sites = self.APIs[ 'ResourceStatusClient' ].getSite( meta = { 'columns' : 'SiteName' })
        if not sites['OK']:
          return { 'Result' : sites }
        sites = [ si[ 0 ] for si in sites[ 'Value' ] ]

      res = self.APIs[ 'PilotsClient' ].getPilotsSimpleEff( 'Site', sites, None, self.APIs[ 'WMSAdministrator' ] )
      if res is None:
        res = []

      resToReturn = {}

      for site in res:
        resToReturn[site] = { 'PE_S' : res[ site ] }

      res = S_OK( resToReturn )

    except Exception, e:
      _msg = '%s (%s): %s' % ( self.__class__.__name__, self.args, e )
      gLogger.exception( _msg )
      return { 'Result' : S_ERROR( _msg ) }

    return { 'Result' : res } 

  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__

################################################################################
################################################################################
#
#class TransferQualityEverySEs_Command( Command ):
#
#  __APIs__ = [ 'ResourceStatusClient', 'ReportsClient' ]
#
#  def doCommand( self, SEs = None ):
#    """ 
#    Returns transfer quality using the DIRAC accounting system for every SE 
#        
#    :params:
#      :attr:`SEs`: list of storage elements (when not given, take every SE)
#    
#    :returns:
#      {'SiteName': {TQ : 'Good'|'Fair'|'Poor'|'Idle'|'Bad'} ...}
#    """
#
#    self.APIs = initAPIs( self.__APIs__, self.APIs )
#
#    if SEs is None:
#      SEs = self.APIs[ 'ResourceStatusClient' ].getStorageElement( meta = {'columns' : 'StorageElementName' })
#      if not SEs['OK']:
#      else:
#        SEs = SEs['Value']
#
#    self.APIs[ 'ReportsClient' ].rpcClient = self.APIs[ 'ReportGenerator' ]
#
#    fromD = datetime.datetime.utcnow() - datetime.timedelta( hours = 2 )
#    toD = datetime.datetime.utcnow()
#
#    try:
#      qualityAll = self.APIs[ 'ReportsClient' ].getReport( 'DataOperation', 'Quality', fromD, toD,
#                                         {'OperationType':'putAndRegister',
#                                          'Destination':SEs}, 'Channel' )
#      if not qualityAll['OK']:
#      else:
#        qualityAll = qualityAll['Value']['data']
#
#    except:
#      gLogger.exception( "Exception when calling TransferQualityEverySEs_Command" )
#      return {}
#
#    listOfDestSEs = []
#
#    for k in qualityAll.keys():
#      try:
#        key = k.split( ' -> ' )[1]
#        if key not in listOfDestSEs:
#          listOfDestSEs.append( key )
#      except:
#        continue
#
#    meanQuality = {}
#
#    for destSE in listOfDestSEs:
#      s = 0
#      n = 0
#      for k in qualityAll.keys():
#        try:
#          if k.split( ' -> ' )[1] == destSE:
#            n = n + len( qualityAll[k] )
#            s = s + sum( qualityAll[k].values() )
#        except:
#          continue
#      meanQuality[destSE] = s / n
#
#    resToReturn = {}
#
#    for se in meanQuality:
#      resToReturn[se] = {'TQ': meanQuality[se]}
#
#    return resToReturn
#
#
#  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__

################################################################################
################################################################################

class DTEverySites_Command( Command ):

  __APIs__ = [ 'ResourceStatusClient', 'GOCDBClient' ]

  def doCommand( self, sites = None ):
    """ 
    Returns downtimes information for all the sites in input.
        
    :params:
      :attr:`sites`: list of site names (when not given, take every site)
    
    :returns:
      {'SiteName': {'SEVERITY': 'OUTAGE'|'AT_RISK', 
                    'StartDate': 'aDate', ...} ... }
    """

    self.APIs = initAPIs( self.__APIs__, self.APIs )

    try:
      
      if sites is None:
        GOC_sites = self.APIs[ 'ResourceStatusClient' ].getGridSite( meta = { 'columns' : 'GridSiteName' })
        if not GOC_sites['OK']:
          return { 'Result' : GOC_sites }
        GOC_sites = [ gs[0] for gs in GOC_sites['Value'] ]
      else:
        GOC_sites = [ getGOCSiteName( x )['Value'] for x in sites ]

      resGOC = self.APIs[ 'GOCDBClient' ].getStatus( 'Site', GOC_sites, None, 120 )

      if not resGOC['OK']:
        return { 'Result' : resGOC }
      
      resGOC = resGOC['Value']

      if resGOC == None:
        resGOC = []

      res = {}

      for dt_ID in resGOC:
        
        try:
          
          dt                = {}
          dt['ID']          = dt_ID
          dt['StartDate']   = resGOC[dt_ID]['FORMATED_START_DATE']
          dt['EndDate']     = resGOC[dt_ID]['FORMATED_END_DATE']
          dt['Severity']    = resGOC[dt_ID]['SEVERITY']
          dt['Description'] = resGOC[dt_ID]['DESCRIPTION'].replace( '\'', '' )
          dt['Link']        = resGOC[dt_ID]['GOCDB_PORTAL_URL']
        
          DIRACnames = getDIRACSiteName( res[dt_ID]['SITENAME'] )
          
          if not DIRACnames['OK']:
            return { 'Result' : DIRACnames }
          
          for DIRACname in DIRACnames['Value']:
            res[dt_ID.split()[0] + ' ' + DIRACname] = dt
            
        except KeyError:
          continue

      res = S_OK( res )        

    except Exception, e:
      _msg = '%s (%s): %s' % ( self.__class__.__name__, self.args, e )
      gLogger.exception( _msg )
      return { 'Result' : S_ERROR( _msg ) }

    return { 'Result' : res } 

  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__

################################################################################
################################################################################

class DTEveryResources_Command( Command ):

  __APIs__ = [ 'ResourceStatusClient', 'GOCDBClient' ]

  def doCommand( self, resources = None ):
    """ 
    Returns downtimes information for all the resources in input.
        
    :params:
      :attr:`sites`: list of resource names (when not given, take every resource)
    
    :returns:
      {'ResourceName': {'SEVERITY': 'OUTAGE'|'AT_RISK', 
                    'StartDate': 'aDate', ...} ... }
    """

    self.APIs = initAPIs( self.__APIs__, self.APIs )

    try:

      if resources is None:
        meta = { 'columns' : 'ResourceName' }
        resources = self.APIs[ 'ResourceStatusClient' ].getResource( meta = meta )
        if not resources['OK']:
          return { 'Result' : resources }
        resources = [ re[0] for re in resources['Value'] ]

      resGOC = self.APIs[ 'GOCDBClient' ].getStatus( 'Resource', resources, None, 120 )
    
      if not resGOC['OK']:
        return { 'Result' : resGOC }
    
      resGOC = resGOC['Value']

      if resGOC == None:
        resGOC = []

      res = {}

      for dt_ID in resGOC:
        dt                 = {}
        dt['ID']           = dt_ID
        dt['StartDate']    = resGOC[dt_ID]['FORMATED_START_DATE']
        dt['EndDate']      = resGOC[dt_ID]['FORMATED_END_DATE']
        dt['Severity']     = resGOC[dt_ID]['SEVERITY']
        dt['Description']  = resGOC[dt_ID]['DESCRIPTION'].replace( '\'', '' )
        dt['Link']         = resGOC[dt_ID]['GOCDB_PORTAL_URL']
        res[dt_ID] = dt

      res = S_OK( res )

    except Exception, e:
      _msg = '%s (%s): %s' % ( self.__class__.__name__, self.args, e )
      gLogger.exception( _msg )
      return { 'Result' : S_ERROR( _msg ) }

    return { 'Result' : res } 

  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF