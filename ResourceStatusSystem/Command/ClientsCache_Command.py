################################################################################
# $HeadURL $
################################################################################
__RCSID__ = "$Id:  $"

""" 
  The ClientsCache_Command class is a command module to know about collective clients results 
  (to be cached)
"""

import datetime

from DIRAC                                        import gLogger
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

    if sites is None:
      sites = self.APIs[ 'ResourceStatusClient' ].getSite( columns = 'SiteName' )
      if not sites['OK']:
        raise RSSException, where( self, self.doCommand ) + " " + sites['Message']
      else:
        sites = [ si[0] for si in sites['Value'] ]

    try:
      res = self.APIs[ 'JobsClient' ].getJobsSimpleEff( sites, self.APIs[ 'WMSAdministrator' ] )
      if res is None:
        res = []
    except:
      gLogger.exception( "Exception when calling JobsClient." )
      return {}

    resToReturn = {}

    for site in res:
      resToReturn[site] = {'JE_S': res[site]}

    return resToReturn

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

    if sites is None:
      sites = self.APIs[ 'ResourceStatusClient' ].getSite( columns = 'SiteName')
      if not sites['OK']:
        raise RSSException, where( self, self.doCommand ) + " " + sites['Message']
      else:
        sites = [ si[0] for si in sites['Value'] ]

    try:
      res = self.APIs[ 'PilotsClient' ].getPilotsSimpleEff( 'Site', sites, None, self.APIs[ 'WMSAdministrator' ] )
      if res is None:
        res = []
    except:
      gLogger.exception( "Exception when calling PilotsClient." )
      return {}

    resToReturn = {}

    for site in res:
      resToReturn[site] = {'PE_S': res[site]}

    return resToReturn

  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__

################################################################################
################################################################################

class TransferQualityEverySEs_Command( Command ):

  __APIs__ = [ 'ResourceStatusClient', 'ReportsClient' ]

  def doCommand( self, SEs = None ):
    """ 
    Returns transfer quality using the DIRAC accounting system for every SE 
        
    :params:
      :attr:`SEs`: list of storage elements (when not given, take every SE)
    
    :returns:
      {'SiteName': {TQ : 'Good'|'Fair'|'Poor'|'Idle'|'Bad'} ...}
    """

    self.APIs = initAPIs( self.__APIs__, self.APIs )

    if SEs is None:
      SEs = self.APIs[ 'ResourceStatusClient' ].getStorageElement( columns = 'StorageElementName')
      if not SEs['OK']:
        raise RSSException, where( self, self.doCommand ) + " " + SEs['Message']
      else:
        SEs = SEs['Value']

    self.APIs[ 'ReportsClient' ].rpcClient = self.APIs[ 'ReportGenerator' ]

    fromD = datetime.datetime.utcnow() - datetime.timedelta( hours = 2 )
    toD = datetime.datetime.utcnow()

    try:
      qualityAll = self.APIs[ 'ReportsClient' ].getReport( 'DataOperation', 'Quality', fromD, toD,
                                         {'OperationType':'putAndRegister',
                                          'Destination':SEs}, 'Channel' )
      if not qualityAll['OK']:
        raise RSSException, where( self, self.doCommand ) + " " + qualityAll['Message']
      else:
        qualityAll = qualityAll['Value']['data']

    except:
      gLogger.exception( "Exception when calling TransferQualityEverySEs_Command" )
      return {}

    listOfDestSEs = []

    for k in qualityAll.keys():
      try:
        key = k.split( ' -> ' )[1]
        if key not in listOfDestSEs:
          listOfDestSEs.append( key )
      except:
        continue

    meanQuality = {}

    for destSE in listOfDestSEs:
      s = 0
      n = 0
      for k in qualityAll.keys():
        try:
          if k.split( ' -> ' )[1] == destSE:
            n = n + len( qualityAll[k] )
            s = s + sum( qualityAll[k].values() )
        except:
          continue
      meanQuality[destSE] = s / n

    resToReturn = {}

    for se in meanQuality:
      resToReturn[se] = {'TQ': meanQuality[se]}

    return resToReturn


  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__

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

    if sites is None:
      GOC_sites = self.APIs[ 'ResourceStatusClient' ].getGridSite( columns = 'GridSiteName' )
      if not GOC_sites['OK']:
        raise RSSException, where( self, self.doCommand ) + " " + sites['Message']
      else:
        GOC_sites = [ gs[0] for gs in GOC_sites['Value'] ]
    else:
      GOC_sites = [ getGOCSiteName( x )['Value'] for x in sites ]

    try:
      res = self.APIs[ 'GOCDBClient' ].getStatus( 'Site', GOC_sites, None, 120 )
    except:
      gLogger.exception( "Exception when calling GOCDBClient." )
      return {}

    if not res['OK']:
      raise RSSException, where( self, self.doCommand ) + " " + res['Message']
    else:
      res = res['Value']

    if res == None:
      return {}

    resToReturn = {}

    for dt_ID in res:
      try:
        dt                = {}
        dt['ID']          = dt_ID
        dt['StartDate']   = res[dt_ID]['FORMATED_START_DATE']
        dt['EndDate']     = res[dt_ID]['FORMATED_END_DATE']
        dt['Severity']    = res[dt_ID]['SEVERITY']
        dt['Description'] = res[dt_ID]['DESCRIPTION'].replace( '\'', '' )
        dt['Link']        = res[dt_ID]['GOCDB_PORTAL_URL']
        
        DIRACnames = getDIRACSiteName( res[dt_ID]['SITENAME'] )
        if not DIRACnames['OK']:
          raise RSSException, DIRACnames['Message']
        DIRACnames = DIRACnames['Value']
        for DIRACname in DIRACnames:
          resToReturn[dt_ID.split()[0] + ' ' + DIRACname] = dt
      except KeyError:
        continue

    return resToReturn

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

    if resources is None:
      resources = self.APIs[ 'ResourceStatusClient' ].getResource( columns = 'ResourceName' )
      if not resources['OK']:
        raise RSSException, where( self, self.doCommand ) + " " + resources['Message']
      else:
        resources = [ re[0] for re in resources['Value'] ]

    try:
      res = self.APIs[ 'GOCDBClient' ].getStatus( 'Resource', resources, None, 120 )
    except:
      gLogger.exception( "Exception when calling GOCDBClient." )
      return {}

    if not res['OK']:
      raise RSSException, where( self, self.doCommand ) + " " + res['Message']
    else:
      res = res['Value']

    if res == None:
      return {}

    resToReturn = {}

    for dt_ID in res:
      dt                 = {}
      dt['ID']           = dt_ID
      dt['StartDate']    = res[dt_ID]['FORMATED_START_DATE']
      dt['EndDate']      = res[dt_ID]['FORMATED_END_DATE']
      dt['Severity']     = res[dt_ID]['SEVERITY']
      dt['Description']  = res[dt_ID]['DESCRIPTION'].replace( '\'', '' )
      dt['Link']         = res[dt_ID]['GOCDB_PORTAL_URL']
      resToReturn[dt_ID] = dt

    return resToReturn

  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__

################################################################################
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #  
################################################################################

'''
  HOW DOES THIS WORK.
    
    will come soon...
'''

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF