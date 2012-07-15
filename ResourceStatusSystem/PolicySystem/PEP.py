# $HeadURL $
''' PEP

  Module used for enforcing policies. Its class is used for:
    1. invoke a PDP and collects results
    2. enforcing results by:
       a. saving result on a DB
       b. raising alarms
       c. other....
'''

from DIRAC                                                       import S_ERROR
from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient      import ResourceStatusClient
from DIRAC.ResourceStatusSystem.Client.ResourceManagementClient  import ResourceManagementClient
from DIRAC.ResourceStatusSystem.PolicySystem.Actions.EmptyAction import EmptyAction
from DIRAC.ResourceStatusSystem.PolicySystem.PDP                 import PDP
from DIRAC.ResourceStatusSystem.Utilities                        import RssConfiguration, Utils

__RCSID__  = '$Id: $'

class PEP:
  '''
  PEP (Policy Enforcement Point) initialization

  :params:
    :attr:`granularity`       : string - a ValidElement (optional)
    :attr:`name`              : string - optional name (e.g. of a site)
    :attr:`status`            : string - optional status
    :attr:`formerStatus`      : string - optional former status
    :attr:`reason`            : string - optional reason for last status change
    :attr:`siteType`          : string - optional site type
    :attr:`serviceType`       : string - optional service type
    :attr:`resourceType`      : string - optional resource type
    :attr:`futureEnforcement` :          optional
      [
        {
          'PolicyType': a PolicyType
          'Granularity': a ValidElement (optional)
        }
      ]
  '''

  def __init__( self, pdp = None, clients = None ):
    '''
    Enforce policies, using a PDP  (Policy Decision Point), based on

     self.__granularity (optional)
     self.__name (optional)
     self.__status (optional)
     self.__formerStatus (optional)
     self.__reason (optional)
     self.__siteType (optional)
     self.__serviceType (optional)
     self.__realBan (optional)
     self.__user (optional)
     self.__futurePolicyType (optional)
     self.__futureGranularity (optional)

     :params:
       :attr:`pdp`       : a custom PDP object (optional)
       :attr:`clients`   : a dictionary containing modules corresponding to clients.
    '''
    
    if clients is None:
      clients = {}
    
    try:             
      self.rsClient = clients[ 'ResourceStatusClient' ]
    except KeyError: 
      self.rsClient = ResourceStatusClient()
    try:             
      self.rmClient = clients[ 'ResourceManagementClient' ]
    except KeyError: 
      self.rmClient = ResourceManagementClient()

    self.clients = clients
    if not pdp:
      self.pdp = PDP( **clients )

  def enforce( self, granularity = None, name = None, statusType = None,
               status = None, formerStatus = None, reason = None, 
               siteType = None, serviceType = None, resourceType = None, 
               tokenOwner = None, useNewRes = False, knownInfo = None  ):
    '''
      Enforce policies for given set of keyworkds. To be better explained.
    '''
  
    ##  real ban flag  #########################################################

    realBan = False
    if tokenOwner is not None:
      if tokenOwner == 'RS_SVC':
        realBan = True

    ## sanitize input ##########################################################
    ## IS IT REALLY NEEDED ??
    
    validElements = RssConfiguration.getValidElements()    
    if granularity is not None and granularity not in validElements:
      return S_ERROR( 'Granularity "%s" not valid' % granularity )

    validStatusTypes = RssConfiguration.getValidStatusTypes()
    if statusType is not None and statusType not in validStatusTypes[ granularity ]['StatusType']:
      return S_ERROR( 'StatusType "%s" not valid' % statusType )
    
    validStatus = RssConfiguration.getValidStatus()
    if status is not None and status not in validStatus:
      return S_ERROR( 'Status "%s" not valid' % status )

    validStatus = RssConfiguration.getValidStatus()
    if formerStatus is not None and formerStatus not in validStatus:
      return S_ERROR( 'FormerStatus "%s" not valid' % formerStatus )

    validSiteTypes = RssConfiguration.getValidSiteTypes()
    if siteType is not None and siteType not in validSiteTypes:
      return S_ERROR( 'SiteType "%s" not valid' % siteType )

    validServiceTypes = RssConfiguration.getValidServiceTypes()
    if serviceType is not None and serviceType not in validServiceTypes:
      return S_ERROR( 'ServiceType "%s" not valid' % serviceType )

    validResourceTypes = RssConfiguration.getValidResourceTypes() 
    if resourceType is not None and resourceType not in validResourceTypes:
      return S_ERROR( 'ResourceType "%s" not valid' % resourceType )
    
    ## policy setup ############################################################  

    self.pdp.setup( granularity = granularity, name = name, 
                    statusType = statusType, status = status,
                    formerStatus = formerStatus, reason = reason, 
                    siteType = siteType, serviceType = serviceType, 
                    resourceType = resourceType, useNewRes = useNewRes )

    ## policy decision #########################################################

    resDecisions = self.pdp.takeDecision( knownInfo = knownInfo )

    ## record all results before doing anything else    
    for resP in resDecisions[ 'SinglePolicyResults' ]:
      
      if not resP.has_key( 'OLD' ):       
        self.clients[ "rmClient" ].insertPolicyResultLog( granularity, name,
                                                          resP[ 'PolicyName' ], 
                                                          statusType,
                                                          resP[ 'Status' ], 
                                                          resP[ 'Reason' ], now )
        
      else:
        gLogger.warn( 'OLD: %s' % resP )
        
    res          = resDecisions[ 'PolicyCombinedResult' ] 
    actionBaseMod = "DIRAC.ResourceStatusSystem.PolicySystem.Actions"

    # Security mechanism in case there is no PolicyType returned
    if res == {}:
      EmptyAction(granularity, name, statusType, resDecisions).run()

    else:
      policyType   = res[ 'PolicyType' ]

      if 'Resource_PolType' in policyType:
        action = Utils.voimport( '%s.ResourceAction' % actionBaseMod )
        action.ResourceAction(granularity, name, statusType, resDecisions,
                         rsClient=self.rsClient,
                         rmClient=self.rmClient).run()

      if 'Alarm_PolType' in policyType:
        action = Utils.voimport( '%s.AlarmAction' % actionBaseMod )
        action.AlarmAction(granularity, name, statusType, resDecisions,
                       Clients=self.clients,
                       Params={"Granularity"  : granularity,
                               "SiteType"     : siteType,
                               "ServiceType"  : serviceType,
                               "ResourceType" : resourceType}).run()

      if 'RealBan_PolType' in policyType and realBan:
        action = Utils.voimport( '%s.RealBanAction' % actionBaseMod )
        action.RealBanAction(granularity, name, resDecisions).run()

    return resDecisions

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF