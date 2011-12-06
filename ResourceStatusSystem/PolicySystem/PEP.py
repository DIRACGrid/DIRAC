################################################################################
# $HeadURL $
################################################################################
"""
  Module used for enforcing policies. Its class is used for:
    1. invoke a PDP and collects results
    2. enforcing results by:
       a. saving result on a DB
       b. raising alarms
       c. other....
"""
from DIRAC.ResourceStatusSystem.Utilities                          import Utils

from DIRAC.ResourceStatusSystem                                    import ValidRes, ValidStatus, ValidStatusTypes, \
    ValidSiteType, ValidServiceType, ValidResourceType

from DIRAC.ResourceStatusSystem.Utilities.Exceptions               import InvalidRes, InvalidStatus, \
    InvalidResourceType, InvalidServiceType, InvalidSiteType

from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient        import ResourceStatusClient
from DIRAC.ResourceStatusSystem.Client.ResourceManagementClient    import ResourceManagementClient

from DIRAC.ResourceStatusSystem.PolicySystem.Actions.Empty_PolType import EmptyPolTypeActions

from DIRAC.ResourceStatusSystem.PolicySystem.PDP                   import PDP

class PEP:
  """
  PEP (Policy Enforcement Point) initialization

  :params:
    :attr:`granularity`       : string - a ValidRes (optional)
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
          'Granularity': a ValidRes (optional)
        }
      ]
  """

  def __init__( self, pdp = None, clients = {} ):
    """
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
    """
    try:
      self.rsAPI = clients[ 'ResourceStatusClient' ]
    except ValueError:
      self.rsAPI = ResourceStatusClient()
    try:
      self.rmAPI = clients[ 'ResourceManagementClient' ]
    except ValueError:
      self.rmAPI = ResourceManagementClient()

    if pdp is None:
      self.pdp = PDP( **clients )

################################################################################

  def enforce( self, granularity = None, name = None, statusType = None,
                status = None, formerStatus = None, reason = None, siteType = None,
                serviceType = None, resourceType = None, tokenOwner = None,
                useNewRes = False, knownInfo = None  ):

    ###################
    #  real ban flag  #
    ###################

    realBan = False
    if tokenOwner is not None:
      if tokenOwner == 'RS_SVC':
        realBan = True

    ################
    # policy setup #
    ################

    granularity = Utils.assignOrRaise( granularity, ValidRes, InvalidRes, self, self.__init__ )
    try:
      statusType   = Utils.assignOrRaise( statusType, ValidStatusTypes[ granularity ]['StatusType'], \
                                            InvalidStatus, self, self.enforce )
    except KeyError:
      statusType = "''" # "strange" default value returned by CS

    status       = Utils.assignOrRaise( status, ValidStatus, InvalidStatus, self, self.enforce )
    formerStatus = Utils.assignOrRaise( formerStatus, ValidStatus, InvalidStatus, self, self.enforce )
    siteType     = Utils.assignOrRaise( siteType, ValidSiteType, InvalidSiteType, self, self.enforce )
    serviceType  = Utils.assignOrRaise( serviceType, ValidServiceType, InvalidServiceType, self, self.enforce )
    resourceType = Utils.assignOrRaise( resourceType, ValidResourceType, InvalidResourceType, self, self.enforce )

    self.pdp.setup(granularity = granularity, name = name,
                    statusType = statusType, status = status,
                    formerStatus = formerStatus, reason = reason, siteType = siteType,
                    serviceType = serviceType, resourceType = resourceType,
                    useNewRes = useNewRes )

    ###################
    # policy decision #
    ###################

    resDecisions = self.pdp.takeDecision( knownInfo = knownInfo )

    res          = resDecisions[ 'PolicyCombinedResult' ]
    actionBaseMod = "DIRAC.ResourceStatusSystem.PolicySystem.Actions"

    # Security mechanism in case there is no PolicyType returned
    if res == {}:
      EmptyPolTypeActions( granularity, name, resDecisions, res )

    else:
      policyType   = res[ 'PolicyType' ]

      if 'Resource_PolType' in policyType:
        m = Utils.voimport(actionBaseMod + ".Resource_PolType")
        m.ResourcePolTypeActions( granularity, name, statusType,
                                  resDecisions, self.rsAPI, self.rmAPI )

      if 'Alarm_PolType' in policyType:
        m = Utils.voimport(actionBaseMod + ".Alarm_PolType")

        m.AlarmPolType(name, res, statusType, clients,
                       Granularity = granularity,
                       SiteType = siteType,
                       ServiceType = serviceType,
                       ResourceType = resourceType)


      if 'RealBan_PolType' in policyType and realBan:
        m = Utils.voimport(actionBaseMod + ".RealBan_PolType")
        m.RealBanPolTypeActions(granularity, name, res)

    return resDecisions

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
