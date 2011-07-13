"""
    Module used for enforcing policies. Its class is used for:

    1. invoke a PDP and collects results

    2. enforcing results by:

       a. saving result on a DB

       b. rasing alarms

       c. other....
"""

from DIRAC.ResourceStatusSystem.Utilities import CS
from DIRAC.ResourceStatusSystem.Utilities import Utils

from DIRAC.ResourceStatusSystem.Policy.Configurations import ValidRes, \
    ValidStatus, ValidSiteType, ValidServiceType, ValidResourceType

from DIRAC.ResourceStatusSystem.Utilities.Exceptions import \
    InvalidRes, InvalidStatus, InvalidResourceType, InvalidServiceType, InvalidSiteType

from DIRAC.ResourceStatusSystem.PolicySystem.Actions.Empty_PolType    import EmptyPolTypeActions

class PEP:
#############################################################################
  """
  PEP (Policy Enforcement Point) initialization

  :params:
    :attr:`VOExtension`: string - VO extension (e.g. 'LHCb')

    :attr:`granularity`: string - a ValidRes (optional)

    :attr:`name`: string - optional name (e.g. of a site)

    :attr:`status`: string - optional status

    :attr:`formerStatus`: string - optional former status

    :attr:`reason`: string - optional reason for last status change

    :attr:`siteType`: string - optional site type

    :attr:`serviceType`: string - optional service type

    :attr:`resourceType`: string - optional resource type

    :attr:`futureEnforcement`: optional
      [
        {
          'PolicyType': a PolicyType
          'Granularity': a ValidRes (optional)
        }
      ]

  """

  def __init__( self, VOExtension, granularity = None, name = None, status = None, formerStatus = None,
                reason = None, siteType = None, serviceType = None, resourceType = None,
                tokenOwner = None, useNewRes = False ):

    self.VOExtension = VOExtension

    try:
      self.__granularity = Utils.assignOrRaise( granularity, ValidRes, InvalidRes, self, self.__init__ )
    except NameError:
      pass

    self.__name         = name
    self.__status       = Utils.assignOrRaise( status, ValidStatus, InvalidStatus, self, self.__init__ )
    self.__formerStatus = Utils.assignOrRaise( formerStatus, ValidStatus, InvalidStatus, self, self.__init__ )
    self.__reason       = reason
    self.__siteType     = Utils.assignOrRaise( siteType, ValidSiteType, InvalidSiteType, self, self.__init__ )
    self.__serviceType  = Utils.assignOrRaise( serviceType, ValidServiceType, InvalidServiceType, self, self.__init__ )
    self.__resourceType = Utils.assignOrRaise( resourceType, ValidResourceType, InvalidResourceType, self, self.__init__ )

    self.__realBan = False
    if tokenOwner is not None:
      if tokenOwner == 'RS_SVC':
        self.__realBan = True

    self.useNewRes = useNewRes

#############################################################################

  def enforce( self, pdpIn = None, rsDBIn = None, rmDBIn = None, ncIn = None, setupIn = None,
               daIn = None, csAPIIn = None, knownInfo = None ):
    """
    enforce policies, using a PDP  (Policy Decision Point), based on

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
       :attr:`pdpIn`: a custom PDP object (optional)

       :attr:`rsDBIn`: a custom (statuses) database object (optional)

       :attr:`rmDBIn`: a custom (management) database object (optional)

       :attr:`setupIn`: a string with the present setup (optional)

       :attr:`ncIn`: a custom notification client object (optional)

       :attr:`daIn`: a custom DiracAdmin object (optional)

       :attr:`csAPIIn`: a custom CSAPI object (optional)

       :attr:`knownInfo`: a string of known provided information (optional)
    """

    #PDP
    if pdpIn is not None:
      pdp = pdpIn
    else:
      # Use standard DIRAC PDP
      from DIRAC.ResourceStatusSystem.PolicySystem.PDP import PDP
      pdp = PDP( self.VOExtension, granularity = self.__granularity, name = self.__name,
                 status = self.__status, formerStatus = self.__formerStatus, reason = self.__reason,
                 siteType = self.__siteType, serviceType = self.__serviceType,
                 resourceType = self.__resourceType, useNewRes = self.useNewRes )

    #DB
    if rsDBIn is not None:
      rsDB = rsDBIn
    else:
      # Use standard DIRAC DB
      from DIRAC.ResourceStatusSystem.DB.ResourceStatusDB import ResourceStatusDB
      rsDB = ResourceStatusDB()

    if rmDBIn is not None:
      rmDB = rmDBIn
    else:
      # Use standard DIRAC DB
      from DIRAC.ResourceStatusSystem.DB.ResourceManagementDB import ResourceManagementDB
      rmDB = ResourceManagementDB()

    #setup
    if setupIn is not None:
      setup = setupIn
    else:
      # get present setup
      setup = CS.getSetup()[ 'Value' ]

    #notification client
    if ncIn is not None:
      nc = ncIn
    else:
      from DIRAC.FrameworkSystem.Client.NotificationClient import NotificationClient
      nc = NotificationClient()

    #DiracAdmin
    if daIn is not None:
      da = daIn
    else:
      from DIRAC.Interfaces.API.DiracAdmin import DiracAdmin
      da = DiracAdmin()

    #CSAPI
    if csAPIIn is not None:
      csAPI = csAPIIn
    else:
      from DIRAC.ConfigurationSystem.Client.CSAPI import CSAPI
      csAPI = CSAPI()

    ###################
    # policy decision #
    ###################

    resDecisions = pdp.takeDecision( knownInfo = knownInfo )
    assert(type(resDecisions) == dict and resDecisions != {})

    res          = resDecisions[ 'PolicyCombinedResult' ]
    actionBaseMod = "DIRAC.ResourceStatusSystem.PolicySystem.Actions"

    # Security mechanism in case there is no PolicyType returned
    if res == {}:
      EmptyPolTypeActions( self.__granularity, self.__name, resDecisions, res )

    else:
      policyType   = res[ 'PolicyType' ]

      if 'Resource_PolType' in policyType:
        m = Utils.voimport(actionBaseMod + ".Resource_PolType", self.VOExtension)
        m.ResourcePolTypeActions( self.__granularity, self.__name, resDecisions, res, rsDB, rmDB )

      if 'Alarm_PolType' in policyType:
        m = Utils.voimport(actionBaseMod + ".AlarmPolTypeActions", self.VOExtension)
        m.AlarmPolTypeActions(self.__name, res, nc, setup, rsDB,
                            Granularity=self.__granularity,
                            SiteType=self.__siteType,
                            ServiceType=self.__serviceType,
                            ResourceType=self.__resourceType)

      if 'RealBan_PolType' in policyType and self.__realBan == True:
        m = Utils.voimport(actionBaseMod + ".RealBanPolTypeActions")
        m.RealBanPolTypeActions( self.__granularity, self.__name, res, da, csAPI, setup )
