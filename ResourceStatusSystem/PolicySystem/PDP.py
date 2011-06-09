"""
The PDP (Policy Decision Point) module is used to:

1. Decides which policies have to be applied.

2. Invokes an evaluation of the policies, and returns the result (to a PEP)
"""
#############################################################################

#import time
import datetime

from DIRAC.ResourceStatusSystem.Utilities.Utils             import where, assignOrRaise
from DIRAC.ResourceStatusSystem.PolicySystem                import Status,Configurations
from DIRAC.ResourceStatusSystem.Utilities.Exceptions        import InvalidRes, \
    InvalidStatus, InvalidSiteType, InvalidServiceType, InvalidResourceType, RSSException
from DIRAC.ResourceStatusSystem.Utilities.InfoGetter        import InfoGetter
from DIRAC.ResourceStatusSystem.PolicySystem.PolicyCaller   import PolicyCaller
from DIRAC.ResourceStatusSystem.Command.CommandCaller       import CommandCaller

#############################################################################


class PDP:
  """
  PDP = Policy Decision Point.

  Used to invoke policies and to take decision based on the polict results combination.
  """

#############################################################################

  def __init__(self, VOExtension, granularity = None, name = None, status = None, formerStatus = None,
               reason = None, siteType = None, serviceType = None, resourceType = None,
               useNewRes = False):
    """
    PDP (Policy Decision Point) initialization

    :params:
      :attr:`VOExtension`: string - VO extension (e.g. 'LHCb')

      :attr:`granularity`: string - a ValidRes

      :attr:`name`: string - name (e.g. of a site)

      :attr:`status`: string - status

      :attr:`formerStatus`: string - former status

      :attr:`reason`: string - optional reason for last status change

      :attr:`siteType`: string - optional site type

      :attr:`serviceType`: string - optional service type

      :attr:`resourceType`: string - optional resource type
    """

    self.VOExtension = VOExtension

    self.__granularity  = assignOrRaise(granularity, Configurations.ValidRes, InvalidRes, self, self.__init__)
    self.__name         = name
    self.__status       = assignOrRaise(status, Configurations.ValidStatus, InvalidStatus, self, self.__init__)
    self.__formerStatus = assignOrRaise(formerStatus, Configurations.ValidStatus, InvalidStatus, self, self.__init__)
    self.__reason       = reason
    self.__siteType     = assignOrRaise(siteType, Configurations.ValidSiteType, InvalidSiteType, self, self.__init__)
    self.__serviceType  = assignOrRaise(serviceType, Configurations.ValidServiceType, InvalidServiceType, self, self.__init__)
    self.__resourceType = assignOrRaise(resourceType, Configurations.ValidResourceType, InvalidResourceType, self, self.__init__)

    cc      = CommandCaller()
    self.pc = PolicyCaller(cc)

    self.useNewRes = useNewRes

    self.args      = None
    self.policy    = None
    self.knownInfo = None
    self.ig        = None


#############################################################################

  def takeDecision(self, policyIn=None, argsIn=None, knownInfo=None):
    """ PDP MAIN FUNCTION

        decides policies that have to be applied, based on

        __granularity,

        __name,

        __status,

        __formerStatus

        __reason

        If more than one policy is evaluated, results are combined.

        Logic for combination: a conservative approach is followed
        (i.e. if a site should be banned for at least one policy, that's what is returned)

        returns:

          { 'PolicyType': a policyType (in a string),
            'Action': True|False,
            'Status': 'Active'|'Probing'|'Banned',
            'Reason': a reason
            'EndDate: datetime.datetime (in a string)}
    """

    self.args      = argsIn
    self.policy    = policyIn
    self.knownInfo = knownInfo


    self.ig = InfoGetter(self.VOExtension)

    EVAL = self.ig.getInfoToApply(('policy', 'policyType'),
                                  granularity  = self.__granularity,
                                  status       = self.__status,
                                  formerStatus = self.__formerStatus,
                                  siteType     = self.__siteType,
                                  serviceType  = self.__serviceType,
                                  resourceType = self.__resourceType,
                                  useNewRes    = self.useNewRes)

    for policyGroup in EVAL:

      policyType = policyGroup['PolicyType']

      if self.policy is not None:
        # Only the policy provided will be evaluated
        # FIXME: Check that the policies are valid.
        singlePolicyResults = self.policy.evaluate()
      else:
        if policyGroup['Policies'] is None:
          return { 'SinglePolicyResults' : [],
                   'PolicyCombinedResult' : {'PolicyType': policyType,
                                             'Action': False,
                                             'Reason':'No policy results'}}
        else:
          singlePolicyResults = self._invocation(self.VOExtension, self.__granularity,
                                                 self.__name, self.__status, self.policy,
                                                 self.args, policyGroup['Policies'])

      policyCombinedResults = self._policyCombination(singlePolicyResults)
      assert(type(policyCombinedResults) == dict)

      if not policyCombinedResults:
        return { 'SinglePolicyResults' : singlePolicyResults,
                 'PolicyCombinedResult': {} }


      #
      # policy results communication
      #

      newstatus = policyCombinedResults['Status']

      if newstatus != self.__status: # Policies satisfy
        reason = policyCombinedResults['Reason']
        newPolicyType = self.ig.getNewPolicyType(self.__granularity, newstatus)
        for npt in newPolicyType:
          if npt not in policyType:
            policyType.append(npt)
        decision = { 'PolicyType': policyType, 'Action': True, 'Status': newstatus, 'Reason': reason }
        if policyCombinedResults.has_key('EndDate'):
          decision['EndDate'] = policyCombinedResults['EndDate']

      else: # Policies does not satisfy
        reason = policyCombinedResults['Reason']
        decision = { 'PolicyType': policyType, 'Action': False, 'Reason': reason }
        if policyCombinedResults.has_key('EndDate'):
          decision['EndDate'] = policyCombinedResults['EndDate']

    res = { 'SinglePolicyResults' : singlePolicyResults,
            'PolicyCombinedResult' : decision }

    assert(type(res) == dict)
    return res

#############################################################################

  def _invocation(self, VOExtension, granularity, name, status, policy, args, policies):
    """ One by one, use the PolicyCaller to invoke the policies, and putting
        their results in `policyResults`. When the status is `Unknown`, invokes
        `self.__useOldPolicyRes`.
    """

    policyResults = []

    for p in policies:
      pName = p['Name']
      pModule = p['Module']
      extraArgs = p['args']
      commandIn = p['commandIn']
      res = self.pc.policyInvocation(VOExtension, granularity = granularity, name = name,
                                     status = status, policy = policy, args = args, pName = pName,
                                     pModule = pModule, extraArgs = extraArgs, commandIn = commandIn)

      # If res is empty, return immediately
      if not res: return policyResults

      if not res.has_key('Status'):
        print("\n\n Policy result " + str(res) + " does not return 'Status'\n\n")
        raise TypeError

      # Else
      if res['Status'] == 'Unknown':
        res = self.__useOldPolicyRes(name = name, policyName = pName)

      if res['Status'] == 'NeedConfirmation':
        pName = p['ConfirmationPolicy']
        triggeredPolicy = self.ig.C_Policies[pName]
        pModule = triggeredPolicy['module']
        extraArgs = triggeredPolicy['args']
        commandIn = triggeredPolicy['commandIn']
        res = self.pc.policyInvocation(VOExtension, granularity = granularity, name = name,
                                       status = status, policy = policy, args = args, pName = pName,
                                       pModule = pModule, extraArgs = extraArgs, commandIn = commandIn)

      if res['Status'] not in ('Error', 'Unknown', 'NeedConfirmation'):
        policyResults.append(res)

    return policyResults

#############################################################################

  def _policyCombination(self, policies):
    """
    INPUT: list type
    OUTPUT: dict type
    * Compute a new status, and store it in variable newStatus, of type integer.
    * Make a list of policies that have the worst result.
    * Concatenate the Reason fields
    * Take the first EndDate field that exists (FIXME: Do something more clever)
    * Finally, return the result
    """
    if policies == []: return {}

    policies.sort(key=Status.value_of_policy)
    newStatus = -1 # First, set an always invalid status

    try:
      # We are in a special status, maybe forbidden transitions
      _prio, access_list, gofun = Status.statesInfo[self.__status]
      if access_list != set():
        # Restrictions on transitions, checking if one is suitable:
        for p in policies:
          if Status.value_of_policy(p) in access_list:
            newStatus = Status.value_of_policy(p)
            break

        # No status from policies suitable, applying stategy and
        # returning result.
        if newStatus == -1:
          newStatus = gofun(access_list)
          return { 'Status': Status.status_of_value(newStatus), 'Reason': 'Status forced by PDP' }

      else:
        # Special Status, but no restriction on transitions
        newStatus = Status.value_of_policy(policies[0])

    except KeyError:
      # We are in a "normal" status: All transitions are possible.
      newStatus = Status.value_of_policy(policies[0])

    # At this point, a new status has been chosen. newStatus is an
    # integer.

    worstPolicies = [p for p in policies if Status.value_of_policy(p) == newStatus]

    # Concatenate reasons
    def getReason(p):
      try:
        res = p['Reason']
      except KeyError:
        res = ''
      return res

    worstPoliciesReasons = [getReason(p) for p in worstPolicies]

    def catRes(x, y):
      if x and y : return x + ' |###| ' + y
      elif x or y:
        if x: return x
        else: return y
      else       : return ''

    concatenatedRes = reduce(catRes, worstPoliciesReasons, '')

    # Handle EndDate
    endDatePolicies = [p for p in worstPolicies if p.has_key('EndDate')]

    # Building and returning result
    res = {}
    res['Status'] = Status.status_of_value(newStatus)
    if concatenatedRes != '': res['Reason']  = concatenatedRes
    if endDatePolicies != []: res['EndDate'] = endDatePolicies[0]['EndDate']
    return res

#############################################################################

  def __useOldPolicyRes(self, name, policyName):
    """ Use the RSS Service to get an old policy result.
        If such result is older than 2 hours, it returns {'Status':'Unknown'}
    """

    from DIRAC.Core.DISET.RPCClient import RPCClient
    rsS = RPCClient("ResourceStatus/ResourceManagement")

    res = rsS.getPolicyRes(name, policyName, True)
    if not res['OK']:
      raise RSSException, where(self, self.__useOldPolicyRes) + ' Could not get a policy result'

    res = res['Value']

    if res == []:
      return {'Status':'Unknown'}

    oldStatus = res[0]
    oldReason = res[1]
    lastCheckTime = res[2]

    if ( lastCheckTime + datetime.timedelta(hours = 2) ) < datetime.datetime.utcnow():
      return {'Status':'Unknown'}

    result = {}

    result['Status'] = oldStatus
    result['Reason'] = oldReason
    result['OLD'] = True
    result['PolicyName'] = policyName

    return result

#############################################################################
