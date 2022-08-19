""" PDP

  PDP ( PolicyDecisionPoint ) is the back-end for the PolicySystem. It discovers
  the policies, finds the best match, evaluates them, merges their results taking
  the most penalizing one, computes the set of actions to be triggered and returns
  all the information to the PEP which will enforce the actions.

"""
from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.ResourceStatusSystem.PolicySystem.PolicyCaller import PolicyCaller
from DIRAC.ResourceStatusSystem.PolicySystem.StateMachine import RSSMachine
from DIRAC.ResourceStatusSystem.Utilities import RssConfiguration
from DIRAC.ResourceStatusSystem.Utilities.InfoGetter import getPolicyActionsThatApply, getPoliciesThatApply


class PDP:
    """PDP ( Policy Decision Point )"""

    def __init__(self, clients=None):
        """Constructor.

        examples:
          >>> pdp  = PDP( None )
          >>> pdp1 = PDP( {} )
          >>> pdp2 = PDP( { 'Client1' : Client1Object } )

        :Parameters:
          **clients** - [ None, `dict` ]
            dictionary with Clients to be used in the Commands. If None, the Commands
            will create their own clients.

        """

        # decision parameters used to match policies and actions
        self.decisionParams = None

        # Helpers to discover policies and RSS metadata in CS
        self.pCaller = PolicyCaller(clients)

        # RSS State Machine, used to calculate most penalizing state while merging them
        self.rssMachine = RSSMachine("Unknown")

        self.log = gLogger.getSubLogger(self.__class__.__name__)

    def setup(self, decisionParams=None):
        """method that sanitizes the decisionParams and ensures that at least it has
        the keys in `standardParamsDict`. This will be relevant while doing the matching
        with the RSS Policies configuration in the CS.
        There is one key-value pair, `active` which is added on this method. This allows
        policies to be de-activated from the CS, changing their active matchParam to
        something else than `Active`.

        examples:
          >>> pdp.setup( None )
          >>> self.decisionParams
              { 'element' : None, 'name' : None, ... }
          >>> pdp.setup( { 'element' : 'AnElement' } )
          >>> self.decisionParams
              { 'element' : 'AnElement', 'name' : None, ... }
          >>> pdp.setup( { 'NonStandardKey' : 'Something' } )
          >>> self.decisionParams
              { 'NonStandardKey' : 'Something', 'element' : None,... }

        :Parameters:
          **decisionParams** - [ None, `dict` ]
            dictionary with the parameters to be matched with the RSS Policies configuration
            in the CS.

        """

        standardParamsDict = {
            "element": None,
            "name": None,
            "elementType": None,
            "statusType": None,
            "status": None,
            "reason": None,
            "tokenOwner": None,
            # Last parameter allows policies to be de-activated
            "active": "Active",
        }

        if decisionParams is not None:
            standardParamsDict.update(decisionParams)
            if standardParamsDict["element"] is not None:
                self.log = gLogger.getSubLogger(f"{self.__class__.__name__}/{standardParamsDict['element']}")
                if standardParamsDict["name"] is not None:
                    self.log = gLogger.getSubLogger(
                        f"{self.__class__.__name__}/{standardParamsDict['element']}/{standardParamsDict['name']}"
                    )
                    self.log.verbose(
                        "Setup - statusType: %s, status: %s"
                        % (standardParamsDict["statusType"], standardParamsDict["status"])
                    )
            self.decisionParams = standardParamsDict

    def takeDecision(self):
        """main PDP method which does all the work. If firstly finds all the policies
        defined in the CS that match <self.decisionParams> and runs them. Once it has
        all the singlePolicyResults, it combines them. Next step is action discovery:
        using a similar approach to the one used to discover the policies, but also
        taking into account the single policy results and their combined result, finds
        the actions to be triggered and returns.

        examples:
          >>> list(pdp.takeDecision()['Value'])
              ['singlePolicyResults', 'policyCombinedResult', 'decisionParams']
          >>> pdp.takeDecision()['Value']['singlePolicyResults']
              [ { 'Status' : 'Active',
                  'Reason' : 'blah',
                  'Policy' : { 'name'        : 'AlwaysActiveForResource',
                               'type'        : 'AlwaysActive',
                               'module'      : 'AlwaysActivePolicy',
                               'description' : 'This is the AlwaysActive policy'
                               'command'     : None,
                               'args'        : {}
                             }
                }, ... ]
          >>> pdp.takeDecision()['Value']['policyCombinedResult']
              { 'Status'       : 'Active',
                'Reason'       : 'blah ###',
                'PolicyAction' : [ ( 'policyActionName1', 'policyActionType1' ), ... ]
              }

        :return: S_OK( { 'singlePolicyResults'  : `list`,
                         'policyCombinedResult' : `dict`,
                         'decisionParams'      : `dict` } ) / S_ERROR

        """
        if self.decisionParams is None:
            return S_OK({"singlePolicyResults": [], "policyCombinedResult": {}, "decisionParams": self.decisionParams})

        self.log.verbose("Taking decision")

        # Policies..................................................................

        # Get policies that match self.decisionParams
        policiesThatApply = getPoliciesThatApply(self.decisionParams)
        if not policiesThatApply["OK"]:
            return policiesThatApply
        policiesThatApply = policiesThatApply["Value"]
        self.log.verbose("Policies that apply: %s" % ", ".join([po["name"] for po in policiesThatApply]))

        # Evaluate policies
        singlePolicyResults = self._runPolicies(policiesThatApply)
        if not singlePolicyResults["OK"]:
            return singlePolicyResults
        singlePolicyResults = singlePolicyResults["Value"]
        self.log.verbose("Single policy results: %s" % singlePolicyResults)

        # Combine policies and get most penalizing status ( see RSSMachine )
        policyCombinedResults = self._combineSinglePolicyResults(singlePolicyResults)
        if not policyCombinedResults["OK"]:
            return policyCombinedResults
        policyCombinedResults = policyCombinedResults["Value"]
        self.log.verbose("Combined policy result: %s" % policyCombinedResults)

        # Actions...................................................................

        policyActionsThatApply = getPolicyActionsThatApply(
            self.decisionParams, singlePolicyResults, policyCombinedResults
        )
        if not policyActionsThatApply["OK"]:
            return policyActionsThatApply
        policyActionsThatApply = policyActionsThatApply["Value"]
        self.log.verbose("Policy actions that apply: %s" % ",".join(pata[0] for pata in policyActionsThatApply))

        policyCombinedResults["PolicyAction"] = policyActionsThatApply

        return S_OK(
            {
                "singlePolicyResults": singlePolicyResults,
                "policyCombinedResult": policyCombinedResults,
                "decisionParams": self.decisionParams,
            }
        )

    def _runPolicies(self, policies):
        """Given a list of policy dictionaries, loads them making use of the PolicyCaller
        and evaluates them. This method requires to have run setup previously.

        examples:
          >>> pdp._runPolicies([])['Value']
              []
          >>> policyDict = { 'name'        : 'AlwaysActiveResource',
                             'type'        : 'AlwaysActive',
                             'args'        : None,
                             'description' : 'This is the AlwaysActive policy',
                             'module'      : 'AlwaysActivePolicy',
                             'command'     : None }
          >>> pdp._runPolicies([ policyDict, ... ] )['Value']
              [ { 'Status' : 'Active', 'Reason' : 'blah', 'Policy' : policyDict }, ... ]

        :Parameters:
          **policies** - `list( dict )`
            list of dictionaries containing the policies selected to be run. Check the
            examples to get an idea of how the policy dictionaries look like.

        :return: S_OK() / S_ERROR

        """

        policyInvocationResults = []

        # Gets all valid status for RSS to avoid misconfigured policies returning statuses
        # that RSS does not understand.
        validStatus = self.rssMachine.getStates()

        for policyDict in policies:

            # Load and evaluate policy described in <policyDict> for element described
            # in <self.decisionParams>
            policyInvocationResult = self.pCaller.policyInvocation(self.decisionParams, policyDict)
            if not policyInvocationResult["OK"]:
                # We should never enter this line ! Just in case there are policies
                # missconfigured !
                _msg = "runPolicies no OK: %s" % policyInvocationResult
                self.log.error(_msg)
                return S_ERROR(_msg)

            policyInvocationResult = policyInvocationResult["Value"]

            # Sanity Checks ( they should never happen ! )
            if "Status" not in policyInvocationResult:
                _msg = "runPolicies (no Status): %s" % policyInvocationResult
                self.log.error(_msg)
                return S_ERROR(_msg)

            if not policyInvocationResult["Status"] in validStatus:
                _msg = "runPolicies ( not valid status ) %s" % policyInvocationResult["Status"]
                self.log.error(_msg)
                return S_ERROR(_msg)

            if "Reason" not in policyInvocationResult:
                _msg = "runPolicies (no Reason): %s" % policyInvocationResult
                self.log.error(_msg)
                return S_ERROR(_msg)

            policyInvocationResults.append(policyInvocationResult)

        return S_OK(policyInvocationResults)

    def _combineSinglePolicyResults(self, singlePolicyRes):
        """method that merges all the policies results into a combined one, which
        will be the most penalizing status and the reasons of the single policy
        results that returned the same penalizing status. All the rest, are ignored.
        If there are no single policy results, it is returned `Unknown` state. While
        combining policies, the ones containing the option `doNotCombine` are ignored.

        examples:
          >>> pdp._combineSingePolicyResults( [] )['Value']
              { 'Status' : 'Unknown', 'Reason' : 'No policy ..' }
          >>> pdp._combineSingePolicyResults( [ { 'Status' : 'Active', 'Reason' : 'blah', 'Policy' : policyDict } ] )
              { 'Status' : 'Active', 'Reason' : 'blah' }
          >>> pdp._combineSingePolicyResults( [ { 'Status' : 'Active', 'Reason' : 'blah', 'Policy' : policyDict },
                                                { 'Status' : 'Banned', 'Reason' : 'blah 2', 'Policy' : policyDict2 } ] )
              { 'Status' : 'Banned', 'Reason' : 'blah 2' }
          >>> pdp._combineSingePolicyResults( [ { 'Status' : 'Active', 'Reason' : 'blah', 'Policy' : policyDict },
                                                { 'Status' : 'Active', 'Reason' : 'blah 2', 'Policy' : policyDict2 } ] )
              { 'Status' : 'Banned', 'Reason' : 'blah ### blah 2' }

        :Parameters:
          **singlePolicyRes** - `list( dict )`
            list with every single policy result to be combined ( see _runPolicy for more details )

        :return: S_OK( dict( Status, Reason ) | S_ERROR

        """

        # Dictionary to be returned
        policyCombined = {
            "Status": "Unknown",  # default, it should be overridden by the policies, if they exist
            "Reason": "",
        }

        # If there are no policyResults, we return Unknown
        if not singlePolicyRes:
            policyCombined["Reason"] = (
                "No policy applies to %(element)s, %(name)s, %(elementType)s" % self.decisionParams
            )
            self.log.warn(policyCombined["Reason"])
            return S_OK(policyCombined)

        # We set the rssMachine on the current state ( ensures it is a valid one )
        # FIXME: probably this check can be done at takeDecision
        machineStatus = self.rssMachine.setState(self.decisionParams["status"], noWarn=True)
        if not machineStatus["OK"]:
            return machineStatus

        # Discard all single policy results which belongs to policies that have set
        # the option `doNotCombine` in the CS
        policiesToCombine = self._findPoliciesToCombine(singlePolicyRes)

        # Sort policy results using ther statuses by most restrictive ( lower level first )
        self.rssMachine.orderPolicyResults(policiesToCombine)

        # As they have been sorted by most restrictive status, the first one is going
        # to be our candidate new state. Let's ask the RSSMachine if it allows us to
        # make such transition.
        candidateState = policiesToCombine[0]["Status"]
        nextState = self.rssMachine.getNextState(candidateState)

        if not nextState["OK"]:
            return nextState
        nextState = nextState["Value"]
        # most restrictive status defines the VO affected. VO='all' will affect all VOs
        policyCombined["VO"] = policiesToCombine[0].get("VO", "all")

        # If the RssMachine does not accept the candidate, return forcing message
        if candidateState != nextState:

            policyCombined["Status"] = nextState
            policyCombined["Reason"] = f"RssMachine forced status {candidateState} to {nextState}"
            return S_OK(policyCombined)

        # If the RssMachine accepts the candidate, just concatenate the reasons
        for policyRes in policiesToCombine:

            if policyRes["Status"] == nextState:
                policyCombined["Reason"] += "%s ###" % policyRes["Reason"]

        policyCombined["Status"] = nextState

        return S_OK(policyCombined)

    def _findPoliciesToCombine(self, singlePolicyRes):
        """method that iterates over the single policy results and checks the CS
        configuration of the policies looking for the option 'doNotCombine'. If it is
        present, that single policy result is discarded.

        :Parameters:
          **singlePolicyRes** - `list( dict )`
            list with every single policy result to be combined ( see _runPolicy for more details )

        :return: `list( dict )`

        """

        # Get policies configuration from the CS. We want to exclude the policies that
        # have set the option `doNotCombine` from this process.
        policiesConfiguration = RssConfiguration.getPolicies()
        if not policiesConfiguration["OK"]:
            return policiesConfiguration
        policiesConfiguration = policiesConfiguration["Value"]

        # Function that let's us know if we should combine the result of a single policy
        # or not.
        def combinePolicy(policyResult):
            # Extract policy name from the dictionary returned by PolicyCaller
            policyName = policyResult["Policy"]["name"]
            try:
                # If doNotCombineResult is defined, the policy is not taken into account
                # to create the combined result. However, the single policy result remains
                _ = policiesConfiguration[policyName]["doNotCombineResult"]
                return False
            except KeyError:
                return True

        # Make a list of policies of which we want to merge their results
        return [policyResult for policyResult in singlePolicyRes if combinePolicy(policyResult)]


# ...............................................................................
# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
