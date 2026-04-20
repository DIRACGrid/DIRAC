"""InfoGetter

Module used to map the policies with the CS.

"""

import copy

from DIRAC import S_OK, S_ERROR, gConfig, gLogger
from DIRAC.Core.Utilities.ObjectLoader import ObjectLoader
from DIRAC.ResourceStatusSystem.Utilities import RssConfiguration, Utils


def getPoliciesThatApply(decisionParams):
    """
    Method that sanitizes the input parameters and returns the policies that
    match them. Matches the input dictionary with the policies configuration in
    the CS. It returns a list of policy dictionaries that matched.
    """

    # InfoGetter is being called from SiteInspector Agent

    decisionParams = _sanitizedecisionParams(decisionParams)
    gLogger.debug(f"Sanitized decisionParams: {str(decisionParams)}")

    policiesThatApply = []

    # Get policies configuration metadata from CS.
    policiesConfig = RssConfiguration.getPolicies()
    if not policiesConfig["OK"]:
        return policiesConfig
    policiesConfig = policiesConfig["Value"]
    gLogger.debug(f"All policies: {str(policiesConfig)}")

    # Each policy, has the following format
    # <policyName>
    # \
    #  policyType = <policyType>
    #  matchParams
    #  \
    #   ...
    #  configParams
    #  \
    #   ...

    # Get policies that match the given decisionParameters
    for policyName, policySetup in policiesConfig.items():
        # The parameter policyType is mandatory. If not present, skip this entry —
        # it is a command-args defaults section, not a policy definition.
        try:
            policyType = policySetup["policyType"][0]
        except KeyError:
            continue

        # The section matchParams is not mandatory, so we set {} as default.
        policyMatchParams = policySetup.get("matchParams", {})
        gLogger.debug(f"matchParams of {policyName}: {str(policyMatchParams)}")

        # Any key in the CS policy entry that is not a reserved keyword is treated as
        # a command-argument override. These override the defaults from POLICIESMETA.
        _reservedKeys = {"policyType", "matchParams", "configParams", "doNotCombineResult", "active"}
        policyConfigParams = {
            k: v[0] if isinstance(v, list) else v for k, v in policySetup.items() if k not in _reservedKeys
        }

        policyMatch = Utils.configMatch(decisionParams, policyMatchParams)
        gLogger.debug(f"PolicyMatch for decisionParams {decisionParams}: {str(policyMatch)}")

        # WARNING: we need an additional filtering function when the matching
        # is not straightforward (e.g. when the policy specify a 'domain', while
        # the decisionParams has only the name of the element)
        if policyMatch and _filterPolicies(decisionParams, policyMatchParams):
            policiesThatApply.append((policyName, policyType, policyConfigParams, policyMatchParams))

    gLogger.debug(f"policies that apply (before post-processing): {str(policiesThatApply)}")
    policiesThatApply = postProcessingPolicyList(policiesThatApply)
    gLogger.debug(f"policies that apply (after post-processing): {str(policiesThatApply)}")

    objectLoader = ObjectLoader()
    policiesToBeLoaded = []
    # Gets policies parameters from code.
    for policyName, policyType, _policyConfigParams, _policyMatchParams in policiesThatApply:
        try:
            result = objectLoader.loadModule("DIRAC.ResourceStatusSystem.Policy.Configurations")
            if not result["OK"]:
                return result
            configModule = result["Value"]
            policies = copy.deepcopy(configModule.POLICIESMETA)
            policyMeta = policies[policyType]
        except KeyError:
            continue

        # We are not going to use name / type anymore, but we keep them for debugging
        # and future usage.
        policyDict = {"name": policyName, "type": policyType, "args": {}}

        # args is one of the parameters we are going to use on the policies. We copy
        # the defaults from POLICIESMETA and then override with whatever comes from the CS.
        policyDict.update(policyMeta)
        if _policyConfigParams and policyDict.get("args") is not None:
            # Build a case-insensitive lookup of the existing arg keys so that CS keys
            # like "Unit" correctly override POLICIESMETA keys like "unit".
            argsKeyMap = {k.lower(): k for k in policyDict["args"]}
            for csKey, csVal in _policyConfigParams.items():
                targetKey = argsKeyMap.get(csKey.lower(), csKey)
                # CS values are always strings; cast to the type of the existing default.
                existingVal = policyDict["args"].get(targetKey)
                if existingVal is not None:
                    try:
                        csVal = type(existingVal)(csVal)
                    except (ValueError, TypeError):
                        pass
                policyDict["args"][targetKey] = csVal

        policiesToBeLoaded.append(policyDict)

    return S_OK(policiesToBeLoaded)


def getPolicyActionsThatApply(decisionParams, singlePolicyResults, policyCombinedResults):
    """
    Method that sanitizes the input parameters and returns the policies actions
    that match them. Matches the input dictionary with the policy actions
    configuration in the CS. It returns a list of policy actions names that
    matched.
    """

    decisionParams = _sanitizedecisionParams(decisionParams)

    policyActionsThatApply = []

    # Get policies configuration metadata from CS.
    policyActionsConfig = RssConfiguration.getPolicyActions()
    if not policyActionsConfig["OK"]:
        return policyActionsConfig
    policyActionsConfig = policyActionsConfig["Value"]

    # Let's create a dictionary to use it with configMatch
    policyResults = {}
    for policyResult in singlePolicyResults:
        try:
            policyResults[policyResult["Policy"]["name"]] = policyResult["Status"]
        except KeyError:
            continue

    # Get policies that match the given decissionParameters
    for policyActionName, policyActionConfig in policyActionsConfig.items():
        # The parameter policyType is mandatory. If not present, we pick policyActionName
        try:
            policyActionType = policyActionConfig["actionType"][0]
        except KeyError:
            policyActionType = policyActionName
            # continue

        # We get matchParams to be compared against decisionParams
        policyActionMatchParams = policyActionConfig.get("matchParams", {})
        policyMatch = Utils.configMatch(decisionParams, policyActionMatchParams)
        # policyMatch = Utils.configMatch( decisionParams, policyActionConfig )
        if not policyMatch:
            continue

        # Let's check single policy results
        # Assumed structure:
        # ...
        # policyResults
        # <PolicyName> = <PolicyResult1>,<PolicyResult2>...
        policyActionPolicyResults = policyActionConfig.get("policyResults", {})
        policyResultsMatch = Utils.configMatch(policyResults, policyActionPolicyResults)
        if not policyResultsMatch:
            continue

        # combinedResult
        # \Status = X,Y
        # \Reason = asdasd,asdsa
        policyActionCombinedResult = policyActionConfig.get("combinedResult", {})
        policyCombinedMatch = Utils.configMatch(policyCombinedResults, policyActionCombinedResult)
        if not policyCombinedMatch:
            continue

        # policyActionsThatApply.append( policyActionName )
        # They may not be necessarily the same
        policyActionsThatApply.append((policyActionName, policyActionType))

    return S_OK(policyActionsThatApply)


def _sanitizedecisionParams(decisionParams):
    """Function that filters the input parameters. If the input parameter keys
    are no present on the "params" tuple, are not taken into account.
    """

    # active is a hook to disable the policy / action if needed
    params = ("element", "name", "vO", "elementType", "statusType", "status", "reason", "tokenOwner", "active")

    sanitizedParams = {}

    for key in params:
        if key in decisionParams:
            # We can get rid of this now
            # In CS names are with upper case, capitalize them here
            # sanitizedParams[ key[0].upper() + key[1:] ] = decisionParams[ key ]
            sanitizedParams[key] = decisionParams[key]

    return sanitizedParams


def _getComputingElementsByDomainName(targetDomain=None):
    """
    WARNING: TO ADD TO CSHelpers
    Gets all computing elements from /Resources/Sites/<domain>/<site_name>/CEs
    """

    _basePath = "Resources/Sites"
    ces = []

    domainNames = gConfig.getSections(_basePath)
    if not domainNames["OK"]:
        return S_ERROR("No domain names have been specified on the CS")
    domainNames = domainNames["Value"]

    unknownDomains = list(set(targetDomain) - set(domainNames))
    if unknownDomains:
        gLogger.warn(f"Domains {unknownDomains} belong to the policy parameters but not to the CS domains")

    knownDomains = list(set(domainNames) & set(targetDomain))
    if not knownDomains:
        gLogger.warn("Policy parameters domain names do not match with any CS domain names")
        return S_OK([])

    for domainName in knownDomains:
        gLogger.info(f"Fetching the list of Computing Elements belonging to domain {domainName}")
        domainSites = gConfig.getSections(f"{_basePath}/{domainName}")
        if not domainSites["OK"]:
            return domainSites
        domainSites = domainSites["Value"]

        for site in domainSites:
            siteCEs = gConfig.getSections(f"{_basePath}/{domainName}/{site}/CEs")
            if not siteCEs["OK"]:
                # return siteCEs
                gLogger.error(siteCEs["Message"])
                continue
            siteCEs = siteCEs["Value"]
            ces.extend(siteCEs)

    # Remove duplicated ( just in case )
    ces = list(set(ces))
    gLogger.info(f"List of CEs: {str(ces)}")

    return S_OK(ces)


def _filterPolicies(decisionParams, policyMatchParams):
    """
    Method that checks if the given policy doesn't meet certain conditions
    """
    elementType = decisionParams.get("elementType")
    name = decisionParams.get("name")

    # some policies may apply or not also depending on the VO's domain
    # 'CEAvailabilityPolicy' can be applied only if the CE is inside LCG
    if elementType and elementType.upper() == "CE" and "domain" in policyMatchParams:
        # WARNING: policyMatchParams['domain'] is a list of domains
        domains = policyMatchParams["domain"]
        result = _getComputingElementsByDomainName(targetDomain=domains)
        if result["OK"]:
            ces = result["Value"]
            # to verify that the given CE is in the list of the LCG CEs
            if name not in ces:
                gLogger.info(f"ComputingElement {name} NOT found in domains {domains}")
                return False
            else:
                gLogger.info(f"ComputingElement {name} found in domains {domains}")
        else:
            gLogger.warn(f"unable to verify if ComputingElement {name} is in domains {domains}")
            return False

    return True


def postProcessingPolicyList(policiesThatApply):
    """Remove lower-priority duplicates when multiple policies of the same type apply.

    When two or more policies share the same ``policyType`` and both match the current
    element, we keep only the most specific one. Specificity is determined by the number
    of ``matchParams`` keys: more keys = more specific. If one of the duplicates matched
    by ``name`` it is always considered more specific than one that did not.

    This replaces the old per-type hacks (``FreeDiskSpaceMB`` > ``FreeDiskSpaceGB`` >
    ``FreeDiskSpaceTB``) with a generic rule that works for any policy type.
    """
    from collections import defaultdict

    # Group policies by policyType
    byType = defaultdict(list)
    for entry in policiesThatApply:
        policyName, policyType, policyConfigParams, policyMatchParams = entry
        byType[policyType].append(entry)

    result = []
    for policyType, entries in byType.items():
        if len(entries) == 1:
            result.extend(entries)
            continue

        # Multiple policies of the same type matched — keep only the most specific.
        # Specificity = number of matchParams keys; ties broken by name-match presence.
        def specificity(entry):
            matchParams = entry[3]  # policyMatchParams
            nameMatch = 1 if "name" in matchParams else 0
            return (nameMatch, len(matchParams))

        most_specific = max(entries, key=specificity)
        result.append(most_specific)
        gLogger.debug(
            f"postProcessing: multiple {policyType!r} policies matched; "
            f"keeping {most_specific[0]!r}, dropping {[e[0] for e in entries if e is not most_specific]}"
        )

    return result
