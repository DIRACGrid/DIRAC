""" InfoGetter

  Module used to map the policies with the CS.

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = '$Id$'

import copy

from DIRAC import S_OK, S_ERROR, gConfig, gLogger
from DIRAC.ResourceStatusSystem.Utilities import RssConfiguration, Utils


def getPoliciesThatApply(decisionParams):
  """
    Method that sanitizes the input parameters and returns the policies that
    match them. Matches the input dictionary with the policies configuration in
    the CS. It returns a list of policy dictionaries that matched.
  """

  # InfoGetter is being called from SiteInspector Agent

  decisionParams = _sanitizedecisionParams(decisionParams)
  gLogger.debug("Sanitized decisionParams: %s" % str(decisionParams))

  policiesThatApply = []

  # Get policies configuration metadata from CS.
  policiesConfig = RssConfiguration.getPolicies()
  if not policiesConfig['OK']:
    return policiesConfig
  policiesConfig = policiesConfig['Value']
  gLogger.debug("All policies: %s" % str(policiesConfig))

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

    # The parameter policyType replaces policyName, so if it is not present,
    # we pick policyName
    try:
      policyType = policySetup['policyType'][0]
    except KeyError:
      policyType = policyName
      # continue

    # The section matchParams is not mandatory, so we set {} as default.
    policyMatchParams = policySetup.get('matchParams', {})
    gLogger.debug("matchParams of %s: %s" % (policyName, str(policyMatchParams)))

    # FIXME: make sure the values in the policyConfigParams dictionary are typed !!
    policyConfigParams = {}
    # policyConfigParams = policySetup.get( 'configParams', {} )
    policyMatch = Utils.configMatch(decisionParams, policyMatchParams)
    gLogger.debug("PolicyMatch for decisionParams %s: %s" % (decisionParams, str(policyMatch)))

    # WARNING: we need an additional filtering function when the matching
    # is not straightforward (e.g. when the policy specify a 'domain', while
    # the decisionParams has only the name of the element)
    if policyMatch and _filterPolicies(decisionParams, policyMatchParams):
      policiesThatApply.append((policyName, policyType, policyConfigParams))

  gLogger.debug("policies that apply (before post-processing): %s" % str(policiesThatApply))
  policiesThatApply = postProcessingPolicyList(policiesThatApply)
  gLogger.debug("policies that apply (after post-processing): %s" % str(policiesThatApply))

  policiesToBeLoaded = []
  # Gets policies parameters from code.
  for policyName, policyType, _policyConfigParams in policiesThatApply:

    try:
      configModule = Utils.voimport('DIRAC.ResourceStatusSystem.Policy.Configurations')
      policies = copy.deepcopy(configModule.POLICIESMETA)
      policyMeta = policies[policyType]
    except KeyError:
      continue

    # We are not going to use name / type anymore, but we keep them for debugging
    # and future usage.
    policyDict = {'name': policyName,
                  'type': policyType,
                  'args': {}}

    # args is one of the parameters we are going to use on the policies. We copy
    # the defaults and then we update if with whatever comes from the CS.
    policyDict.update(policyMeta)

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
  if not policyActionsConfig['OK']:
    return policyActionsConfig
  policyActionsConfig = policyActionsConfig['Value']

  # Let's create a dictionary to use it with configMatch
  policyResults = {}
  for policyResult in singlePolicyResults:
    try:
      policyResults[policyResult['Policy']['name']] = policyResult['Status']
    except KeyError:
      continue

  # Get policies that match the given decissionParameters
  for policyActionName, policyActionConfig in policyActionsConfig.items():

    # The parameter policyType is mandatory. If not present, we pick policyActionName
    try:
      policyActionType = policyActionConfig['actionType'][0]
    except KeyError:
      policyActionType = policyActionName
      # continue

    # We get matchParams to be compared against decisionParams
    policyActionMatchParams = policyActionConfig.get('matchParams', {})
    policyMatch = Utils.configMatch(decisionParams, policyActionMatchParams)
    # policyMatch = Utils.configMatch( decisionParams, policyActionConfig )
    if not policyMatch:
      continue

    # Let's check single policy results
    # Assumed structure:
    # ...
    # policyResults
    # <PolicyName> = <PolicyResult1>,<PolicyResult2>...
    policyActionPolicyResults = policyActionConfig.get('policyResults', {})
    policyResultsMatch = Utils.configMatch(policyResults, policyActionPolicyResults)
    if not policyResultsMatch:
      continue

    # combinedResult
    # \Status = X,Y
    # \Reason = asdasd,asdsa
    policyActionCombinedResult = policyActionConfig.get('combinedResult', {})
    policyCombinedMatch = Utils.configMatch(policyCombinedResults, policyActionCombinedResult)
    if not policyCombinedMatch:
      continue

    # policyActionsThatApply.append( policyActionName )
    # They may not be necessarily the same
    policyActionsThatApply.append((policyActionName, policyActionType))

  return S_OK(policyActionsThatApply)


def _sanitizedecisionParams(decisionParams):
  """ Function that filters the input parameters. If the input parameter keys
      are no present on the "params" tuple, are not taken into account.
  """

  # active is a hook to disable the policy / action if needed
  params = ('element', 'name', 'elementType', 'statusType', 'status', 'reason', 'tokenOwner', 'active')

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
    Gets all computing elements from /Resources/Sites/<>/<>/CE
  """

  _basePath = 'Resources/Sites'
  ces = []

  domainNames = gConfig.getSections(_basePath)
  if not domainNames['OK']:
    return S_ERROR("No domain names have been specified on the CS")
  domainNames = domainNames['Value']

  unknownDomains = list(set(targetDomain) - set(domainNames))
  if unknownDomains:
    gLogger.warn("Domains %s belong to the policy parameters but not to the CS domains" % unknownDomains)

  knownDomains = list(set(domainNames) & set(targetDomain))
  if not knownDomains:
    gLogger.warn("Policy parameters domain names do not match with any CS domain names")
    return S_OK([])

  for domainName in knownDomains:
    gLogger.info("Fetching the list of Computing Elements belonging to domain %s" % domainName)
    domainSites = gConfig.getSections('%s/%s' % (_basePath, domainName))
    if not domainSites['OK']:
      return domainSites
    domainSites = domainSites['Value']

    for site in domainSites:
      siteCEs = gConfig.getSections('%s/%s/%s/CEs' % (_basePath, domainName, site))
      if not siteCEs['OK']:
        # return siteCEs
        gLogger.error(siteCEs['Message'])
        continue
      siteCEs = siteCEs['Value']
      ces.extend(siteCEs)

  # Remove duplicated ( just in case )
  ces = list(set(ces))
  gLogger.info("List of CEs: %s" % str(ces))

  return S_OK(ces)


def _filterPolicies(decisionParams, policyMatchParams):
  """
    Method that checks if the given policy doesn't meet certain conditions
  """
  elementType = decisionParams.get('elementType')
  name = decisionParams.get('name')

  # some policies may apply or not also depending on the VO's domain
  # 'CEAvailabilityPolicy' can be applied only if the CE is inside LCG
  if elementType and elementType.upper() == 'CE' and 'domain' in policyMatchParams:
    # WARNING: policyMatchParams['domain'] is a list of domains
    domains = policyMatchParams['domain']
    result = _getComputingElementsByDomainName(targetDomain=domains)
    if result['OK']:
      ces = result['Value']
      # to verify that the given CE is in the list of the LCG CEs
      if name not in ces:
        gLogger.info("ComputingElement %s NOT found in domains %s" % (name, domains))
        return False
      else:
        gLogger.info("ComputingElement %s found in domains %s" % (name, domains))
    else:
      gLogger.warn("unable to verify if ComputingElement %s is in domains %s" % (name, domains))
      return False

  return True


def postProcessingPolicyList(policiesThatApply):
  """ Put here any "hacky" post-processing
  """

  # FIXME: the following 2 "if" are a "hack" for dealing with the following case:
  # an SE happens to be subject to, e.g., both the 'FreeDiskSpaceMB' and the 'FreeDiskSpaceGB' policies
  # (currently, there is no way to avoid that this happens, see e.g. LogSE)
  # When this is the case, supposing that an SE has 50 MB free, the policies evaluation will be the following:
  # - 'FreeDiskSpaceMB' will evaluate 'Active'
  # - 'FreeDiskSpaceGB' will evaluate 'Banned'
  # so the SE will end up being banned, but we want only the 'FreeDiskSpaceMB' to be considered.
  if ('FreeDiskSpaceMB', 'FreeDiskSpaceMB', {}) in policiesThatApply:
    try:
      policiesThatApply.remove(('FreeDiskSpaceGB', 'FreeDiskSpaceGB', {}))
    except ValueError:
      pass
    try:
      policiesThatApply.remove(('FreeDiskSpaceTB', 'FreeDiskSpaceTB', {}))
    except ValueError:
      pass
  if ('FreeDiskSpaceGB', 'FreeDiskSpaceGB', {}) in policiesThatApply:
    try:
      policiesThatApply.remove(('FreeDiskSpaceTB', 'FreeDiskSpaceTB', {}))
    except ValueError:
      pass

  return policiesThatApply
