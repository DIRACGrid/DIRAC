'''
:mod: Utils

Module that collects utility functions.

'''
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = '$Id$'

import fnmatch

from DIRAC import gConfig, S_OK
from DIRAC.Core.Utilities import List
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations


def voimport(base_mod):
  '''
    Function to import from extensions, if not found, tries from DIRAC.
  '''

  for ext in gConfig.getValue('DIRAC/Extensions', []):

    try:
      return __import__(ext + base_mod, globals(), locals(), ['*'])
    except ImportError:
      continue
  # If not found in extensions, import it in DIRAC base.
  return __import__(base_mod, globals(), locals(), ['*'])


def getCSTree(csPath=''):
  '''
    Gives the configuration rooted at path in a Python dict. The
    result is a Python dictionary that reflects the structure of the
    configuration file.
  '''

  opHelper = Operations()

  def getCSTreeAsDict(treePath):
    '''
      Function to recursively iterate over a CS tree
    '''

    csTreeDict = {}

    opts = opHelper.getOptionsDict(treePath)
    if opts['OK']:

      opts = opts['Value']

      for optKey, optValue in opts.items():
        if optValue.find(',') > -1:
          optValue = List.fromChar(optValue)
        else:
          optValue = [optValue]
        csTreeDict[optKey] = optValue

    secs = opHelper.getSections(treePath)
    if secs['OK']:

      secs = secs['Value']

      for sec in secs:

        secTree = getCSTreeAsDict('%s/%s' % (treePath, sec))
        if not secTree['OK']:
          return secTree

        csTreeDict[sec] = secTree['Value']

    return S_OK(csTreeDict)

  return getCSTreeAsDict(csPath)


def configMatch(candidateParams, configParams):
  '''
  For a given configuration, the candidate will be rejected if:

    - it is missing at least one of the params in the config
    - if a param of the candidate does not match the config params
    - if a candidate param is None, is considered as wildcard

  '''

  for key in candidateParams:

    if key not in configParams:
      # The candidateParams is missing one of the parameters required
      # return False
      continue

    if candidateParams[key] is None:
      # None is assumed to be a wildcard (*)
      continue

    cParameter = candidateParams[key]
    if not isinstance(cParameter, list):
      cParameter = [cParameter]

    # We allow using UNIX-like regular expression ( wild-cards ) on the CS
    _matches = False
    for configItem in configParams[key]:
      if fnmatch.filter(set(cParameter), configItem):
        _matches = True
        break

    if not _matches:
      return False

  return True

################################################################################
# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
