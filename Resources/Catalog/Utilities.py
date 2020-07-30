""" DIRAC FileCatalog client utilities
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

import six
import os
import errno
import functools

from DIRAC import S_OK, S_ERROR


def checkArgumentFormat(path, generateMap=False):
  """ Bring the various possible form of arguments to FileCatalog methods to
      the standard dictionary form
  """

  def checkArgumentDict(path):
    """ Check and process format of the arguments to FileCatalog methods """
    if isinstance(path, six.string_types):
      urls = {path: True}
    elif isinstance(path, list):
      urls = {}
      for url in path:
        urls[url] = True
    elif isinstance(path, dict):
      urls = path
    else:
      return S_ERROR(errno.EINVAL, "Utils.checkArgumentFormat: Supplied path is not of the correct format.")
    return S_OK(urls)

  if not path:
    return S_ERROR(errno.EINVAL, 'Empty input: %s' % str(path))

  result = checkArgumentDict(path)
  if not result['OK']:
    return result

  pathDict = result['Value']

  # Bring the lfn path to the normalized form
  urls = {}
  urlMap = {}
  for url in pathDict:
    # avoid empty path...
    if not url:
      continue
    mUrl = url
    if url.lower().startswith('lfn:'):
      mUrl = url[4:]
    # Strip off the leading /grid prefix as required for the LFC
    if mUrl.startswith('/grid/'):
      uList = mUrl.split('/')
      uList.pop(1)
      mUrl = '/'.join(uList)
    normPath = os.path.normpath(mUrl)
    urls[normPath] = pathDict[url]
    if normPath != url:
      urlMap[normPath] = url
  if generateMap:
    return S_OK((urls, urlMap))
  else:
    return S_OK(urls)


def checkCatalogArguments(f):
  """ Decorator to check arguments of FileCatalog calls in the clients
  """
  @functools.wraps(f)
  def processWithCheckingArguments(*args, **kwargs):

    checkFlag = kwargs.pop('LFNChecking', True)
    if checkFlag:
      argList = list(args)
      lfnArgument = argList[1]
      result = checkArgumentFormat(lfnArgument, generateMap=True)
      if not result['OK']:
        return result
      checkedLFNDict, lfnMap = result['Value']
      argList[1] = checkedLFNDict
      argTuple = tuple(argList)
    else:
      argTuple = args
    result = f(*argTuple, **kwargs)
    if not result['OK']:
      return result

    if not checkFlag:
      return result

    # Restore original paths
    argList[1] = lfnArgument
    failed = {}
    successful = {}
    for lfn in result['Value']['Failed']:
      failed[lfnMap.get(lfn, lfn)] = result['Value']['Failed'][lfn]
    for lfn in result['Value']['Successful']:
      successful[lfnMap.get(lfn, lfn)] = result['Value']['Successful'][lfn]

    result['Value'].update({"Successful": successful, "Failed": failed})
    return result

  return processWithCheckingArguments
