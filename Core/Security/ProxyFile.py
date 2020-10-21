""" Collection of utilities for dealing with security files (i.e. proxy files)
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

import six
import os
import stat
import tempfile

from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Utilities import DErrno
from DIRAC.Core.Security.X509Chain import X509Chain  # pylint: disable=import-error
from DIRAC.Core.Security.Locations import getProxyLocation


def writeToProxyFile(proxyContents, fileName=False):
  """ Write a proxy string to file

      arguments:
        - proxyContents : string object to dump to file
        - fileName : filename to dump to
  """
  if not fileName:
    try:
      fd, proxyLocation = tempfile.mkstemp()
      os.close(fd)
    except IOError:
      return S_ERROR(DErrno.ECTMPF)
    fileName = proxyLocation
  try:
    with open(fileName, 'wb') as fd:
      fd.write(proxyContents)
  except Exception as e:
    return S_ERROR(DErrno.EWF, " %s: %s" % (fileName, repr(e).replace(',)', ')')))
  try:
    os.chmod(fileName, stat.S_IRUSR | stat.S_IWUSR)
  except Exception as e:
    return S_ERROR(DErrno.ESPF, "%s: %s" % (fileName, repr(e).replace(',)', ')')))
  return S_OK(fileName)


def writeChainToProxyFile(proxyChain, fileName):
  """
  Write an X509Chain to file

  arguments:
    - proxyChain : X509Chain object to dump to file
    - fileName : filename to dump to
  """
  retVal = proxyChain.dumpAllToString()
  if not retVal['OK']:
    return retVal
  return writeToProxyFile(retVal['Value'], fileName)


def writeChainToTemporaryFile(proxyChain):
  """
  Write a proxy chain to a temporary file
  return S_OK( string with name of file )/ S_ERROR
  """
  try:
    fd, proxyLocation = tempfile.mkstemp()
    os.close(fd)
  except IOError:
    return S_ERROR(DErrno.ECTMPF)
  retVal = writeChainToProxyFile(proxyChain, proxyLocation)
  if not retVal['OK']:
    try:
      os.unlink(proxyLocation)
    except BaseException:
      pass
    return retVal
  return S_OK(proxyLocation)


def deleteMultiProxy(multiProxyDict):
  """
  Delete a file from a multiProxyArgument if needed
  """
  if multiProxyDict['tempFile']:
    try:
      os.unlink(multiProxyDict['file'])
    except BaseException:
      pass


def multiProxyArgument(proxy=False):
  """
  Load a proxy:


  :param proxy: param can be:

      * Default -> use current proxy
      * string -> upload file specified as proxy
      * X509Chain -> use chain

  :returns:  S_OK/S_ERROR

    .. code-block:: python

        S_OK( { 'file' : <string with file location>,
                'chain' : X509Chain object,
                'tempFile' : <True if file is temporal>
              } )
        S_ERROR

  """
  tempFile = False
  # Set env
  if isinstance(proxy, X509Chain):
    tempFile = True
    retVal = writeChainToTemporaryFile(proxy)
    if not retVal['OK']:
      return retVal
    proxyLoc = retVal['Value']
  else:
    if not proxy:
      proxyLoc = getProxyLocation()
      if not proxyLoc:
        return S_ERROR(DErrno.EPROXYFIND)
    if isinstance(proxy, six.string_types):
      proxyLoc = proxy
    # Load proxy
    proxy = X509Chain()
    retVal = proxy.loadProxyFromFile(proxyLoc)
    if not retVal['OK']:
      return S_ERROR(DErrno.EPROXYREAD, "ProxyLocation: %s" % proxyLoc)
  return S_OK({'file': proxyLoc,
               'chain': proxy,
               'tempFile': tempFile})
