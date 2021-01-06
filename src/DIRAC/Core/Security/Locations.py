""" Collection of utilities for locating certs, proxy, CAs
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

import os
import DIRAC
from DIRAC import gConfig
g_SecurityConfPath = "/DIRAC/Security"


def getProxyLocation():
  """ Get the path of the currently active grid proxy file
  """

  for envVar in ['GRID_PROXY_FILE', 'X509_USER_PROXY']:
    if envVar in os.environ:
      proxyPath = os.path.realpath(os.environ[envVar])
      if os.path.isfile(proxyPath):
        return proxyPath
  # /tmp/x509up_u<uid>
  proxyName = "x509up_u%d" % os.getuid()
  if os.path.isfile("/tmp/%s" % proxyName):
    return "/tmp/%s" % proxyName

  # No gridproxy found
  return False

# Retrieve CA's location


def getCAsLocation():
  """ Retrieve the CA's files location
  """
  # Grid-Security
  retVal = gConfig.getOption('%s/Grid-Security' % g_SecurityConfPath)
  if retVal['OK']:
    casPath = "%s/certificates" % retVal['Value']
    if os.path.isdir(casPath):
      return casPath
  # CAPath
  retVal = gConfig.getOption('%s/CALocation' % g_SecurityConfPath)
  if retVal['OK']:
    casPath = retVal['Value']
    if os.path.isdir(casPath):
      return casPath
  # Look up the X509_CERT_DIR environment variable
  if 'X509_CERT_DIR' in os.environ:
    casPath = os.environ['X509_CERT_DIR']
    return casPath
  # rootPath./etc/grid-security/certificates
  casPath = "%s/etc/grid-security/certificates" % DIRAC.rootPath
  if os.path.isdir(casPath):
    return casPath
  # /etc/grid-security/certificates
  casPath = "/etc/grid-security/certificates"
  if os.path.isdir(casPath):
    return casPath
  # No CA's location found
  return False

# Retrieve CA's location


def getCAsDefaultLocation():
  """ Retrievethe CAs Location inside DIRAC etc directory
  """
  # rootPath./etc/grid-security/certificates
  casPath = "%s/etc/grid-security/certificates" % DIRAC.rootPath
  return casPath

# TODO: Static depending on files specified on CS
# Retrieve certificate


def getHostCertificateAndKeyLocation(specificLocation=None):
  """ Retrieve the host certificate files location.

      Lookup order:

      * ``specificLocation`` (probably broken, don't use it)
      * Environment variables (``DIRAC_X509_HOST_CERT`` and ``DIRAC_X509_HOST_KEY``)
      * CS (``/DIRAC/Security/CertFile`` and ``/DIRAC/Security/CertKey``)
      * Alternative exotic options, with ``prefix`` in  ``server``, ``host``, ``dirac``, ``service``:
        * in `<DIRAC rootpath>/etc/grid-security/` for ``<prefix>cert.pem`` and ``<prefix>key.pem``
        * in the path defined in the CS in ``/DIRAC/Security/Grid-Security``

      :param specificLocation: CS path to look for a the path to cert and key, which then should be the same.
                               Probably does not work, don't use it

      :returns: tuple ``(<cert location>, <key location>)`` or ``False``

  """

  fileDict = {}

  # First, check the environment variables
  for fileType, envVar in (('cert', 'DIRAC_X509_HOST_CERT'), ('key', 'DIRAC_X509_HOST_KEY')):
    if envVar in os.environ and os.path.exists(os.environ[envVar]):
      fileDict[fileType] = os.environ[envVar]

  for fileType in ("cert", "key"):
    # Check if we already have the info
    if fileType in fileDict:
      continue

    # Direct file in config
    retVal = gConfig.getOption('%s/%sFile' % (g_SecurityConfPath, fileType.capitalize()))
    if retVal['OK']:
      fileDict[fileType] = retVal['Value']
      continue
    fileFound = False
    for filePrefix in ("server", "host", "dirac", "service"):
      # Possible grid-security's
      paths = []
      retVal = gConfig.getOption('%s/Grid-Security' % g_SecurityConfPath)
      if retVal['OK']:
        paths.append(retVal['Value'])
      paths.append("%s/etc/grid-security/" % DIRAC.rootPath)
      for path in paths:
        filePath = os.path.realpath("%s/%s%s.pem" % (path, filePrefix, fileType))
        if os.path.isfile(filePath):
          fileDict[fileType] = filePath
          fileFound = True
          break
      if fileFound:
        break
  if "cert" not in fileDict or "key" not in fileDict:
    return False
  # we can specify a location outside /opt/dirac/etc/grid-security directory
  if specificLocation:
    fileDict["cert"] = gConfig.getValue(specificLocation, fileDict["cert"])
    fileDict["key"] = gConfig.getValue(specificLocation, fileDict["key"])

  return (fileDict["cert"], fileDict["key"])


def getCertificateAndKeyLocation():
  """ Get the locations of the user X509 certificate and key pem files
  """

  certfile = ''
  if 'X509_USER_CERT' in os.environ:
    if os.path.exists(os.environ["X509_USER_CERT"]):
      certfile = os.environ["X509_USER_CERT"]
  if not certfile:
    if os.path.exists(os.environ["HOME"] + '/.globus/usercert.pem'):
      certfile = os.environ["HOME"] + '/.globus/usercert.pem'

  if not certfile:
    return False

  keyfile = ''
  if 'X509_USER_KEY' in os.environ:
    if os.path.exists(os.environ["X509_USER_KEY"]):
      keyfile = os.environ["X509_USER_KEY"]
  if not keyfile:
    if os.path.exists(os.environ["HOME"] + '/.globus/userkey.pem'):
      keyfile = os.environ["HOME"] + '/.globus/userkey.pem'

  if not keyfile:
    return False

  return (certfile, keyfile)


def getDefaultProxyLocation():
  """ Get the location of a possible new grid proxy file
  """

  for envVar in ['GRID_PROXY_FILE', 'X509_USER_PROXY']:
    if envVar in os.environ:
      proxyPath = os.path.realpath(os.environ[envVar])
      return proxyPath

  # /tmp/x509up_u<uid>
  proxyName = "x509up_u%d" % os.getuid()
  return "/tmp/%s" % proxyName
