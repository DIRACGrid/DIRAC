from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import uuid
import tempfile
import tornado.process
from io import open

from DIRAC import gConfig
from DIRAC.Core.Security import Locations, X509Chain, X509CRL
from DIRAC.Core.Utilities.Decorators import deprecated

__RCSID__ = "$Id$"

BASECS = "/WebApp"


def getCSValue(opt, defValue=None):
  """ Get option from CS

      :param str opt: option
      :param defValue: default value

      :return: defValue type result
  """
  return gConfig.getValue("%s/%s" % (BASECS, opt), defValue)


def getCSSections(opt):
  """ Get sections from CS

      :param str opt: option

      :return: S_OK(list)/S_ERROR()
  """
  return gConfig.getSections("%s/%s" % (BASECS, opt))


def getCSOptions(opt):
  """ Get options from CS

      :param str opt: option

      :return: S_OK(list)/S_ERROR()
  """
  return gConfig.getOptions("%s/%s" % (BASECS, opt))


def getCSOptionsDict(opt):
  """ Get options dictionary from CS

      :param str opt: option

      :return: S_OK(dict)/S_ERROR()
  """
  return gConfig.getOptionsDict("%s/%s" % (BASECS, opt))


def getTitle():
  """ Get title

      :return: str
  """
  defVal = gConfig.getValue("/DIRAC/Configuration/Name", gConfig.getValue("/DIRAC/Setup"))
  return "%s - DIRAC" % gConfig.getValue("%s/Title" % BASECS, defVal)


def devMode():
  """ Get development mode status

      :result: bool
  """
  return getCSValue("DevelopMode", False)


def rootURL():
  """ Get root URL

      :return: str
  """
  return getCSValue("RootURL", "/DIRAC")


def balancer():
  """ Get balancer

      :return: str
  """
  b = getCSValue("Balancer", "").lower()
  if b in ("", "none"):
    return ""
  return b


def numProcesses():
  """ Get number of processes

      :return: int
  """
  return getCSValue("NumProcesses", 1)


def HTTPS():
  """ Get flag of enable HTTPS

      :return: bool
  """
  if balancer():
    return False
  return getCSValue("HTTPS/Enabled", True)


def HTTPPort():
  """ Get HTTP port

      :return: int
  """
  if balancer():
    default = 8000
  else:
    default = 8080
  procAdd = tornado.process.task_id() or 0
  return getCSValue("HTTP/Port", default) + procAdd


def HTTPSPort():
  """ Get HTTPS port

      :return: int
  """
  return getCSValue("HTTPS/Port", 8443)


def HTTPSCert():
  """ Get certificate path for HTTPS

      :return: str
  """
  cert = Locations.getHostCertificateAndKeyLocation()
  if cert:
    cert = cert[0]
  else:
    cert = "/opt/dirac/etc/grid-security/hostcert.pem"
  return getCSValue("HTTPS/Cert", cert)


def HTTPSKey():
  """ Get key path for HTTPS

      :return: str
  """
  key = Locations.getHostCertificateAndKeyLocation()
  if key:
    key = key[1]
  else:
    key = "/opt/dirac/etc/grid-security/hostkey.pem"
  return getCSValue("HTTPS/Key", key)


def setup():
  """ Get setup path

      :return: str
  """
  return gConfig.getValue("/DIRAC/Setup")


def cookieSecret():
  """ Get cookie secret

      :return: str
  """
  # TODO: Store the secret somewhere
  return gConfig.getValue("CookieSecret", uuid.getnode())


def generateCAFile():
  """ Generate a single CA file with all the PEMs

      :return: str or bool
  """
  caDir = Locations.getCAsLocation()
  for fn in (os.path.join(os.path.dirname(caDir), "cas.pem"),
             os.path.join(os.path.dirname(HTTPSCert()), "cas.pem"),
             False):
    if not fn:
      fn = tempfile.mkstemp(prefix="cas.", suffix=".pem")[1]
    try:
      fd = open(fn, "w")
    except IOError:
      continue
    for caFile in os.listdir(caDir):
      caFile = os.path.join(caDir, caFile)
      chain = X509Chain.X509Chain()
      result = chain.loadChainFromFile(caFile)
      if not result['OK']:
        continue
      expired = chain.hasExpired()
      if not expired['OK'] or expired['Value']:
        continue
      fd.write(chain.dumpAllToString()['Value'])
    fd.close()
    return fn
  return False


def generateRevokedCertsFile():
  """ Generate a single CA file with all the PEMs

      :return: str or bool
  """
  caDir = Locations.getCAsLocation()
  for fn in (os.path.join(os.path.dirname(caDir), "allRevokedCerts.pem"),
             os.path.join(os.path.dirname(HTTPSCert()), "allRevokedCerts.pem"),
             False):
    if not fn:
      fn = tempfile.mkstemp(prefix="allRevokedCerts", suffix=".pem")[1]
    try:
      fd = open(fn, "w")
    except IOError:
      continue
    for caFile in os.listdir(caDir):
      caFile = os.path.join(caDir, caFile)
      chain = X509CRL.X509CRL()
      result = chain.loadCRLFromFile(caFile)
      if not result['OK']:
        continue
      fd.write(chain.dumpAllToString()['Value'])
    fd.close()
    return fn
  return False


def getAuthSectionForHandler(route):
  """ Get auth section for handler

      :return: str
  """
  return "%s/Access/%s" % (BASECS, route)


def getTheme():
  """ Get theme

      :return: str
  """
  return getCSValue("Theme", "tabs")


def getIcon():
  """ Get icon path

      :return: str
  """
  return getCSValue("Icon", "/static/core/img/icons/system/favicon.ico")


@deprecated("Please, use SSLProtocol instead.")
def SSLProrocol():
  return SSLProtocol()


def SSLProtocol():
  """ Get ssl protocol

      :return: str
  """
  return getCSValue("SSLProtocol", getCSValue("SSLProtcol", ""))


def getDefaultStaticDirs():
  """ Get default static directories

      :return: list
  """
  defDirs = getCSValue("DefaultStaticDirs", ['defaults', 'demo'])
  if defDirs == ['None']:
    return []
  return defDirs


def getStaticDirs():
  """ Get static directories

      :return: str
  """
  return list(set(getCSValue("StaticDirs", []) + getDefaultStaticDirs()))


def getLogo():
  """ Get logo path

      :return: str
  """
  return getCSValue("Logo", "/static/core/img/icons/system/_logo_waiting.gif")


def getBackgroud():
  """ Get background path

      :return: str
  """
  return getCSValue("BackgroundImage", "/static/core/img/wallpapers/dirac_background_6.png")


def getWelcome():
  """ Get welcome

      :return: str
  """
  return getCSValue("WelcomeHTML", "")


def bugReportURL():
  """ Get bug report URL

      :return: str
  """
  return getCSValue("bugReportURL", "")


def getAuthNames():
  """ Get enabled id providers

      :return: S_OK(list)/S_ERROR()
  """
  return getCSSections("TypeAuths")


def getAppSettings(app):
  """ Get applications options

      :param str app: application name

      :return: S_OK(dict)/S_ERROR
  """
  return gConfig.getOptionsDictRecursively("%s/%s" % (BASECS, app))
