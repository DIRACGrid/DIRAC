
import os
import uuid
import tempfile
import tornado.process
from DIRAC import S_OK, S_ERROR, gConfig
from DIRAC.Core.Security import Locations, X509Chain, X509CRL

_RCSID_ = "$Id$"

BASECS = "/WebApp"


def getCSValue(opt, defValue=None):
  return gConfig.getValue("%s/%s" % (BASECS, opt), defValue)


def getCSSections(opt):
  return gConfig.getSections("%s/%s" % (BASECS, opt))


def getCSOptions(opt):
  return gConfig.getOptions("%s/%s" % (BASECS, opt))


def getCSOptionsDict(opt):
  return gConfig.getOptionsDict("%s/%s" % (BASECS, opt))


def getTitle():
  defVal = gConfig.getValue("/DIRAC/Configuration/Name", gConfig.getValue("/DIRAC/Setup"))
  return "%s - DIRAC" % gConfig.getValue("%s/Title" % BASECS, defVal)


def devMode():
  return getCSValue("DevelopMode", True)


def rootURL():
  return getCSValue("RootURL", "/DIRAC")


def balancer():
  b = getCSValue("Balancer", "").lower()
  if b in ("", "none"):
    return ""
  return b


def numProcesses():
  return getCSValue("NumProcesses", 1)


def HTTPS():
  if balancer():
    return False
  return getCSValue("HTTPS/Enabled", True)


def HTTPPort():
  if balancer():
    default = 8000
  else:
    default = 8080
  procAdd = tornado.process.task_id() or 0
  return getCSValue("HTTP/Port", default) + procAdd


def HTTPSPort():
  return getCSValue("HTTPS/Port", 8443)


def HTTPSCert():
  cert = Locations.getHostCertificateAndKeyLocation()
  if cert:
    cert = cert[0]
  else:
    cert = "/opt/dirac/etc/grid-security/hostcert.pem"
  return getCSValue("HTTPS/Cert", cert)


def HTTPSKey():
  key = Locations.getHostCertificateAndKeyLocation()
  if key:
    key = key[1]
  else:
    key = "/opt/dirac/etc/grid-security/hostkey.pem"
  return getCSValue("HTTPS/Key", key)


def setup():
  return gConfig.getValue("/DIRAC/Setup")


def cookieSecret():
  # TODO: Store the secret somewhere
  return gConfig.getValue("CookieSecret", uuid.getnode())


def generateCAFile():
  """
  Generate a single CA file with all the PEMs
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
      result = X509Chain.X509Chain.instanceFromFile(caFile)
      if not result['OK']:
        continue
      chain = result['Value']
      expired = chain.hasExpired()
      if not expired['OK'] or expired['Value']:
        continue
      fd.write(chain.dumpAllToString()['Value'])
    fd.close()
    return fn
  return False


def generateRevokedCertsFile():
  """
  Generate a single CA file with all the PEMs
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
      result = X509CRL.X509CRL.instanceFromFile(caFile)
      if not result['OK']:
        continue
      chain = result['Value']
      fd.write(chain.dumpAllToString()['Value'])
    fd.close()
    return fn
  return False


def getAuthSectionForHandler(route):
  return "%s/Access/%s" % (BASECS, route)


def getTheme():
  return getCSValue("Theme", "desktop")


def getIcon():
  return getCSValue("Icon", "/static/core/img/icons/system/favicon.ico")


def SSLProrocol():
  return getCSValue("SSLProtcol", "")


def getStaticDirs():
  return getCSValue("StaticDirs", [])


def getLogo():
  return getCSValue("Logo", "/static/core/img/icons/system/_logo_waiting.gif")


def getBackgroud():
  return getCSValue("BackgroundImage", "/static/core/img/wallpapers/dirac_background_6.png")


def getWelcome():
  return getCSValue("WelcomeHTML","")


def bugReportURL():
  return getCSValue("bugReportURL", "")


def getAuthNames():
  return getCSSections("TypeAuths")


def getAuthSettingsDict(authname):
  return getCSOptionsDict("TypeAuths/%s" % authname)


def getAuthSettingsOptions(authname):
  return getCSOptions("TypeAuths/%s" % authname)


def getAuthCFG(authname,getvalue):
  return getCSValue("TypeAuths/%s/%s" % (authname, getvalue))
  

