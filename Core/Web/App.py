from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import ssl
import imp
import sys
import signal
import tornado.web
import tornado.process
import tornado.httpserver
import tornado.autoreload

from diraccfg import CFG

import DIRAC

from DIRAC import gLogger, gConfig
from DIRAC.ConfigurationSystem.Client.Helpers import CSGlobals
from DIRAC.Core.Web.HandlerMgr import HandlerMgr
from DIRAC.Core.Web.TemplateLoader import TemplateLoader
from DIRAC.Core.Web.SessionData import SessionData
from DIRAC.Core.Web import Conf

__RCSID__ = "$Id$"


class App(object):

  def __init__(self, handlersLoc='WebApp.handler'):
    self.__handlerMgr = HandlerMgr(handlersLoc, Conf.rootURL())
    self.__servers = {}
    self.log = gLogger.getSubLogger("Web")

  def _logRequest(self, handler):
    status = handler.get_status()
    if status < 400:
      logm = self.log.notice
    elif status < 500:
      logm = self.log.warn
    else:
      logm = self.log.error
    request_time = 1000.0 * handler.request.request_time()
    logm("%d %s %.2fms" % (status, handler._request_summary(), request_time))

  def __reloadAppCB(self):
    gLogger.notice("\n !!!!!! Reloading web app...\n")

  def _loadWebAppCFGFiles(self):
    """
    Load WebApp/web.cfg definitions
    """
    exts = []
    for ext in CSGlobals.getCSExtensions():
      if ext == "DIRAC":
        continue
      if ext[-5:] != "DIRAC":
        ext = "%sDIRAC" % ext
      if ext != "WebAppDIRAC":
        exts.append(ext)
    exts.append("DIRAC")
    exts.append("WebAppDIRAC")
    webCFG = CFG()
    for modName in reversed(exts):
      try:
        modPath = imp.find_module(modName)[1]
      except ImportError:
        continue
      gLogger.verbose("Found module %s at %s" % (modName, modPath))
      cfgPath = os.path.join(modPath, "WebApp", "web.cfg")
      if not os.path.isfile(cfgPath):
        gLogger.verbose("Inexistant %s" % cfgPath)
        continue
      try:
        modCFG = CFG().loadFromFile(cfgPath)
      except Exception as excp:
        gLogger.error("Could not load %s: %s" % (cfgPath, excp))
        continue
      gLogger.verbose("Loaded %s" % cfgPath)
      expl = [Conf.BASECS]
      while len(expl):
        current = expl.pop(0)
        if not modCFG.isSection(current):
          continue
        if modCFG.getOption("%s/AbsoluteDefinition" % current, False):
          gLogger.verbose("%s:%s is an absolute definition" % (modName, current))
          try:
            webCFG.deleteKey(current)
          except BaseException:
            pass
          modCFG.deleteKey("%s/AbsoluteDefinition" % current)
        else:
          for sec in modCFG[current].listSections():
            expl.append("%s/%s" % (current, sec))
      # Add the modCFG
      webCFG = webCFG.mergeWith(modCFG)
    gConfig.loadCFG(webCFG)

  def _loadDefaultWebCFG(self):
    """ This method reloads the web.cfg file from etc/web.cfg """
    modCFG = None
    cfgPath = os.path.join(DIRAC.rootPath, 'etc', 'web.cfg')
    isLoaded = True
    if not os.path.isfile(cfgPath):
      isLoaded = False
    else:
      try:
        modCFG = CFG().loadFromFile(cfgPath)
      except Exception as excp:
        isLoaded = False
        gLogger.error("Could not load %s: %s" % (cfgPath, excp))

    if modCFG:
      if modCFG.isSection("/Website"):
        gLogger.warn("%s configuration file is not correct. It is used by the old portal!" % (cfgPath))
        isLoaded = False
      else:
        gConfig.loadCFG(modCFG)
    else:
      isLoaded = False

    return isLoaded

  def stopChildProcesses(self, sig, frame):
    """
    It is used to properly stop tornado when more than one process is used.
    In principle this is doing the job of runsv....
    :param int sig: the signal sent to the process
    :param object frame: execution frame which contains the child processes
    """
    # tornado.ioloop.IOLoop.instance().add_timeout(time.time()+5, sys.exit)
    for child in frame.f_locals.get('children', []):
      gLogger.info("Stopping child processes: %d" % child)
      os.kill(child, signal.SIGTERM)
    # tornado.ioloop.IOLoop.instance().stop()
    # gLogger.info('exit success')
    sys.exit(0)

  def bootstrap(self):
    """
    Configure and create web app
    """
    self.log.always("\n ====== Starting DIRAC web app ====== \n")

    # Load required CFG files
    if not self._loadDefaultWebCFG():
      # if we have a web.cfg under etc directory we use it, otherwise
      # we use the configuration file defined by the developer
      self._loadWebAppCFGFiles()
    # Calculating routes
    result = self.__handlerMgr.getRoutes()
    if not result['OK']:
      return result
    routes = result['Value']
    # Initialize the session data
    SessionData.setHandlers(self.__handlerMgr.getHandlers()['Value'])
    # Create the app
    tLoader = TemplateLoader(self.__handlerMgr.getPaths("template"))
    kw = dict(debug=Conf.devMode(), template_loader=tLoader, cookie_secret=str(Conf.cookieSecret()),
              log_function=self._logRequest, autoreload=Conf.numProcesses() < 2)

    # please do no move this lines. The lines must be before the fork_processes
    signal.signal(signal.SIGTERM, self.stopChildProcesses)
    signal.signal(signal.SIGINT, self.stopChildProcesses)

    # Check processes if we're under a load balancert
    if Conf.balancer() and Conf.numProcesses() not in (0, 1):
      tornado.process.fork_processes(Conf.numProcesses(), max_restarts=0)
      kw['debug'] = False
    # Debug mode?
    if kw['debug']:
      self.log.info("Configuring in developer mode...")
    # Configure tornado app
    self.__app = tornado.web.Application(routes, **kw)
    self.log.notice("Configuring HTTP on port %s" % (Conf.HTTPPort()))
    # Create the web servers
    srv = tornado.httpserver.HTTPServer(self.__app, xheaders=True)
    port = Conf.HTTPPort()
    srv.listen(port)
    self.__servers[('http', port)] = srv

    Conf.generateRevokedCertsFile()  # it is used by nginx....

    if Conf.HTTPS():
      self.log.notice("Configuring HTTPS on port %s" % Conf.HTTPSPort())
      sslops = dict(certfile=Conf.HTTPSCert(),
                    keyfile=Conf.HTTPSKey(),
                    cert_reqs=ssl.CERT_OPTIONAL,
                    ca_certs=Conf.generateCAFile(),
                    ssl_version=ssl.PROTOCOL_TLSv1)

      sslprotocol = str(Conf.SSLProrocol())
      aviableProtocols = [i for i in dir(ssl) if i.find('PROTOCOL') == 0]
      if sslprotocol and sslprotocol != "":
        if (sslprotocol in aviableProtocols):
          sslops['ssl_version'] = getattr(ssl, sslprotocol)
        else:
          message = "%s protocol is not provided." % sslprotocol
          message += "The following protocols are provided: %s" % str(aviableProtocols)
          gLogger.warn(message)

      self.log.debug(" - %s" % "\n - ".join(["%s = %s" % (k, sslops[k]) for k in sslops]))
      srv = tornado.httpserver.HTTPServer(self.__app, ssl_options=sslops, xheaders=True)
      port = Conf.HTTPSPort()
      srv.listen(port)
      self.__servers[('https', port)] = srv
    else:
      # when NGINX is used then the Conf.HTTPS return False, it means tornado
      # does not have to be configured using 443 port
      Conf.generateCAFile()  # if we use Nginx we have to generate the cas as well...
    return result

  def run(self):
    """
    Start web servers
    """
    bu = Conf.rootURL().strip("/")
    urls = []
    for proto, port in self.__servers:
      urls.append("%s://0.0.0.0:%s/%s/" % (proto, port, bu))
    self.log.always("Listening on %s" % " and ".join(urls))
    tornado.autoreload.add_reload_hook(self.__reloadAppCB)
    tornado.ioloop.IOLoop.instance().start()
