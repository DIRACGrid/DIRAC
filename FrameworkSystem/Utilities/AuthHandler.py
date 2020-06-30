""" Handler to serve the DIRAC configuration data
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import re

from tornado import web, gen
from tornado.template import Template

from DIRAC import S_OK, S_ERROR, gConfig, gLogger
from DIRAC.FrameworkSystem.Client.NotificationClient import NotificationClient
from DIRAC.ConfigurationSystem.Client.Helpers.Resources import getProvidersForInstance

from DIRAC.FrameworkSystem.Client.AuthManagerClient import gSessionManager

from DIRAC.Core.Web.WebHandler import WebHandler, asyncGen, WErr

__RCSID__ = "$Id$"


class AuthHandler(WebHandler):
  OVERPATH = True
  AUTH_PROPS = "all"
  LOCATION = "/"

  def initialize(self):
    super(AuthHandler, self).initialize()
    self.args = {}
    for arg in self.request.arguments:
      if len(self.request.arguments[arg]) > 1:
        self.args[arg] = self.request.arguments[arg]
      else:
        self.args[arg] = self.request.arguments[arg][0] or ''
    return S_OK()

  @asyncGen
  def web_auth(self):
    """ Authentication endpoint, used:
          GET /auth/<IdP>?<options> -- submit authentication flow, retrieve session with status and describe
            IdP - Identity provider name for authentication
            options:
            email - email to get authentcation URL(optional)

          GET /auth/<session> -- will redirect to authentication endpoint
          GET /auth/<session>/status -- retrieve session with status and describe
            session - session number

          GET /auth/redirect?<options> -- redirect endpoint to catch authentication responce
            options - responce options

        :return: json
    """
    optns = self.overpath.strip('/').split('/')
    if not optns or len(optns) > 2:
      raise WErr(404, "Wrone way")
    result = getProvidersForInstance('Id')
    if not result['OK']:
      raise WErr(500, result['Message'])
    idPs = result['Value']
    idP = optns[0] if optns[0] in idPs else None
    session = re.match("([A-z0-9]+)?", optns[0]).group()

    if idP:
      # Create new authenticate session
      session = self.get_cookie(idP)
      self.log.info('Initialize "%s" authorization flow' % idP, 'with %s session' % session if session else '')
      result = yield self.threadTask(gSessionManager.submitAuthorizeFlow, idP, session)
      if not result['OK']:
        self.clear_cookie(idP)
        raise WErr(500, result['Message'])
      if result['Value']['Status'] == 'ready':
        self.set_cookie("TypeAuth", idP)
      elif result['Value']['Status'] == 'needToAuth':
        if self.args.get('email'):
          notify = yield self.threadTask(NotificationClient().sendMail, self.args['email'],
                                         'Authentication throught %s' % idP,
                                         'Please, go throught the link %s to authorize.' % result['Value']['URL'])
          if not notify['OK']:
            result['Value']['Comment'] = '%s\n%s' % (result['Value'].get('Comment') or '', notify['Message'])
        self.log.notice('%s authorization session "%s" provider was created' % (result['Value']['Session'], idP))
      else:
        raise WErr(500, 'Not correct status "%s" of %s' % (result['Value']['Status'], idP))
      self.finishJEncode(result['Value'])

    elif optns[0] == 'redirect':
      # Redirect endpoint for response
      self.log.info('REDIRECT RESPONSE:\n', self.request)
      if self.args.get('error'):
        raise WErr(500, '%s session crashed with error:\n%s\n%s' % (self.args.get('state') or '',
                                                                    self.args['error'],
                                                                    self.args.get('error_description') or ''))
      if 'state' not in self.args:
        raise WErr(404, '"state" argument not set.')
      if not self.args.get('state'):
        raise WErr(404, '"state" argument is empty.')
      self.log.info(self.args['state'], 'session, parsing authorization response %s' % self.args)
      result = yield self.threadTask(gSessionManager.parseAuthResponse, self.args, self.args['state'])
      if not result['OK']:
        raise WErr(500, result['Message'])
      comment = result['Value']['Comment']
      status = result['Value']['Status']
      t = Template('''<!DOCTYPE html>
        <html><head><title>Authetication</title>
          <meta charset="utf-8" /></head><body>
            %s <br>
            <script type="text/javascript">
              if ("%s" == "redirect") { window.open("%s","_self") }
              else { window.close() }
            </script>
          </body>
        </html>''' % (comment, status, comment))
      self.log.info('>>>REDIRECT:\n', comment)
      self.finish(t.generate())

    elif session:
      if optns[-1] == session:
        # Redirect to authentication endpoint
        self.log.info(session, 'authorization session flow.')
        result = yield self.threadTask(gSessionManager.getSessionAuthLink, session)
        if not result['OK']:
          raise WErr(500, '%s session not exist or expired!\n%s' % (session, result['Message']))
        self.log.notice('Redirect to', result['Value'])
        self.redirect(result['Value'])

      elif optns[-1] == 'status':
        # Get session authentication status
        self.log.info(session, 'session, get status of authorization.')
        result = yield self.threadTask(gSessionManager.getSessionStatus, session)
        if not result['OK']:
          raise WErr(500, result['Message'])
        self.set_cookie("TypeAuth", result['Value']['Provider'])
        self.set_cookie(result['Value']['Provider'], session)
        self.finishJEncode(result['Value'])

      else:
        raise WErr(404, "Wrone way")

    else:
      raise WErr(404, "Wrone way")
