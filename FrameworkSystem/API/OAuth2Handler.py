""" Handler to serve the DIRAC configuration data
"""

__RCSID__ = "$Id$"

import json
import time
import tornado
from tornado import web, gen
from tornado.template import Template

from DIRAC import S_OK, S_ERROR, gConfig, gLogger
from DIRAC.Core.HTTP.Lib.WebHandler import WebHandler, asyncGen
from DIRAC.FrameworkSystem.Utilities.OAuth2 import OAuth2
from DIRAC.FrameworkSystem.Client.OAuthClient import OAuthClient
from DIRAC.FrameworkSystem.Client.NotificationClient import NotificationClient

from DIRAC.Core.Security.X509Chain import X509Chain

gOAuthCli = OAuthClient()

class OAuth2Handler( WebHandler ):
  OFF = False
  AUTH_PROPS = "all"
  LOCATION = "/oauth2"

  @asyncGen
  def web_oauth( self ):
    """ Method to push auth
    :return: requested data
    """
    gLogger.debug('Get oauth request:\n %s' % self.request)
    args = self.request.arguments
    if args:
      # Redirect to authentication endpoint
      if 'getlink' in args:
        if not args['getlink']:
          self.finish('"getlink" option is empty!')
        else:
          gLogger.debug('Redirection..')
          result = yield self.threadTask( gOAuthCli.get_link_by_state,args['getlink'][0] )
          if not result['OK']:
            gLogger.error(result['Message'])
            self.finish('Link has expired!')
          else:
            gLogger.debug('Redirect url: %s' % result['Value'])
            self.redirect(result['Value'])
      # Create new authenticate session
      elif 'IdP' in args:
        idp = args['IdP'][0]
        result = yield self.threadTask( gOAuthCli.create_auth_request_uri,idp )
        if not result['OK']:
          gLogger.error(result['Message'])
          raise tornado.web.HTTPError(404, result['Message'])
        state = result['Value']['state']
        # url = '%s/oauth?getlink=%s' % (gConfig.getValue("Systems/Framework/Production/URLs/API/OAuth"),state)
        if 'email' in args:
          result = yield self.threadTask( NotificationClient().sendMail,args['email'],'Authentication throught %s IdP' % idp,
                              'Please, go throught the link %s to authorize.' % url )
          result['Value'] = {'state':state}
        # result['Value']['url'] = url
        gLogger.debug('Created authorized session "%s" for "%s" IdP' % (state,idp))
        self.finish( json.dumps(result) )

  @asyncGen
  def web_redirect( self ):
    """ Method to push responses to OAuth2Service
    :return: requested data
    """
    gLogger.debug('Get redirect request:\n %s' % self.request)
    args = self.request.arguments
    if args:
      # Parse response of authentication request
      if 'code' in args:
        code = args['code'][0]
        if not 'state' in args:
          self.finish('No state argument found.')
        else:  
          state = args['state'][0]
          gLogger.debug('Parsing authentication response\n Code: %s' % code)
          result = yield self.threadTask( gOAuthCli.parse_auth_response,code,state )
          if not result['OK']:
            gLogger.error(result['Message'])
            raise tornado.web.HTTPError(404, result['Message'])
          else:
            oDict = result['Value']
            if oDict['redirect']:
              self.redirect(oDict['redirect'])
            else:
              t = Template('''<!DOCTYPE html>
              <html><head><title>Authetication</title>
                <meta charset="utf-8" /></head><body>
                  {{ Messages }} <br>
                  Done! You can close this window.
                  <script type="text/javascript">
                    window.close();
                  </script>
                </body>
              </html>''')
              gLogger.debug('Complite authentication to "%s" session' % state)
              self.finish(t.generate(Messages=oDict['Messages']))
      # Get status of authentication
      elif 'status' in args:
        state = args['status'][0]
        voms = None
        group = None
        time_out = None
        needProxy = False
        if 'voms' in args:
          voms = args['voms'][0]
        if 'group' in args: 
          group = args['group'][0]
        if 'proxy' in args:
          needProxy = args['proxy'][0]
        if 'time_out' in args:
          time_out = args['time_out'][0]
        if 'proxyLifeTime' in args:
          proxyLifeTime = args['proxyLifeTime'][0]
        gLogger.debug('Read authentication status of "%s" session.' % state)
        result = yield self.threadTask( gOAuthCli.waitStateResponse,state,group,needProxy,voms,proxyLifeTime,time_out ) 
        if not result['OK']:
          gLogger.error(result['Message'])
          raise tornado.web.HTTPError(404, result['Message'])
        self.finish( json.dumps(result) )     
      else:
        self.finish( json.dumps(S_ERROR('No supported args!')) )
    # Switch options in hash to request parameters
    else:
      t = Template('''<!DOCTYPE html>
        <html><head><title>Authetication</title>
          <meta charset="utf-8" /></head><body>
          Waiting...
            <script type="text/javascript">
              var xhr = new XMLHttpRequest();
              xhr.onreadystatechange = function() {
                if (xhr.readyState === 4) {
                  if (xhr.response == 'Done') { 
                    opener.location.protocol = "https:"                    
                  } else {
                    opener.alert('Not registered user')
                  }
                  close()
                }
              }
              xhr.open("GET", "{{ redirect_uri }}?" + location.hash.substring(1), true);
              xhr.send();
            </script>
          </body>
        </html>''')
      self.finish(t.generate(redirect_uri = "restapi/redirect"))

  @asyncGen
  def post( self ):
    """ Post method """
    pass