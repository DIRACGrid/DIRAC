from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

from dominate import document, tags as dom
from authlib.oauth2.rfc8414 import AuthorizationServerMetadata

from DIRAC.ConfigurationSystem.Client.Utilities import getAuthorizationServerMetadata


def collectMetadata(issuer=None, ignoreErrors=False):
  """ Collect metadata for DIRAC Authorization Server(DAS), a metadata format defines by IETF specification:
      https://datatracker.ietf.org/doc/html/rfc8414#section-2

      :param str issuer: issuer to set

      :return: dict -- dictionary is the AuthorizationServerMetadata object in the same time
  """
  result = getAuthorizationServerMetadata(issuer, ignoreErrors=ignoreErrors)
  if not result['OK']:
    raise Exception('Cannot prepare authorization server metadata. %s' % result['Message'])
  metadata = result['Value']
  for name, endpoint in [('jwks_uri', 'jwk'),
                         ('token_endpoint', 'token'),
                         ('userinfo_endpoint', 'userinfo'),
                         ('revocation_endpoint', 'revoke'),
                         ('redirect_uri', 'redirect'),
                         ('authorization_endpoint', 'authorization'),
                         ('device_authorization_endpoint', 'device')]:
    metadata[name] = metadata['issuer'].strip('/') + '/' + endpoint
  metadata['scopes_supported'] = ['g:', 'proxy', 'lifetime:']
  metadata['grant_types_supported'] = ['code', 'authorization_code', 'refresh_token',
                                       'urn:ietf:params:oauth:grant-type:device_code']
  metadata['response_types_supported'] = ['code', 'device', 'token']
  metadata['code_challenge_methods_supported'] = ['S256']
  return AuthorizationServerMetadata(metadata)


def getHTML(title, info=None, body=None, style=None, state=None, theme=None, icon=None):
  """ Provide HTML object

      :param str title: short name of the notification, e.g.: server error
      :param str info: some short description if needed, e.g.: Seems it because server down.
      :param body: dominate tag object, main content, e.g.: dom.pre(dom.code(result['Message']))
      :param str style: additional css style if needed, e.g.: '.card{color:red;}'
      :param str state: response state, if needed, e.g.: 404
      :param str theme: message color theme, the same that in bootstrap 5.
      :param str icon: awesome icon name

      :return: HTML document object
  """
  html = document("DIRAC - %s" % title)

  icon = icon or 'flask'
  theme = theme or 'secondary'
  if theme == 'warning':
    icon = icon or 'exclamation-triangle'
  elif theme == 'info':
    icon = icon or 'info'
  elif theme == 'success':
    icon = icon or 'check'
  elif theme == 'error':
    theme = 'danger'
    icon = icon or 'times'

  diracLogo = collectMetadata(ignoreErrors=True).get('logoURL', '')

  # Create head
  with html.head:
    # Provide icons
    dom.link(rel="icon", href="/static/core/img/icons/system/favicon.ico", type="image/x-icon")
    dom.script(src="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/5.15.4/js/all.min.js")
    # Enable bootstrap 5
    dom.link(rel='stylesheet', integrity="sha384-EVSTQN3/azprG1Anm3QDgpJLIm9Nao0Yz1ztcQTwFspd3yD65VohhpuuCOmLASjC",
             href="https://cdn.jsdelivr.net/npm/bootstrap@5.0.2/dist/css/bootstrap.min.css", crossorigin="anonymous")
    dom.script(src='https://cdn.jsdelivr.net/npm/bootstrap@5.0.2/dist/js/bootstrap.bundle.min.js',
               integrity="sha384-MrcW6ZMFYlzcLA8Nl+NtUVF0sA7MsXsP1UyJoMp4YLEuNSfAP+JcXn/tWtIaxVXM",
               crossorigin="anonymous")
    # Provide additional css
    style = ".card{transition:.3s;}.card:hover{transform:scale(1.03);}" + (style or '')
    dom.style(style)

  # Create body
  with html:
    # Background image
    dom.i(cls='position-absolute bottom-0 start-0 translate-middle-x fa fa-{icon} m-5 text-{theme}',
          style="font-size:40vw;z-index:-1;")

    # A4 page with align center
    with dom.div(cls="row vh-100 vw-100 justify-content-md-center align-items-center m-0"):
      with dom.div(cls='container', style="max-width: 600px;").add(dom.div(cls="row align-items-center")):
            
        # Logo
        dom.div(dom.img(src=diracLogo, cls="card-img px-2"), cls="col-md-6 my-3")
        # Information card
        with dom.div(cls="col-md-6 my-3"):
          
          # Show response state number
          if state and state != 200:
            dom.div(dom.h1(state, cls='text-center badge bg-{theme} text-wrap'), cls="row py-2")

          # Message title
          with dom.div(cls="row"):
            dom.div(dom.i(cls="fa fa-{icon} text-{theme}"), cls="col-auto")
            dom.div(title, cls='col-auto ps-0 pb-2 fw-bold')
          
          # Description
          if description:
            dom.small(dom.i(cls="fa fa-info text-info"))
            dom.small(info, cls="ps-1")
      
      # Add content
      if body:
        body

  return html
