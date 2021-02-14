import os
import stat
import requests
import urllib3

from DIRAC import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.Core.Utilities.JEncode import decode, encode
from DIRAC.ConfigurationSystem.Client.Helpers import Registry


class notebookAuth(object):
  """ The main goal of this class provide authentication with access token
  """
  def __init__(self, group, lifetime=3600 * 12, voms=False, aToken=None, proxyPath=None):
    """ C'r

        :param str group: requested group
        :param int lifetime: requested proxy lifetime
        :param bool voms: requested voms extension
        :param str aToken: access token or path
        :param str proxyPath: proxy path
    """
    self.log = gLogger.getSubLogger(__name__)
    # Defaulf location for proxy is /tmp/x509up_uXXXX
    self.pPath = proxyPath or '/tmp/x509up_u%s' % os.getuid()
    self.group = group
    self.lifetime = lifetime
    self.voms = voms
    # Default access token path for notebook: /var/run/secrets/egi.eu/access_token
    self.accessToken = aToken or '/var/run/secrets/egi.eu/access_token'
    # Load client metadata
    result = gConfig.getOptionsDict("/LocalInstallation/AuthorizationClient")
    if not result['OK']:
      raise Exception("Can't load client settings.")
    self.metadata = result['Value']
    # For this open client we don't verify ssl certs
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

  def getToken(self):
    """ Get access_token

        :return: S_OK(dict)/S_ERROR()
    """
    # Read noteboot access token
    if self.accessToken.startswith('/'):
      with open(self.accessToken, 'rb') as f:
        self.accessToken = f.read()

    # Fill authorization URL
    url = '%s/authorization' % self.metadata['issuer']
    url += '?client_id=%s' % self.metadata['client_id']
    url += '&redirect_uri=%s' % self.metadata['redirect_uri']
    url += '&response_type=%s' % self.metadata['response_type']
    if self.group:
      url += '&scope=g:%s' % self.group
    # For this version of code we use only CheckIn provider
    url += '&provider=CheckIn&access_token=%s' % self.accessToken
    try:
      r = requests.get(url, verify=False)
      r.raise_for_status()
      return S_OK(r.json())
    except requests.exceptions.Timeout:
      return S_ERROR('Authentication server is not answer.')
    except requests.exceptions.RequestException as ex:
      return S_ERROR(r.content or ex)
    except Exception as ex:
      return S_ERROR('Cannot read response: %s' % ex)

  def getProxyWithToken(self, token):
    """ Get proxy with token

        :param str token: access token

        :return: S_OK()/S_ERROR()
    """
    # Get REST endpoints from local CS
    confUrl = gConfig.getValue("/LocalInstallation/ConfigurationServerAPI")
    if not confUrl:
      return S_ERROR('Could not get configuration server API URL.')
    setup = gConfig.getValue("/DIRAC/Setup")
    if not setup:
      return S_ERROR('Could not get setup name.')

    # Get REST endpoints from ConfigurationService
    try:
      r = requests.get('%s/option?path=/Systems/Framework/Production/URLs/ProxyAPI' % confUrl, verify=False)
      r.raise_for_status()
      proxyAPI = r.text
      # proxyAPI = decode(r.text)[0]
    except requests.exceptions.Timeout:
      return S_ERROR('Time out')
    except requests.exceptions.RequestException as e:
      return S_ERROR(str(e))
    except Exception as e:
      return S_ERROR('Cannot read response: %s' % e)
    
    # Fill the proxy request URL
    # url = '%ss:%s/g:%s/proxy?lifetime=%s' % (proxyAPI, setup, self.group, self.lifetime)
    url = '%sproxy?lifetime=%s' % (proxyAPI, self.lifetime)
    voms = self.voms or Registry.getGroupOption(self.group, "AutoAddVOMS", False)
    if voms:
      url += '&voms=%s' % voms

    # Get proxy from REST API
    try:
      r = requests.get(url, headers={'Authorization': 'Bearer ' + token}, verify=False)
      r.raise_for_status()
      proxy = r.text
      # proxy = decode(r.text)[0]
    except requests.exceptions.Timeout:
      return S_ERROR('Time out')
    except requests.exceptions.RequestException as e:
      return S_ERROR(str(e))
    except Exception as e:
      return S_ERROR('Cannot read response: %s' % e)

    if not proxy:
      return S_ERROR("Result is empty.")

    self.log.notice('Saving proxy.. to %s..' % self.pPath)

    # Save proxy to file
    try:
      with open(self.pPath, 'w+') as fd:
        fd.write(proxy.encode("UTF-8"))
      os.chmod(self.pPath, stat.S_IRUSR | stat.S_IWUSR)
    except Exception as e:
      return S_ERROR("%s :%s" % (self.pPath, repr(e).replace(',)', ')')))

    self.log.notice('Proxy is saved to %s.' % self.pPath)
    return S_OK()
