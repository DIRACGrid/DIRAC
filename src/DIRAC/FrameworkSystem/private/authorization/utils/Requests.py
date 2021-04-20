from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from authlib.oauth2.rfc6749.util import scope_to_list, list_to_scope
from authlib.oauth2 import OAuth2Request as _OAuth2Request
from tornado.escape import json_decode

__RCSID__ = "$Id$"


class OAuth2Request(_OAuth2Request):
  """ OAuth request object """

  def addScopes(self, scopes):
    """ Add new scopes to query

        :param list scopes: scopes
    """
    # Remove "scope" argument from uri
    self.uri = re.sub(r"&scope(=[^&]*)?|^scope(=[^&]*)?&?", "", self.uri)
    # Add "scope" argument to uri with new scopes
    self.uri += "&scope=%s" % list_to_scope(list(set(scope_to_list(self.scope) + scopes))) or ''
    # Reinit all attributes with new uri
    self.__init__(self.method, self.uri)

  @property
  def groups(self):
    """ Serarch DIRAC groups in scopes

        :return: list
    """
    return [s.split(':')[1] for s in scope_to_list(self.scope) if s.startswith('g:')]

  def toDict(self):
    """ Convert class to dictionary

        :return: dict
    """
    return {'method': self.method, 'uri': self.uri}


def createOAuth2Request(request, method_cls=OAuth2Request, use_json=False):
  """ Create request object

      :param request: request
      :type request: object, dict
      :param object method_cls: returned class
      :param str use_json: if data is json

      :return: object -- `OAuth2Request`
  """
  if isinstance(request, method_cls):
    return request
  if isinstance(request, dict):
    return method_cls(request['method'], request['uri'], request.get('body'), request.get('headers'))
  if use_json:
    body = json_decode(request.body)
  else:
    body = {}
    for k, v in request.body_arguments.items():
      body[k] = ' '.join(v)
  return method_cls(request.method, request.full_url(), body, request.headers)
