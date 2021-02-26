from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from authlib.oauth2 import OAuth2Request as _OAuth2Request
from tornado.escape import json_decode

__RCSID__ = "$Id$"


class OAuth2Request(_OAuth2Request):
  """ OAuth request object """
  # def __init__(self, method, uri, body=None, headers=None):
  #   super(OAuth2Request, self).__init__(method, uri.replace('http://', 'https://'), body, headers)

  def toDict(self):
    """ Convert class to dictionary

        :return: dict
    """
    return {'method': self.method,
            'uri': self.uri,
            'body': self.body,
            'headers': dict(self.headers)}


def createOAuth2Request(request, method_cls=OAuth2Request, use_json=False):
  """ Create request object

      :param request: request
      :type request: object, dict
      :param object method_cls: returned class
      :param str use_json: if data is json

      :return: object
  """
  if isinstance(request, method_cls):
    return request
  if isinstance(request, dict):
    return method_cls(request['method'], request['uri'],
                      request.get('body'), request.get('headers'))
  if use_json:
    body = json_decode(request.body)
  else:
    body = {}
    for k, v in request.body_arguments.items():
      body[k] = ' '.join(v)
  return method_cls(request.method, request.full_url(), body, request.headers)
