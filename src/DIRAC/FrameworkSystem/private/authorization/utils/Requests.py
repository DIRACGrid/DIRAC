import re

from tornado.escape import json_decode
from authlib.common.encoding import to_unicode
from authlib.oauth2 import OAuth2Request as _OAuth2Request
from authlib.oauth2.rfc6749.util import scope_to_list
from urllib.parse import quote


class OAuth2Request(_OAuth2Request):
    """OAuth2 request object"""

    def addScopes(self, scopes):
        """Add new scopes to query

        :param list scopes: scopes
        """
        self.setQueryArguments(scope=list(set(scope_to_list(self.scope or "") + scopes)))

    def setQueryArguments(self, **kwargs):
        """Set query arguments"""
        for k in kwargs:
            # Quote value before add it to request query
            value = (
                "+".join([quote(str(v)) for v in kwargs[k]]) if isinstance(kwargs[k], list) else quote(str(kwargs[k]))
            )
            # Remove argument from uri
            query = re.sub(r"&{argument}(=[^&]*)?|^{argument}(=[^&]*)?&?".format(argument=k), "", self.query)
            # Add new one
            if query:
                query += "&"
            query += f"{k}={value}"
        # Re-init class
        self.__init__(self.method, to_unicode(self.path + "?" + query))

    @property
    def path(self):
        """URL path

        :return: str
        """
        return self.uri.replace("?%s" % (self.query or ""), "")

    @property
    def groups(self):
        """Search DIRAC groups in scopes

        :return: list
        """
        return [s.split(":")[1] for s in scope_to_list(self.scope or "") if s.startswith("g:") and s.split(":")[1]]

    @property
    def group(self):
        """Search DIRAC group in scopes

        :return: str
        """
        groups = [s.split(":")[1] for s in scope_to_list(self.scope or "") if s.startswith("g:") and s.split(":")[1]]
        return groups[0] if groups else None

    @property
    def provider(self):
        """Search IdP in scopes

        :return: str
        """
        return self.data.get("provider")

    @provider.setter
    def provider(self, provider):
        self.setQueryArguments(provider=provider)

    @property
    def sessionID(self):
        """Search IdP in scopes

        :return: str
        """
        return self.data.get("id")

    @sessionID.setter
    def sessionID(self, sessionID):
        self.setQueryArguments(id=sessionID)

    def toDict(self):
        """Convert class to dictionary

        :return: dict
        """
        return {"method": self.method, "uri": self.uri}


def createOAuth2Request(request, method_cls=OAuth2Request, use_json=False):
    """Create request object

    :param request: request
    :type request: object, dict
    :param object method_cls: returned class
    :param str use_json: if data is json

    :return: object -- `OAuth2Request`
    """
    if isinstance(request, method_cls):
        return request
    if isinstance(request, dict):
        return method_cls(request["method"], request["uri"], request.get("body"), request.get("headers"))
    if use_json:
        return method_cls(request.method, request.full_url(), json_decode(request.body), request.headers)
    body = {
        k: request.body_arguments[k][-1].decode("utf-8") for k in request.body_arguments if request.body_arguments[k]
    }
    return method_cls(request.method, request.full_url(), body, request.headers)
