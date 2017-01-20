
# This is a temporary file that should go away as soon as this PR is merged and released
# https://gitlab.cern.ch/fts/fts-rest/merge_requests/9



#
#   See www.eu-emi.eu for details on the copyright holders
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

#pylint: disable=undefined-variable,bad-indentation

try:
    import simplejson as json
except ImportError:
    import json
import logging
import requests
import tempfile
from fts3.rest.client.exceptions import * #pylint: disable=import-error
import os

class Request(object):

    def __init__(self, ucert, ukey, capath=None, passwd=None, verify=False, access_token=None, connectTimeout=30, timeout=30):
        self.ucert = ucert
        self.ukey  = ukey
        self.passwd = passwd
        self.access_token = access_token
        self.verify = verify
        # Disable the warnings
        if not verify:
          requests.packages.urllib3.disable_warnings()

        self.connectTimeout = connectTimeout
        self.timeout = timeout

        self.session = requests.Session()


    def _handle_error(self, url, code, response_body=None):
        # Try parsing the response, maybe we can get the error message
        message = None
        response = None
        if response_body:
            try:
                response = json.loads(response_body)
                if 'message' in response:
                    message = response['message']
                else:
                    message = response_body
            except:
                message = response_body

        if code == 207:
            try:
                raise ClientError('\n'.join(map(lambda m: m['http_message'], response)))
            except (KeyError, TypeError):
                raise ClientError(message)
        elif code == 400:
            if message:
                raise ClientError('Bad request: ' + message)
            else:
                raise ClientError('Bad request')
        elif 401 <= code <= 403:
            raise Unauthorized()
        elif code == 404:
            raise NotFound(url, message)
        elif code == 419:
            raise NeedDelegation('Need delegation')
        elif code == 424:
            raise FailedDependency('Failed dependency')
        elif 404 < code < 500:
            raise ClientError(str(code))
        elif code == 503:
            raise TryAgain(str(code))
        elif code >= 500:
            raise ServerError(str(code))

    def method(self, method, url, body=None, headers=None):
        _headers = {'Accept': 'application/json'}
        if headers:
            _headers.update(headers)
        if self.access_token:
            _headers['Authorization'] = 'Bearer ' + self.access_token

        response = self.session.request(method=method, url=str(url),
                             data=body, headers=_headers, verify = self.verify,
                             timeout=(self.connectTimeout, self.timeout),
                             cert=(self.ucert, self.ukey))


        #log.debug(response.text)

        self._handle_error(url, response.status_code, response.text)

        return str( response.text )


__all__ = ['Request']
