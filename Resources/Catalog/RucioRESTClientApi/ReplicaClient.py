# -*- coding: utf-8 -*-
# Copyright 2013-2020 CERN for the benefit of the ATLAS collaboration.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Authors:
# - Vincent Garonne <vgaronne@gmail.com>, 2013-2018
# - Mario Lassnig <mario.lassnig@cern.ch>, 2013-2019
# - Cedric Serfon <cedric.serfon@cern.ch>, 2014-2015
# - Ralph Vigne <ralph.vigne@cern.ch>, 2015
# - Brian Bockelman <bbockelm@cse.unl.edu>, 2018
# - Martin Barisits <martin.barisits@cern.ch>, 2018
# - Hannes Hansen <hannes.jakob.hansen@cern.ch>, 2019
# - Andrew Lister <andrew.lister@stfc.ac.uk>, 2019
# - Luc Goossens <luc.goossens@cern.ch>, 2020
# - Benedikt Ziemons <benedikt.ziemons@cern.ch>, 2020
#
"""
  Based on:

   https://github.com/rucio/rucio/blob/master/lib/rucio/client/replicaclient.py

  Modified to be used by Dirac. Modifications include:
     - only use a limited number of methods from the original class, which are needed for the Dirac Rucio
       File Catalog Client.
     - eliminate any references to Rucio config file. All values needed to configure the client are
       obtained from Dirac CS,
     - avoid throwing exceptions. They are converted to Dirac S_ERROR or S_OK objects.
"""

try:
  from urllib import quote_plus
except ImportError:
  from urllib.parse import quote_plus

import os
import random
from datetime import datetime
from json import dumps, loads
from requests.status_codes import codes

from DIRAC.Resources.Catalog.RucioRESTClientApi.BaseClient import BaseClient
from DIRAC.Resources.Catalog.RucioRESTClientApi import Utils
from DIRAC import gLogger, S_OK, S_ERROR


class ReplicaClient(BaseClient):
  """Replica client class for working with replicas"""

  REPLICAS_BASEURL = 'replicas'

  def __init__(self, rucioHost=None, authHost=None, account=None):
    super(ReplicaClient, self).__init__(rucioHost, authHost, account)

  def listReplicas(self, dids, schemes=None, unavailable=False,
                   all_states=False, metalink=False, rse_expression=None,
                   client_location=None, sort=None, domain=None,
                   resolve_archives=True, resolve_parents=False,
                   updated_after=None):
    """
    List file replicas for a list of data identifiers (DIDs).

    :param dids: The list of data identifiers (DIDs) like :
        [{'scope': <scope1>, 'name': <name1>}, {'scope': <scope2>, 'name': <name2>}, ...]
    :param schemes: A list of schemes to filter the replicas. (e.g. file, http, ...)
    :param unavailable: Also include unavailable replicas in the list.
    :param metalink: ``False`` (default) retrieves as JSON,
                     ``True`` retrieves as metalink4+xml.
    :param rse_expression: The RSE expression to restrict replicas on a set of RSEs.
    :param client_location: Client location dictionary for PFN modification {'ip', 'fqdn', 'site'}
    :param sort: Sort the replicas: ``geoip`` - based on src/dst IP topographical distance
                                    ``closeness`` - based on src/dst closeness
                                    ``dynamic`` - Rucio Dynamic Smart Sort (tm)
    :param domain: Define the domain. None is fallback to 'wan', otherwise 'wan, 'lan', or 'all'
    :param resolve_archives: When set to True, find archives which contain the replicas.
    :param resolve_parents: When set to True, find all parent datasets which contain the replicas.
    :param updated_after: epoch timestamp or datetime object (UTC time), only return replicas updated after this time
    :returns: A list of dictionaries with replica information.
    """
    data = {'dids': dids,
            'domain': domain}

    if schemes:
      data['schemes'] = schemes
    if unavailable:
      data['unavailable'] = True
    data['all_states'] = all_states

    if rse_expression:
      data['rse_expression'] = rse_expression

    if client_location:
      data['client_location'] = client_location

    if sort:
      data['sort'] = sort

    if updated_after:
      if isinstance(updated_after, datetime):
        # encode in UTC string with format '%Y-%m-%dT%H:%M:%S'  e.g. '2020-03-02T12:01:38'
        data['updated_after'] = updated_after.strftime('%Y-%m-%dT%H:%M:%S')
      else:
        data['updated_after'] = updated_after

    data['resolve_archives'] = resolve_archives

    data['resolve_parents'] = resolve_parents

    path = '/'.join([self.REPLICAS_BASEURL, 'list'])
    url = os.path.join(self.rucioHost, path)

    headers = {}
    if metalink:
      headers['Accept'] = 'application/metalink4+xml'

    # pass json dict in querystring
    r = self._sendRequest(url, headers=headers, type='POST', data=dumps(data), stream=False)
    if r.status_code == codes.ok:
      if not metalink:
        return S_OK(Utils.parseResponse(r.text))
      return S_OK(r.text)
    return S_ERROR(self._getError(status_code=r.status_code, data=r.content))

  def addReplica(self, rse, scope, name, bytes, adler32, pfn=None, md5=None, meta={}):
    """
    Add file replicas to a RSE.

    :param rse: the RSE name.
    :param scope: The scope of the file.
    :param name: The name of the file.
    :param bytes: The size in bytes.
    :param adler32: adler32 checksum.
    :param pfn: PFN of the file for non deterministic RSE.
    :param md5: md5 checksum.
    :param meta: Metadata attributes.
    :return: True if files were created successfully.
    """
    dict = {'scope': scope, 'name': name, 'bytes': bytes, 'meta': meta, 'adler32': adler32}
    if md5:
      dict['md5'] = md5
    if pfn:
      dict['pfn'] = pfn
    return self.addReplicas(rse=rse, files=[dict])

  def addReplicas(self, rse, files, ignore_availability=True):
    """
    Bulk add file replicas to a RSE.

    :param rse: the RSE name.
    :param files: The list of files. This is a list of DIDs like :
        [{'scope': <scope1>, 'name': <name1>}, {'scope': <scope2>, 'name': <name2>}, ...]
    :param ignore_availability: Ignore the RSE blacklisting.
    :return: True if files were created successfully.
    """
    path = self.REPLICAS_BASEURL
    url = os.path.join(self.rucioHost, path)
    data = {'rse': rse, 'files': files, 'ignore_availability': ignore_availability}
    r = self._sendRequest(url, type='POST', data=Utils.render_json(**data))
    if r.status_code == codes.created:
      return S_OK(True)
    return S_ERROR(self._getError(status_code=r.status_code, data=r.content))

  def deleteReplicas(self, rse, files, ignore_availability=True):
    """
    Bulk delete file replicas from a RSE.

    :param rse: the RSE name.
    :param files: The list of files. This is a list of DIDs like :
        [{'scope': <scope1>, 'name': <name1>}, {'scope': <scope2>, 'name': <name2>}, ...]
    :param ignore_availability: Ignore the RSE blacklisting.
    :return: True if files have been deleted successfully.
    """

    path = self.REPLICAS_BASEURL
    url = os.path.join(self.rucioHost, path)
    data = {'rse': rse, 'files': files, 'ignore_availability': ignore_availability}
    r = self._sendRequest(url, type='DEL', data=Utils.render_json(**data))
    if r.status_code == codes.ok:
      return S_OK(True)
    return S_ERROR(self._getError(status_code=r.status_code, data=r.content))
