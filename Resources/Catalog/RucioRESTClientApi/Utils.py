# -*- coding: utf-8 -*-
# Copyright 2012-2020 CERN
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
# - Vincent Garonne <vincent.garonne@cern.ch>, 2012-2018
# - Thomas Beermann <thomas.beermann@cern.ch>, 2012-2018
# - Mario Lassnig <mario.lassnig@cern.ch>, 2012-2020
# - Cedric Serfon <cedric.serfon@cern.ch>, 2013-2020
# - Ralph Vigne <ralph.vigne@cern.ch>, 2013
# - Joaquín Bogado <jbogado@linti.unlp.edu.ar>, 2015-2018
# - Martin Barisits <martin.barisits@cern.ch>, 2016-2020
# - Brian Bockelman <bbockelm@cse.unl.edu>, 2018
# - Tobias Wegner <twegner@cern.ch>, 2018-2019
# - Hannes Hansen <hannes.jakob.hansen@cern.ch>, 2018-2019
# - Tomas Javurek <tomas.javurek@cern.ch>, 2019-2020
# - Andrew Lister <andrew.lister@stfc.ac.uk>, 2019
# - James Perry <j.perry@epcc.ed.ac.uk>, 2019
# - Gabriele Fronze' <gfronze@cern.ch>, 2019
# - Jaroslav Guenther <jaroslav.guenther@cern.ch>, 2019-2020
# - Eli Chadwick <eli.chadwick@stfc.ac.uk>, 2020
# - Patrick Austin <patrick.austin@stfc.ac.uk>, 2020
# - Benedikt Ziemons <benedikt.ziemons@cern.ch>, 2020
"""
original: https://github.com/rucio/rucio/blob/master/lib/rucio/common/utils.py

A collection of utilities. These utilities are
used by modified Rucio client code to be used by Dirac.
Names modified to camelCase, as required by Dirac.

"""
import os
import tempfile
import json
import re
from six import string_types
try:
  # Python 2
  from urllib import urlencode, quote
except ImportError:
  # Python 3
  from urllib.parse import urlencode, quote


def datetimeParser(dct):
  """ datetime parser
  """
  for k, v in list(dct.items()):
    if isinstance(v, string_types) and re.search(" UTC", v):
      try:
        dct[k] = datetime.datetime.strptime(v, DATE_FORMAT)
      except Exception:
        pass
  return dct


def parseResponse(data):
  """
  JSON render function
  """
  ret_obj = None
  try:
    ret_obj = data.decode('utf-8')
  except AttributeError:
    ret_obj = data

  return json.loads(ret_obj, object_hook=datetimeParser)


def buildURL(url, path=None, params=None, doseq=False):
  """
  Utility function to build an url for requests to the rucio system.
  If the optional parameter doseq is evaluates to True, individual key=value pairs
  separated by '&' are generated for each element of the value sequence for the key.
  """
  complete_url = url
  if path is not None:
    complete_url += "/" + path
  if params is not None:
    complete_url += "?"
    if isinstance(params, str):
      complete_url += quote(params)
    else:
      complete_url += urlencode(params, doseq=doseq)
  return complete_url


def getTempDir():
  return os.path.abspath(tempfile.gettempdir())


class APIEncoder(json.JSONEncoder):
  """
  Proprietary JSONEconder subclass used by the json render function.
  This is needed to address the encoding of special values.
  """

  def default(self, obj):  # pylint: disable=E0202
    if isinstance(obj, datetime.datetime):
      # convert any datetime to RFC 1123 format
      return date_to_str(obj)
    elif isinstance(obj, (datetime.time, datetime.date)):
      # should not happen since the only supported date-like format
      # supported at dmain schema level is 'datetime' .
      return obj.isoformat()
    elif isinstance(obj, datetime.timedelta):
      return obj.days * 24 * 60 * 60 + obj.seconds
    elif isinstance(obj, EnumSymbol):
      return obj.description
    elif isinstance(obj, (InternalAccount, InternalScope)):
      return obj.external
    return json.JSONEncoder.default(self, obj)


def render_json(**data):
  """
  JSON render function
  """
  return json.dumps(data, cls=APIEncoder)
