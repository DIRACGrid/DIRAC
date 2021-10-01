from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import json


class RMSEncoder(json.JSONEncoder):
    """This class is an encoder for the Requests, Operation and Files."""

    def default(self, obj):  # pylint: disable=method-hidden

        if hasattr(obj, "_getJSONData"):
            return obj._getJSONData()
        elif isinstance(obj, bytes):
            return obj.decode()
        else:
            return json.JSONEncoder.default(self, obj)
