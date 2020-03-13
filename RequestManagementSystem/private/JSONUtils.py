import json


class RMSEncoder(json.JSONEncoder):
  """ This class is an encoder for the Requests, Operation and Files.
  """

  def default(self, obj):  # pylint: disable=method-hidden

    if hasattr(obj, '_getJSONData'):
      return obj._getJSONData()
    else:
      return json.JSONEncoder.default(self, obj)
