from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

###########################################################
#
#  Simple executor script for Batch class methods.
#
#  The script is concatenated on the fly with the required
#  batch system class definition.
#
#  NB: This scipt is executed using the local (to the WN)
#  python version, so support for py2 and py3 is necessary.
#
#  15.11.2014
#  Author: A.T.
#
###########################################################

executeBatchContent = """
if __name__ == "__main__":

  import sys
  import json
  import traceback
  try:
    from six.moves.urllib.parse import quote as urlquote
    from six.moves.urllib.parse import unquote as urlunquote
  except ImportError:
    try:
      from urllib import unquote as urlunquote
      from urllib import quote as urlquote
    except ImportError:
      from urllib.parse import quote as urlquote
      from urllib.parse import unquote as urlunquote


  # Read options from JSON file
  optionsFilePath = sys.argv[1]
  with open(optionsFilePath, 'r') as f:
    inputDict = json.load(f)

  method = inputDict.pop('Method')
  batchSystem = inputDict.pop('BatchSystem')
  batch = locals()[batchSystem]()

  try:
    result = getattr(batch, method)(**inputDict)
  except Exception:
    # Wrap the traceback in a proper error structure
    result = {
      'Status': -1,
      'Message': 'Exception during batch method execution',
      'Traceback': traceback.format_exc()
    }

  # Write result to JSON file
  resultFilePath = optionsFilePath.replace('.json', '_result.json')
  with open(resultFilePath, 'w') as f:
    json.dump(result, f)
"""
