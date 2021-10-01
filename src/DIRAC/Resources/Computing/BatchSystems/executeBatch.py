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


  arguments = sys.argv[1]
  inputDict = json.loads(urlunquote(arguments))

  method = inputDict.pop('Method')
  batchSystem = inputDict.pop('BatchSystem')
  batch = locals()[batchSystem]()

  try:
    result = getattr(batch, method)(**inputDict)
  except Exception:
    result = traceback.format_exc()

  resultJson = urlquote(json.dumps(result))
  print("============= Start output ===============")
  print(resultJson)
"""
