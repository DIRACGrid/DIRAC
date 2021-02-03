from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
###########################################################
#
#  Simple executor script for Batch class methods.
#
#  The script is concatenated on the fly with the required
#  batch system class definition
#
#  15.11.2014
#  Author: A.T.
#
###########################################################

executeBatchContent = """
if __name__ == "__main__":

  import sys
  import json
  from six.moves.urllib.parse import quote as urlquote
  from six.moves.urllib.parse import unquote as urlunquote


  arguments = sys.argv[1]
  inputDict = json.loads(urlunquote(arguments))

  method = inputDict.pop('Method')
  batchSystem = inputDict.pop('BatchSystem')
  batch = locals()[batchSystem]()

  try:
    result = getattr(batch, method)(**inputDict)
  except Exception as x:
    result = 'Exception: %s' % str(x)

  resultJson = urlquote(json.dumps(result))
  print("============= Start output ===============")
  print(resultJson)
"""
