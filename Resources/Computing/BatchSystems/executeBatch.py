

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

if __name__ == "__main__":

  import sys, json, urllib

  arguments = sys.argv[1]
  inputDict = json.loads( urllib.unquote( arguments ) )

  method = inputDict.pop('Method')
  batchSystem = inputDict.pop('BatchSystem')
  batch = locals()[batchSystem]()

  result = getattr( batch, method )( **inputDict )

  resultJson = urllib.quote( json.dumps( result ) )
  print "============= Start output ==============="
  print resultJson