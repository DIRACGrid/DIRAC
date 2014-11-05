###########################################################
#  Simple executor script for Batch class methods
###########################################################  

if __name__ == "__main__":

  import sys, json, urllib

  arguments = sys.argv[1]
  inputDict = json.loads( urllib.unquote( arguments ) )

  method = inputDict.pop('Method')
  backend = inputDict.pop('Backend')
  batch = locals()['%sBatch' % backend]()

  result = getattr( batch, method )( **inputDict )

  resultJson = urllib.quote( json.dumps( result ) )
  print "============= Start output ==============="
  print resultJson