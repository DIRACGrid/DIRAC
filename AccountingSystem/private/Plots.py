
try:
  from graphtool.graphs.common_graphs import *
except:
  raise Exception( "Missing GraphTool" )

from DIRAC import S_OK, S_ERROR

class TimeBarGraph( TimeGraph, BarGraph ):
  pass

class TimeStackedBarGraph( TimeGraph, StackedBarGraph ):
  pass

def generateTimedStackedBar( fileName, data, metadata ):
  try:
    fn = file( fileName, "wb" )
  except:
    return S_ERROR( "Can't open %s" % filename )
  TSBG = TimeStackedBarGraph()
  TSBG( data, fn, metadata )
  fn.close()
  return S_OK()
