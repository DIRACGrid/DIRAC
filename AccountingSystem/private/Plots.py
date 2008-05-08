
try:
  from graphtool.graphs.common_graphs import *
except:
  raise Exception( "Missing GraphTool" )

from DIRAC import S_OK, S_ERROR

class TimeBarGraph( TimeGraph, BarGraph ):
  pass

class TimeStackedBarGraph( TimeGraph, StackedBarGraph ):
  pass

def generateTimedStackedBarPlot( fileName, data, metadata ):
  try:
    fn = file( fileName, "wb" )
  except:
    return S_ERROR( "Can't open %s" % filename )
  if not data:
    return S_ERROR( "No data for that selection" )
  plotter = TimeStackedBarGraph()
  plotter( data, fn, metadata )
  fn.close()
  return S_OK()

def generateQualityPlot( fileName, data, metadata ):
  try:
    fn = file( fileName, "wb" )
  except:
    return S_ERROR( "Can't open %s" % filename )
  if not data:
    return S_ERROR( "No data for that selection" )
  plotter = QualityMap()
  plotter( data, fn, metadata )
  fn.close()
  return S_OK()

def generateCumulativePlot( fileName, data, metadata ):
  try:
    fn = file( fileName, "wb" )
  except:
    return S_ERROR( "Can't open %s" % filename )
  if 'is_cumulative' not in metadata:
    metadata[ 'is_cumulative' ] = False
  if not data:
    return S_ERROR( "No data for that selection" )
  plotter = CumulativeGraph()
  plotter( data, fn, metadata )
  fn.close()
  return S_OK()