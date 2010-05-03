import time
import datetime
try:
  from DIRAC.Core.Utilities.Graphs import barGraph, lineGraph, pieGraph, cumulativeGraph, qualityGraph, textGraph
except Exception, e:
  raise Exception( "Missing plotting lib: %s" % str( e ) )

from DIRAC import S_OK, S_ERROR, rootPath

def checkMetadata( metadata ):
  if 'span' in metadata:
    granularity = metadata[ 'span' ]
    if 'starttime' in metadata:
      metadata[ 'starttime' ] = metadata[ 'starttime' ] - metadata[ 'starttime' ] % granularity
    if 'endtime' in metadata:
      metadata[ 'endtime' ] = metadata[ 'endtime' ] - metadata[ 'endtime' ] % granularity
  if 'limit_labels' not in metadata:
    metadata[ 'limit_labels' ] = 30

def generateNoDataPlot( fileName, data, metadata ):
  try:
    fn = file( fileName, "wb" )
  except:
    return S_ERROR( "Can't open %s" % filename )
  textGraph( 'No data for this selection', fn, metadata )
  fn.close()
  return S_OK()

def generateTimedStackedBarPlot( fileName, data, metadata ):
  try:
    fn = file( fileName, "wb" )
  except:
    return S_ERROR( "Can't open %s" % fileName )
  checkMetadata( metadata )
  barGraph( data, fn, **metadata )
  fn.close()
  return S_OK()

def generateQualityPlot( fileName, data, metadata ):
  try:
    fn = file( fileName, "wb" )
  except:
    return S_ERROR( "Can't open %s" % fileName )
  checkMetadata( metadata )
  metadata[ 'legend' ] = False
  #HACK: Pad a bit to the left until the proper padding is calculated
  maxKeyLength = max( [ len( key ) for key in data ] )
  metadata[ 'limit_labels' ] = 9999
  metadata[ 'sort_labels' ] = 'alpha'
  metadata[ 'plot_left_padding' ] = int( maxKeyLength * 2.5 )
  qualityGraph( data, fn, **metadata )
  fn.close()
  return S_OK()

def generateCumulativePlot( fileName, data, metadata ):
  try:
    fn = file( fileName, "wb" )
  except:
    return S_ERROR( "Can't open %s" % fileName )
  checkMetadata( metadata )
  if 'sort_labels' not in metadata:
    metadata[ 'sort_labels' ] = 'last_value'
  cumulativeGraph( data, fn, **metadata )
  fn.close()
  return S_OK()

def generateStackedLinePlot( fileName, data, metadata ):
  try:
    fn = file( fileName, "wb" )
  except:
    return S_ERROR( "Can't open %s" % filename )
  checkMetadata( metadata )
  lineGraph( data, fn, **metadata )
  fn.close()
  return S_OK()

def generatePiePlot( fileName, data, metadata ):
  try:
    fn = file( fileName, "wb" )
  except:
    return S_ERROR( "Can't open %s" % fileName )
  checkMetadata( metadata )
  pieGraph( data, fn, **metadata )
  fn.close()
  return S_OK()
