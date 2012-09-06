import cStringIO
try:
  from DIRAC.Core.Utilities.Graphs import barGraph, lineGraph, pieGraph, cumulativeGraph, qualityGraph, textGraph
except Exception, plotException:
  raise Exception( "Missing plotting lib: %s" % str( plotException ) )

from DIRAC import S_OK, S_ERROR

def checkMetadata( metadata ):
  if 'span' in metadata:
    granularity = metadata[ 'span' ]
    if 'starttime' in metadata:
      metadata[ 'starttime' ] = metadata[ 'starttime' ] - metadata[ 'starttime' ] % granularity
    if 'endtime' in metadata:
      metadata[ 'endtime' ] = metadata[ 'endtime' ] - metadata[ 'endtime' ] % granularity
  if 'limit_labels' not in metadata:
    metadata[ 'limit_labels' ] = 9999999

def generateNoDataPlot( fileName, data, metadata ):
  try:
    fn = file( fileName, "wb" )
  except:
    return S_ERROR( "Can't open %s" % fileName )
  text = "No data for this selection for the plot: %s" % metadata['title']
  textGraph( text, fn, metadata )
  fn.close()
  return S_OK()

def generateErrorMessagePlot( msgText ):
  fn = cStringIO.StringIO()
  textGraph( msgText, fn, {} )
  data = fn.getvalue()
  fn.close()
  return S_OK( data )


def generateTimedStackedBarPlot( fileName, data, metadata ):
  try:
    fn = file( fileName, "wb" )
  except:
    return S_ERROR( "Can't open %s" % fileName )
  checkMetadata( metadata )
  for key, value in ( ( 'sort_labels', 'sum' ), ( 'legend_unit', '%' ) ):
    if key not in metadata:
      metadata[ key ] = value
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
  lineGraph( data, fn, **metadata )
  fn.close()
  return S_OK()

def generateStackedLinePlot( fileName, data, metadata ):
  try:
    fn = file( fileName, "wb" )
  except:
    return S_ERROR( "Can't open %s" % fileName )
  checkMetadata( metadata )
  for key, value in ( ( 'sort_labels', 'sum' ), ( 'legend_unit', '%' ) ):
    if key not in metadata:
      metadata[ key ] = value
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
