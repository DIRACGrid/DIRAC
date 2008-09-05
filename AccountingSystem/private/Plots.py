import time
try:
  from graphtool.graphs.common_graphs import *
except:
  raise Exception( "Missing GraphTool" )

from DIRAC import S_OK, S_ERROR

def convertUTCToLocal( metadata, data ):
  """
  Convert epoch times from utc to local
  bucketsData must be a list of lists where each list contains
    - field 0: datetime
    - field 1: bucketLength
    - fields 2-n: numericalFields
  """
  for mF in ( 'starttime', 'endtime' ):
    if mF in metadata:
      metadata[ mF ] = metadata[ mF ] - time.altzone
  for kF in data:
    convertedData = {}
    for iP in data[ kF ]:
      convertedData[ iP - time.altzone ] = data[kF][iP]
    data[kF] = convertedData
  return metadata, data

def alignToGranularity( metadata ):
  if 'span' in metadata:
    granularity = metadata[ 'span' ]
    if 'starttime' in metadata:
      metadata[ 'starttime' ] = metadata[ 'starttime' ] - metadata[ 'starttime' ] % granularity
    if 'endtime' in metadata:
      metadata[ 'endtime' ] = metadata[ 'endtime' ] - metadata[ 'endtime' ] % granularity + granularity

class TimeBarGraph( TimeGraph, BarGraph ):
  pass

class TimeStackedBarGraph( TimeGraph, StackedBarGraph ):

  def make_stacked_bar( self, points, bottom, color ):

    if not 'skipEdgeColor' in self.metadata or not self.metadata[ 'skipEdgeColor' ]:
      return super( TimeStackedBarGraph, self ).make_stacked_bar( points, bottom, color )
    else:
      if bottom == None:
        bottom = {}
      tmp_x = []; tmp_y = []; tmp_b = []

      for key in points.keys():
        if self.is_timestamps:
          key_date = datetime.datetime.utcfromtimestamp( key )
          key_val = date2num( key_date )
        else:
          key_val = key
        tmp_x.append( key_val )
        tmp_y.append( points[key] )
        if not bottom.has_key( key ):
          if self.log_yaxis:
              bottom[key] = 0.001
          else:
              bottom[key] = 0
        tmp_b.append( bottom[key] )
        bottom[key] += points[key]
      if len( tmp_x ) == 0:
        return bottom, None
      width = float(self.width)
      if self.is_timestamps:
          width = float(width) / 86400.0
      elif self.string_mode:
          tmp_x = [i + .1*width for i in tmp_x]
          width = .8 * width
      bars = self.ax.bar( tmp_x, tmp_y, bottom=tmp_b, width=width, color=color, edgecolor=color )
      setp( bars, linewidth=0.5 )
      return bottom, bars

def generateTimedStackedBarPlot( fileName, data, metadata ):
  try:
    fn = file( fileName, "wb" )
  except:
    return S_ERROR( "Can't open %s" % filename )
  if not data:
    return S_ERROR( "No data for that selection" )
  metadata, data = convertUTCToLocal( metadata, data )
  alignToGranularity( metadata )
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
  metadata, data = convertUTCToLocal( metadata, data )
  alignToGranularity( metadata )
  plotter( data, fn, metadata )
  fn.close()
  return S_OK()

def generateCumulativePlot( fileName, data, metadata ):
  try:
    fn = file( fileName, "wb" )
  except:
    return S_ERROR( "Can't open %s" % filename )
  if 'is_cumulative' not in metadata:
    metadata[ 'is_cumulative' ] = True
  if not data:
    return S_ERROR( "No data for that selection" )
  plotter = CumulativeGraph()
  metadata, data = convertUTCToLocal( metadata, data )
  alignToGranularity( metadata )
  plotter( data, fn, metadata )
  fn.close()
  return S_OK()

def generatePiePlot( fileName, data, metadata ):
  try:
    fn = file( fileName, "wb" )
  except:
    return S_ERROR( "Can't open %s" % filename )
  plotter = PieGraph()
  plotter( data, fn, metadata )
  fn.close()
  return S_OK()