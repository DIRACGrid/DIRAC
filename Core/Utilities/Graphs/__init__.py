########################################################################
# $HeadURL$
########################################################################

""" DIRAC Graphs package provides tools for creation of various plots to provide
    graphical representation of the DIRAC Monitoring and Accounting data
    
    The DIRAC Graphs package is derived from the GraphTool plotting package of the
    CMS/Phedex Project by ... <to be added>
"""

__RCSID__ = "$Id$"

import DIRAC

matplotlib = True
try:
  from DIRAC.Core.Utilities.Graphs.Graph import Graph
  from DIRAC.Core.Utilities.Graphs.GraphUtilities import evalPrefs
except:
  matplotlib = False
  pass

import time

common_prefs = {
  'background_color':'white',
  'figure_padding':12,
  'plot_grid':'1:1',
  'plot_padding':0,
  'frame':'On',
  'font' : 'Lucida Grande',
  'font_family' : 'sans-serif',
  'dpi':100,
  'legend':True,
  'legend_position':'bottom',
  'legend_max_rows':99,
  'legend_max_columns':4,
  'square_axis':False
}

graph_large_prefs = {
  'width':1000,
  'height':700,
  'text_size':8,
  'subtitle_size':10,
  'subtitle_padding':5,
  'title_size':15,
  'title_padding':5,
  'text_padding':5,
  'figure_padding':15,
  'plot_title_size':12,
  'legend_width':980,
  'legend_height':150,
  'legend_padding':20,
  'limit_labels':15,
  'graph_time_stamp':True
}

graph_normal_prefs = {
  'width':800,
  'height':600,
  'text_size':8,
  'subtitle_size':10,
  'subtitle_padding':5,
  'title_size':15,
  'title_padding':10,
  'text_padding':5,
  'figure_padding':12,
  'plot_title_size':12,
  'legend_width':780,
  'legend_height':120,
  'legend_padding':20,
  'limit_labels':15,
  'graph_time_stamp':True
}

graph_small_prefs = {
  'width':450,
  'height':330,
  'text_size':10,
  'subtitle_size':5,
  'subtitle_padding':4,
  'title_size':10,
  'title_padding':6,
  'text_padding':3,
  'figure_padding':10,
  'plot_title_size':8,
  'legend_width':430,
  'legend_height':50,
  'legend_padding':10,
  'limit_labels':15,
  'graph_time_stamp':True
}

graph_thumbnail_prefs = {
  'width':100,
  'height':80,
  'text_size':6,
  'subtitle_size':0,
  'subtitle_padding':0,
  'title_size':8,
  'title_padding':2,
  'text_padding':1,
  'figure_padding':2,
  'plot_title':'NoTitle',
  'legend':False,
  'plot_axis_grid':False,
  'plot_axis':False,
  'plot_axis_labels':False,
  'graph_time_stamp':False,
  'tight_bars':True
}

def graph( data, file, *args, **kw ):

  prefs = evalPrefs( *args, **kw )
  if prefs.has_key( 'graph_size' ):
    graph_size = prefs['graph_size']
  else:
    graph_size = "normal"

  if graph_size == "normal":
    defaults = graph_normal_prefs
  elif graph_size == "small":
    defaults = graph_small_prefs
  elif graph_size == "thumbnail":
    defaults = graph_thumbnail_prefs
  elif graph_size == "large":
    defaults = graph_large_prefs

  graph = Graph()
  graph.makeGraph( data, common_prefs, defaults, prefs )
  graph.writeGraph( file, 'PNG' )
  return DIRAC.S_OK({'plot':file})

if matplotlib:
  def __checkKW( kw ):
    if 'watermark' not in kw:
      kw[ 'watermark' ] = "%s/DIRAC/Core/Utilities/Graphs/Dwatermark.png" % DIRAC.rootPath
    return kw
  
  def barGraph( data, fileName, *args, **kw ):
    kw = __checkKW( kw )
    graph( data, fileName, plot_type = 'BarGraph', statistics_line=True, *args, **kw )
  
  def lineGraph( data, fileName, *args, **kw ):
    kw = __checkKW( kw )
    graph( data, fileName, plot_type = 'LineGraph', statistics_line=True, *args, **kw )
  
  def curveGraph( data, fileName, *args, **kw ):
    kw = __checkKW( kw )
    graph( data, fileName, plot_type = 'CurveGraph', statistics_line=False, *args, **kw )
  
  def cumulativeGraph( data, fileName, *args, **kw ):
    kw = __checkKW( kw )
    graph( data, fileName, plot_type = 'LineGraph', cumulate_data = True, *args, **kw )
  
  def pieGraph( data, fileName, *args, **kw ):
    kw = __checkKW( kw )
    prefs = {'xticks':False, 'yticks':False, 'legend_position':'right'}
    graph( data, fileName, prefs, plot_type = 'PieGraph', *args, **kw )
  
  def qualityGraph( data, fileName, *args, **kw ):
    kw = __checkKW( kw )
    prefs = {'plot_axis_grid':False}
    graph( data, fileName, prefs, plot_type = 'QualityMapGraph', *args, **kw )
  
  def textGraph( text, fileName, *args, **kw ):
    kw = __checkKW( kw )
    prefs = {'text_image':text}
    graph( {}, fileName, prefs, *args, **kw )
  
  def histogram( data, fileName, bins, *args, **kw ):
    try:
      from pylab import hist
    except:
      print "No pylab module available"  
      return 
    kw = __checkKW( kw )
    values,vbins,patches = hist(data,bins)
    histo = dict(zip(vbins,values))
    span = (max(data)-min(data))/float(bins)*0.95
    kw = __checkKW( kw )
    graph( histo, fileName, plot_type = 'BarGraph', span=span, statistics_line=True, *args, **kw )
