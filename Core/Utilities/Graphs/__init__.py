########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/Utilities/Graphs/__init__.py,v 1.3 2009/06/03 07:46:12 atsareg Exp $
########################################################################

""" DIRAC Graphs package provides tools for creation of various plots to provide
    graphical representation of the DIRAC Monitoring and Accounting data
    
    The DIRAC Graphs package is derived from the GraphTool plotting package of the
    CMS/Phedex Project by ... <to be added>
"""

__RCSID__ = "$Id: __init__.py,v 1.3 2009/06/03 07:46:12 atsareg Exp $"

from DIRAC.Core.Utilities.Graphs.Graph import Graph
from DIRAC.Core.Utilities.Graphs.GraphUtilities import evalPrefs

graph_large_prefs = {
  'width':1000,
  'height':700,
  'max_rows':99,
  'max_columns':4,
  'text_size':8,
  'subtitle_size':10,
  'subtitle_padding':5,
  'title_size':15,
  'title_padding':10,
  'dpi':100,
  'text_padding':5,
  'figure_padding':40,
  'plot_title_size':12,
  'frame':'On',
  'font' : 'Lucida Grande',
  'font_family' : 'sans-serif',
  'square_axis':False,
  'legend':True,
  'legend_position':'bottom',
  'legend_width':600,
  'legend_height':200,
  'legend_padding':20,
  'plot_grid':'1:1',
  'limit_labels':15                        
}

graph_normal_prefs = {
  'width':1000,
  'height':700,
  'max_rows':99,
  'max_columns':4,
  'text_size':8,
  'subtitle_size':10,
  'subtitle_padding':5,
  'title_size':15,
  'title_padding':10,
  'dpi':100,
  'text_padding':5,
  'figure_padding':40,
  'plot_title_size':12,
  'frame':'On',
  'font' : 'Lucida Grande',
  'font_family' : 'sans-serif',
  'square_axis':False,
  'legend':True,
  'legend_position':'bottom',
  'legend_width':600,
  'legend_height':200,
  'legend_padding':20,
  'plot_grid':'1:1',
  'limit_labels':15                        
}

graph_small_prefs = {
  'width':450,
  'height':330,
  'max_rows':99,
  'max_columns':4,
  'text_size':10,
  'subtitle_size':5,
  'subtitle_padding':4,
  'title_size':10,
  'title_padding':6,
  'dpi':100,
  'text_padding':3,
  'figure_padding':20,
  'plot_title_size':8,
  'frame':'On',
  'font' : 'Lucida Grande',
  'font_family' : 'sans-serif',
  'square_axis':False,
  'legend':True,
  'legend_position':'bottom',
  'legend_width':300,
  'legend_height':50,
  'legend_padding':10,
  'plot_grid':'2:2',
  'limit_labels':15                        
}

graph_thumbnail_prefs = {
  'width':100,
  'height':80,
  'max_rows':99,
  'max_columns':4,
  'text_size':6,
  'subtitle_size':0,
  'subtitle_padding':0,
  'title_size':8,
  'title_padding':2,
  'dpi':100,
  'text_padding':1,
  'figure_padding':2,
  'plot_title_size':8,
  'frame':'On',
  'font' : 'Lucida Grande',
  'font_family' : 'sans-serif',
  'square_axis':False,
  'legend':False,
  'plot_grid':'1:1'                       
}

def graph(data,file,*args,**kw):
  
  prefs = evalPrefs(*args,**kw)
  if prefs.has_key('graph_size'):
    graph_size = prefs['graph_size']
  else:
    graph_size = "normal"
     
  if graph_size == "normal":
    defaults = graph_normal_prefs
  elif graph_size == "small":
    defaults = graph_small_prefs  
  elif graph_size == "thumbnail":
    defaults = graph_thumbnail_prefs    
        
  graph = Graph()
  graph.makeGraph(data,defaults,prefs)
  graph.writeGraph(file,'PNG')

def bar_graph(data,file,*args,**kw):
  
  graph(data,file,plot_type='BarGraph',*args,**kw)
  
def line_graph(data,file,*args,**kw):
  
  graph(data,file,plot_type='LineGraph',*args,**kw)  
  
def cumulative_graph(data,file,*args,**kw):
  
  graph(data,file,plot_type='LineGraph',cumulate_data=True,*args,**kw)  
  
def pie_graph(data,file,*args,**kw):
  
  graph(data,file,plot_type='PieGraph',*args,**kw)  