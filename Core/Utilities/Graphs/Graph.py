########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/Utilities/Graphs/Graph.py,v 1.6 2009/06/04 09:26:31 atsareg Exp $
########################################################################

""" Graph is a class providing layouts for the complete plot images including
    titles multiple plots and a legend
    
    The DIRAC Graphs package is derived from the GraphTool plotting package of the
    CMS/Phedex Project by ... <to be added>
"""

__RCSID__ = "$Id: Graph.py,v 1.6 2009/06/04 09:26:31 atsareg Exp $"

import types, datetime
from matplotlib.backends.backend_agg import FigureCanvasAgg
from matplotlib.figure import Figure 
from DIRAC.Core.Utilities.Graphs.GraphUtilities import *
from DIRAC.Core.Utilities.Graphs.GraphData import GraphData
from DIRAC.Core.Utilities.Graphs.Legend import Legend
#from DIRAC import S_OK, S_ERROR

class Graph(object):

  def __init__( self, *args, **kw ):
    super( Graph, self ).__init__( *args, **kw )
    
  def layoutFigure(self,*args,**kw):
  
    prefs = self.prefs
  
    # Get the main Figure object
    self.figure = Figure()
    figure = self.figure
    self.canvas = FigureCanvasAgg(figure) 
    canvas = self.canvas
    
    dpi = prefs['dpi']
    width = float(prefs['width'])
    height = float(prefs['height'])
    width_inch = width/dpi
    height_inch = height/dpi
    figure.set_size_inches( width_inch, height_inch )
    figure.set_dpi( dpi )
    figure.set_facecolor('white')
    
    #######################################
    # Make the graph title
    
    title = prefs['title']
    title_size = prefs['title_size']
    title_padding = float(prefs['title_padding'])
    figure_padding = float(prefs['figure_padding'])
    figure.text(0.5,1.-(title_size+figure_padding)/height,title,
                ha='center',va='bottom',size=pixelToPoint(title_size,dpi) )
     
    subtitle = prefs.get('subtitle','') 
    if subtitle:            
      sublines = subtitle.split('\n')
      nsublines = len(sublines)
      subtitle_size = prefs['subtitle_size']
      subtitle_padding = float(prefs['subtitle_padding']) 
      top_offset = subtitle_size+subtitle_padding+title_size+figure_padding  
      for subline in sublines:
        figure.text(0.5,1.-(top_offset)/height,
                    subline,ha='center',va='bottom',
                    size=pixelToPoint(subtitle_size,dpi),fontstyle='italic' ) 
        top_offset +=  subtitle_size + subtitle_padding          

    graph_width = width - 2.*figure_padding
    graph_height = height - 2.*figure_padding - title_padding - title_size
    if subtitle:
      graph_height = graph_height - nsublines*(subtitle_size + subtitle_padding)     
    graph_left = figure_padding
    graph_bottom = figure_padding      
                
    #########################################
    # Make the plot time stamp
    timeString = "Generated on " + \
                 datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S ')+'UTC'
    time_size = prefs['text_size']*.8
    figure.text(0.995,0.005,timeString,ha='right',va='bottom',size=pixelToPoint(time_size,dpi),fontstyle='italic' )               
                
    #########################################
    # Make the graph Legend if requested     
                  
    legend_flag = prefs['legend']
    legend_ax = None
    if legend_flag:
      legend_position = prefs['legend_position']
      legend_width = float(prefs['legend_width']) 
      legend_height = float(prefs['legend_height']) 
      legend_padding = float(prefs['legend_padding']) 
      if legend_position in ['right','left']:
        bottom = (height-title_size-title_padding-legend_height)/2./height
        if legend_position == 'right':
          left = 1. - (figure_padding+legend_width)/width
        else:
          left = figure_padding/height  
          graph_left = graph_left - legend_width  
        graph_width = graph_width - legend_width - legend_padding  
      elif legend_position == 'bottom':
        bottom = figure_padding/height
        left = (width-legend_width)/2./width
        graph_height = graph_height  - legend_height - legend_padding
        graph_bottom = graph_bottom + legend_height + legend_padding
        
      legend_rect = (left,bottom,legend_width/width,legend_height/height)
      legend_ax = figure.add_axes(legend_rect)
    
    ###########################################
    # Make the plot spots 
    plot_grid = prefs['plot_grid']
    nx = int(plot_grid.split(':')[0])
    ny = int(plot_grid.split(':')[1]) 
    
    plot_axes = []
    for i in range(nx):
      for j in range(ny):
        plot_rect = ((graph_left+graph_width*i/nx)/width,
                     (graph_bottom+graph_height*j/ny)/height,
                     graph_width/nx/width,
                     graph_height/ny/height)
                     
        plot_axes.append(figure.add_axes(plot_rect))
    
    return legend_ax, plot_axes

  def makeGraph(self, data, *args, **kw):
  
    # Evaluate all the preferences
    self.prefs = evalPrefs(*args,**kw)
    prefs = self.prefs
    
    metadata = prefs.get('metadata',{})
    
    legend_ax, plot_axes = self.layoutFigure(metadata,*args,**kw)
    nPlots = len(plot_axes)
    if nPlots == 1:
      if type(data) != types.ListType:
        data = [data]
      if type(metadata) != types.ListType:
        metadata = [metadata]
    else:
      if type(data) != types.ListType:
        #return S_ERROR('Single data for multiplot graph')
        print 'Single data for multiplot graph'
        return 
      if type(metadata) != types.ListType:
        metaList = []
        for ip in range(nPlots):
          metaList.append(metadata)
        metadata = metaList
      
    # Make plots    
    graphData = []
    for i in range(nPlots):
      plot_prefs = evalPrefs(prefs,metadata[i])
      plot_type = plot_prefs['plot_type']     
      try:
        exec "import %s" % plot_type
      except ImportError, x:
        print "Failed to import graph type %s" % plot_type 
        return None
        
      ax = plot_axes[i]  
      gdata = GraphData(data[i])
      if plot_prefs.has_key('limit_labels'):
        gdata.truncateLabels(plot_prefs['limit_labels'])
      if plot_prefs.has_key('cumulate_data'):  
        gdata.makeCumulativeGraph()
      graphData.append(gdata)
      if gdata.key_type == "time":
        time_title = add_time_to_title(gdata.min_key,gdata.max_key)
        plot_prefs['plot_title'] = plot_prefs.get('plot_title','')+' '+time_title
      plot = eval("%s.%s(graphData[i],ax,plot_prefs)" % (plot_type,plot_type) )
      plot.draw()
      if i == 0:
        legendData = plot.getLegendData()
      
    # Make legend
    if legend_ax:
      legend = Legend(legendData,legend_ax,prefs)
      legend.draw()  
      
    #return S_OK()  
      
  def writeGraph(self,fname,format):

    self.canvas.draw()
    if format.lower() == 'png':
      self.canvas.print_png(fname)
    elif format.lower() == 'svg':
      self.canvas.print_svg(fname)     
        
                     
    
    
            
          
        
    
