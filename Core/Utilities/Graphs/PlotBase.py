import types, random
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/Utilities/Graphs/PlotBase.py,v 1.2 2009/06/01 22:37:40 atsareg Exp $
########################################################################

""" PlotBase is a base class for various Graphs plots
    
    The DIRAC Graphs package is derived from the GraphTool plotting package of the
    CMS/Phedex Project by ... <to be added>
"""

__RCSID__ = "$Id: PlotBase.py,v 1.2 2009/06/01 22:37:40 atsareg Exp $"

from matplotlib.patches import Rectangle
from matplotlib.text import Text
from DIRAC.Core.Utilities.Graphs.GraphUtilities import *
from matplotlib.axes import Axes    
from matplotlib.pylab import setp

class PlotBase:

  def __init__(self,data=None,axes=None,*aw,**kw):
  
    self.ax_contain = axes
    self.canvas = None
    self.figure = None
    if self.ax_contain:
      self.figure = self.ax_contain.get_figure()
      self.canvas = self.figure.canvas
      self.dpi = self.ax_contain.figure.get_dpi()
      self.ax_contain.set_axis_off()
    self.prefs = evalPrefs(*aw,**kw)
    self.coords = {}

  def dumpPrefs(self):
  
    for key in self.prefs:
      print key.rjust(20),':',str(self.prefs[key]).ljust(40)
      
  def setAxes(self,axes):
  
    self.ax_contain = axes  
    self.ax_contain.set_axis_off()
    self.figure = self.ax_contain.get_figure()
    self.canvas = self.figure.canvas
    self.dpi = self.ax_contain.figure.get_dpi()

  def draw(self):
  
    prefs = self.prefs
    dpi = self.ax_contain.figure.get_dpi()
    
    xlabel = prefs.get('xlabel','') 
    ylabel = prefs.get('ylabel','') 
    text_size = prefs['text_size']
    text_size_point = pixelToPoint(text_size,dpi)
    plot_title = prefs.get('plot_title','')
    if not plot_title:
      plot_title_size = 0
      plot_title_padding = 0
    else:
      plot_title_size = prefs['plot_title_size']
      plot_title_padding = prefs['text_padding']  
    plot_title_size_point = pixelToPoint(plot_title_size,dpi)
    figure_padding = prefs['figure_padding']
    frame_flag = prefs['frame']
    
    # Create plot axes, and set properties
    
    left,bottom,width,height = self.ax_contain.get_window_extent().get_bounds()
    l,b,f_width,f_height = self.figure.get_window_extent().get_bounds()
        
    ax_plot_rect = (float(figure_padding+left)/f_width,
                    float(figure_padding+bottom)/f_height,
                    float(width-figure_padding)/f_width,
                    float(height-figure_padding-plot_title_size-2*plot_title_padding)/f_height)
    ax = Axes(self.figure,ax_plot_rect)                
    if prefs['square_axis']:
      l,b,a_width,a_height = ax.get_window_extent().get_bounds()
      delta = abs(a_height-a_width)
      if a_height>a_width:
        a_height = a_width
        ax_plot_rect = (float(figure_padding+left)/f_width,
                        float(figure_padding+bottom+delta/2.)/f_height,
                        float(width-figure_padding)/f_width,
                        float(height-figure_padding-plot_title_size-2*plot_title_padding-delta)/f_height)
      else:
        a_width = a_height
        ax_plot_rect = (float(figure_padding+left+delta/2.)/f_width,
                        float(figure_padding+bottom)/f_height,
                        float(width-figure_padding-delta)/f_width,
                        float(height-figure_padding-plot_title_size-2*plot_title_padding)/f_height)
      ax.set_position(ax_plot_rect)                  
                              
                    
    self.figure.add_axes(ax)   
    self.ax = ax                
    frame = ax.get_frame()
    frame.set_fill( False )

    if frame_flag.lower() == 'off':
      self.ax.set_axis_off()
    else:  
      # If requested, make x/y axis logarithmic
      if prefs.get('log_xaxis','False').find('r') >= 0:
          ax.semilogx()
          self.log_xaxis = True
      else:
          self.log_xaxis = False
      if prefs.get('log_yaxis','False').find('r') >= 0:
          ax.semilogy()
          self.log_yaxis = True
      else:
          self.log_yaxis = False

      setp( ax.get_xticklabels(), family=prefs['font_family'] )
      setp( ax.get_xticklabels(), fontname=prefs['font'] )
      setp( ax.get_xticklabels(), size=text_size_point)

      setp( ax.get_yticklabels(), family=prefs['font_family'] )
      setp( ax.get_yticklabels(), fontname=prefs['font'] )
      setp( ax.get_yticklabels(), size=text_size_point )

      setp( ax.get_xticklines(),  markeredgewidth=pixelToPoint(1.0,dpi) )
      setp( ax.get_xticklines(),  markersize=pixelToPoint(text_size/2.,dpi) )
      setp( ax.get_yticklines(),  markeredgewidth=pixelToPoint(1.0,dpi) )
      setp( ax.get_yticklines(),  markersize=pixelToPoint(text_size/2.,dpi) )
      setp( ax.get_xticklines(),  zorder=4.0 )

      setp( ax.get_frame(), linewidth=pixelToPoint(.1,dpi) )
      setp( ax.axesFrame, linewidth=pixelToPoint(1.0,dpi) )
      #setp( ax.axvline(), linewidth=pixelToPoint(1.0,dpi) ) 
      ax.grid( True, color='#555555', linewidth=pixelToPoint(0.1,dpi) )
      
      # Set labels
      if xlabel:
        t = ax.set_xlabel( xlabel )
        t.set_family(prefs['font_family'])
        t.set_fontname(prefs['font'])
        t.set_size(prefs['text_size'])

      if ylabel:
        t = ax.set_ylabel( ylabel )
        t.set_family(prefs['font_family'])
        t.set_fontname(prefs['font'])
        t.set_size(prefs['text_size']) 

    # Create a plot title, if necessary
    if plot_title:    
      self.ax.title = self.ax.text( 0.5, 
                                    1.+float(plot_title_padding)/height,
                                    plot_title,
                                    verticalalignment='bottom', 
                                    horizontalalignment='center',
                                    size = pixelToPoint(plot_title_size,dpi),
                                    family=prefs['font_family'],
                                    fontname=prefs['font']
                                    )
      self.ax.title.set_transform(self.ax.transAxes)                                                  
      self.ax.title.set_family( prefs['font_family'] )
      self.ax.title.set_fontname( prefs['font'] )
                                          
