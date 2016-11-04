########################################################################
# $HeadURL$
########################################################################

""" Legend encapsulates a graphical plot legend drawing tool
    
    The DIRAC Graphs package is derived from the GraphTool plotting package of the
    CMS/Phedex Project by ... <to be added>
"""

__RCSID__ = "$Id$"

from matplotlib.patches import Rectangle
from matplotlib.text import Text
from DIRAC.Core.Utilities.Graphs.GraphUtilities import *  
from DIRAC.Core.Utilities.Graphs.Palette import Palette  
from DIRAC.Core.Utilities.Graphs.GraphData import GraphData
from matplotlib.figure import Figure 
from matplotlib.backends.backend_agg import FigureCanvasAgg
import types

class Legend( object ):

  def __init__(self,data=None,axes=None,*aw,**kw): 
    
    self.text_size = 0
    self.column_width = 0
    self.labels = {}
    if type(data) == types.DictType:
      for label,ddict in data.items():
        #self.labels[label] = pretty_float(max([ float(x) for x in ddict.values() if x ]) )  
        self.labels[label] = "%.1f" % max([ float(x) for x in ddict.values() if x ])   
    elif type(data) == types.InstanceType and data.__class__ == GraphData:
      self.labels = data.getLabels()    
    else:
      self.labels = data  
    #self.labels.reverse()  
    self.ax = axes
    self.canvas = None
    if self.ax:
      self.canvas = self.ax.figure.canvas
      self.ax.set_axis_off()
    self.prefs = evalPrefs(*aw,**kw)
    self.palette = Palette()
        
    if self.labels and self.labels[0][0] != 'NoLabels':
      percent_flag = self.prefs.get('legend_unit','')
      if percent_flag == "%":       
        sum_value = sum(data.label_values)
        if sum_value > 0.:
          self.labels = [(l,v/sum_value*100.) for l,v in self.labels ]
    self.__get_column_width()
    
  def dumpPrefs(self):
  
    for key in self.prefs:
      print key.rjust(20),':',str(self.prefs[key]).ljust(40)
      
  def setLabels(self,labels):
  
    self.labels = labels   
    
  def setAxes(self,axes):
  
    self.ax = axes  
    self.canvas = self.ax.figure.canvas
    self.ax.set_axis_off() 
    
  def getLegendSize(self):
    self.__get_column_width()
    legend_position = self.prefs['legend_position']
    legend_width = float(self.prefs['legend_width']) 
    legend_height = float(self.prefs['legend_height']) 
    legend_padding = float(self.prefs['legend_padding'])   
    legend_text_size = self.prefs.get('legend_text_size',self.prefs['text_size'])
    legend_text_padding = self.prefs.get('legend_text_padding',self.prefs['text_padding'])
    if legend_position in ['right','left']:
      # One column in case of vertical legend
      legend_width = self.column_width+legend_padding
      nLabels = len(self.labels)
      legend_max_height = nLabels*(legend_text_size+legend_text_padding)
    elif legend_position == 'bottom':
      nColumns = min(self.prefs['legend_max_columns'],int(legend_width/self.column_width))
      nLabels = len(self.labels)
      maxRows = self.prefs['legend_max_rows']
      nRows_ax = int(legend_height/1.6/self.prefs['text_size'])
      nRows_label = nLabels/nColumns + (nLabels%nColumns != 0)
      nRows = max(1,min(min(nRows_label,maxRows),nRows_ax ))
      text_padding = self.prefs['text_padding']    
      text_padding = pixelToPoint(text_padding,self.prefs['dpi'])    
      legend_height = min(legend_height,(nRows*(self.text_size+text_padding)+text_padding))
      legend_max_height = nLabels*(self.text_size+text_padding)
      
    return legend_width,legend_height,legend_max_height  
  
  def __get_legend_text_size(self):
    
    dpi = self.prefs['dpi']
    text_size = self.prefs['text_size']
    text_padding = self.prefs['text_padding']
    legend_text_size = self.prefs.get('legend_text_size',text_size)
    legend_text_padding = self.prefs.get('legend_text_padding',text_padding)
    return legend_text_size,legend_text_padding

  def __get_column_width(self):
  
    max_length = 0
    max_column_text = ''
    flag = self.prefs.get('legend_numbers',True)
    unit = self.prefs.get('legend_unit',False)
    for label,num in self.labels:      
      if not flag: num = None
      if num is not None:
        column_length = len(str(label)+str(num)) + 1
      else:
        column_length = len(str(label)) + 1  
      if column_length > max_length:
        max_length = column_length
        if flag:
          if type(num) == types.IntType or type(num) == types.LongType:
            numString = str(num)
          else:
            numString = "%.1f" % float(num)  
          max_column_text = '%s  %s' % (str(label),numString)
          if unit:
            max_column_text += "%"
        else:
          max_column_text = '%s   ' % str(label)  
                         
    figure = Figure()   
    canvas = FigureCanvasAgg(figure) 
    dpi = self.prefs['dpi']
    figure.set_dpi( dpi ) 
    l_size, _ = self.__get_legend_text_size()    
    self.text_size = pixelToPoint(l_size,dpi)           
    text = Text(0.,0.,text=max_column_text,size=self.text_size)
    text.set_figure(figure)
    bbox = text.get_window_extent(canvas.get_renderer())
    columnwidth = bbox.width+6*l_size
    self.column_width = columnwidth if columnwidth <= self.prefs['legend_width'] else self.prefs['legend_width'] - 6*l_size #make sure the legend fit in the box
        
  def draw(self):
  
    dpi = self.prefs['dpi']
    ax_xsize = self.ax.get_window_extent().width
    ax_ysize = self.ax.get_window_extent().height  
    nLabels = len(self.labels)
    nColumns = min(self.prefs['legend_max_columns'],int(ax_xsize/self.column_width))
    
    maxRows = self.prefs['legend_max_rows']
    nRows_ax = int(ax_ysize/1.6/self.prefs['text_size'])
    nRows_label = nLabels/nColumns + (nLabels%nColumns != 0)
    nRows = max(1,min(min(nRows_label,maxRows),nRows_ax ))
    maxLabels = nColumns*nRows - 1
    self.ax.set_xlim(0.,float(ax_xsize))
    self.ax.set_ylim(-float(ax_ysize),0.)
   
    legend_text_size,legend_text_padding = self.__get_legend_text_size()
    legend_text_size_point = pixelToPoint(legend_text_size,dpi)
        
    box_width = legend_text_size
    legend_offset = (ax_xsize - nColumns*self.column_width)/2 
    
    nc = 0
    #self.labels.reverse()
    
    for label,num in self.labels:
      num_flag = self.prefs.get('legend_numbers',True)
      percent_flag = self.prefs.get('legend_unit','')
      if num_flag:
        if percent_flag == "%":
          num = "%.1f" % num +'%'
        else:  
          num = "%.1f" % num
      else:
        num = None  
      color = self.palette.getColor(label)
      row = nc%nRows 
      column = nc/nRows          
      if row == nRows-1 and column == nColumns-1 and nc != nLabels-1:
        last_text = '... plus %d more' % (nLabels-nc)
        self.ax.text(float(column*self.column_width)+legend_offset,-float(row*1.6*box_width),
                     last_text,horizontalalignment='left',
                     verticalalignment='top',size=legend_text_size_point)  
        break   
      else:
        self.ax.text(float(column*self.column_width)+2.*box_width+legend_offset,-row*1.6*box_width,
                     str(label),horizontalalignment='left',
                     verticalalignment='top',size=legend_text_size_point)
        if num is not None:
          self.ax.text(float((column+1)*self.column_width)-2*box_width+legend_offset,-float(row*1.6*box_width),
                       str(num),horizontalalignment='right',
                       verticalalignment='top',size=legend_text_size_point)             
        box = Rectangle((float(column*self.column_width)+legend_offset,-float(row*1.6*box_width)-box_width),
                        box_width,box_width)            
        box.set_ec('black')
        box.set_linewidth(pixelToPoint(0.5,dpi))
        box.set_fc(color)
        self.ax.add_patch(box)
        nc += 1    
