########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/Utilities/Graphs/BarGraph.py,v 1.3 2009/06/07 20:01:21 atsareg Exp $
########################################################################

""" BarGraph represents bar graphs with vertical bars both simple
    and stacked. 
    
    The DIRAC Graphs package is derived from the GraphTool plotting package of the
    CMS/Phedex Project by ... <to be added>
"""

__RCSID__ = "$Id: BarGraph.py,v 1.3 2009/06/07 20:01:21 atsareg Exp $"

from DIRAC.Core.Utilities.Graphs.PlotBase import PlotBase
from DIRAC.Core.Utilities.Graphs.GraphData import GraphData
from DIRAC.Core.Utilities.Graphs.GraphUtilities import *
from pylab import setp


class BarGraph( PlotBase ):

    """
    The BarGraph class is a straightforward bar graph; given a dictionary
    of values, it takes the keys as the independent variable and the values
    as the dependent variable.
    """
    
    def __init__(self,data,ax,prefs,*args,**kw):
  
      PlotBase.__init__(self,data,ax,prefs,*args,**kw)
      if type(data) == types.DictType:
        self.gdata = GraphData(data)
      elif type(data) == types.InstanceType and data.__class__ == GraphData:
        self.gdata = data  
      if self.prefs.has_key('span'):
        self.width = self.prefs['span']
      else:  
        self.width = 1.0
        if self.gdata.key_type == "time":
          self.width = time_interval(min(self.gdata.all_keys),max(self.gdata.all_keys))
     
#    def make_bottom_text(self ):
#        """
#        Attempt to calculate the maximum, minimum, average, and current values
#        for the graph.  These statistics will be printed on the bottom of the 
#        graph.
#        """
#        units = str(self.prefs.get('column_units','')).strip()
#        results = dict(self.parsed_data)
#        try:
#            vars = getattr( self, 'vars', {} )
#            span = find_info('span',vars,self.metadata,None)
#            if getattr(self, 'is_timestamps',False) and span != None:
#                starttime = self.begin
#                starttime = starttime - (starttime % span)
#                results[starttime] = 0
#            if self.is_timestamps:
#                data_min, data_max, data_average, data_current = statistics(results, span, True)
#            else:
#                data_min, data_max, data_average = statistics(results, span)
#        except Exception, e:
#            values = results.values()
#            try:
#                data_max = max(values)
#            except:
#                data_max = None
#            try:
#                data_min = min(values)
#            except:
#                data_min = None
#            try:
#                data_average = numpy.average( values )
#            except:
#                data_average = None
#            try:
#                last_time = max(results.keys())
#                data_current = results[last_time]
#            except:
#                data_current = None
#        retval = ''
#        if data_max != None:
#            try:
#                retval += "Maximum: " + pretty_float( data_max ) + " " + units
#            except Exception, e:
#                pass
#        if data_min != None:
#            try:
#                retval += ", Minimum: " + pretty_float( data_min ) + " " + units
#            except Exception, e:
#                pass
#        if data_average != None:
#            try:
#                retval += ", Average: " + pretty_float( data_average ) + " " + units
#            except Exception, e:
#                pass
#        if (self.is_timestamps) and (data_current != None):
#            try:
#                retval += ", Current: " + pretty_float( data_current ) + " " + units
#            except Exception, e:
#                pass
#        return retval

    def draw( self ):
    
      PlotBase.draw(self)
      self.x_formatter_cb(self.ax)
    
      if self.gdata.isEmpty():
          return None
          
      tmp_x = []; tmp_y = []
  
      # Evaluate the bar width
      width = float(self.width)
      if self.gdata.key_type == 'time':
          #width = (1 - self.bar_graph_space) * width / 86400.0
          width = width / 86400.0
          offset = 0
      elif self.gdata.key_type == 'string':
          width = (1 - self.bar_graph_space) * width
          offset = self.bar_graph_space / 2.0
      else:
          offset = 0
          
      labels = self.gdata.getLabels()  
      nKeys = self.gdata.getNumberOfKeys()
      tmp_b = []
      for n in range(nKeys):
        if self.prefs.has_key('log_yaxis'):
          tmp_b.append(0.001)
          ymin = 0.001
        else:
          tmp_b.append(0.)  
          ymin = 0.
          
      self.bars = []    
      self.legendData = []
      labels = self.gdata.getLabels()
      labels.reverse()
      for label,num in labels:  
        color = self.palette.getColor(label)
        tmp_x = []
        tmp_y = []
        for key, value in self.gdata.getPlotNumData(label):
          tmp_x.append( key + offset )
          tmp_y.append( float(value) )          
        self.bars += self.ax.bar( tmp_x, tmp_y, bottom=tmp_b, width=width, color=color )
        for i in range(nKeys):
          tmp_b[i] += tmp_y[i]
          
        self.legendData.append((label,num))  
          
      dpi = self.prefs.get('dpi',100)
      tight_bars_flag = self.prefs.get('tight_bars',False)  
      if not tight_bars_flag:  
        setp( self.bars, linewidth=pixelToPoint(0.5,dpi) )
      else:
        setp( self.bars, linewidth=0. )  
      
      #pivots = keys
      #for idx in range(len(pivots)):
      #    self.coords[ pivots[idx] ] = self.bars[idx]
      
      ymax = max(tmp_b); ymax *= 1.1
      if self.log_xaxis:  
          xmin = 0.001
      else: 
          xmin = 0
      self.ax.set_xlim( xmin=xmin, xmax=max(tmp_x)+width+offset )
      self.ax.set_ylim( ymin=ymin, ymax=ymax )
      if self.gdata.key_type == 'time':
          self.ax.set_xlim( xmin=min(tmp_x), xmax=max(tmp_x)+width )
          
    def getLegendData(self):
    
      return self.legendData            
            
    def x_formatter_cb( self, ax ):
        if self.gdata.key_type == "string":
            smap = self.gdata.getStringMap()
            reverse_smap = {}
            for key, val in smap.items():
                reverse_smap[val] = key
            ticks = smap.values()
            ticks.sort()
            ax.set_xticks( [i+.5 for i in ticks] )
            ax.set_xticklabels( [reverse_smap[i] for i in ticks] )
            labels = ax.get_xticklabels()
            ax.grid( False )
            if self.log_xaxis:
                xmin = 0.001
            else:
                xmin = 0
            ax.set_xlim( xmin=xmin,xmax=len(ticks) )
        elif self.gdata.key_type == "time":
        
          #ax.set_xlim( xmin=self.begin_num,xmax=self.end_num )
          dl = PrettyDateLocator()
          df = PrettyDateFormatter( dl )
          ax.xaxis.set_major_locator( dl )
          ax.xaxis.set_major_formatter( df )
          ax.xaxis.set_clip_on(False)
          sf = PrettyScalarFormatter( )
          ax.yaxis.set_major_formatter( sf )
          #labels = ax.get_xticklabels()
            
        else:
            try:
                super(BarGraph, self).x_formatter_cb( ax )
            except:
                return None
