########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/Utilities/Graphs/PieGraph.py,v 1.1 2009/06/01 22:03:05 atsareg Exp $
########################################################################

""" PieGraph represents a pie graph
    
    The DIRAC Graphs package is derived from the GraphTool plotting package of the
    CMS/Phedex Project by ... <to be added>
"""

__RCSID__ = "$Id: PieGraph.py,v 1.1 2009/06/01 22:03:05 atsareg Exp $"

import numpy, math
from matplotlib.patches import Wedge, Shadow
from matplotlib.cbook import is_string_like
from DIRAC.Core.Utilities.Graphs.PlotBase import PlotBase
from DIRAC.Core.Utilities.Graphs.GraphData import GraphData
from DIRAC.Core.Utilities.Graphs.Palette import Palette
from DIRAC.Core.Utilities.Graphs.GraphUtilities import *

class PieGraph( PlotBase ):

  def __init__(self,data,ax,prefs,*args,**kw):
  
    PlotBase.__init__(self,data,ax,prefs,*args,**kw)
    self.pdata = data
    self.palette = Palette()

  def pie(self, explode=None,
            colors=None,      
            autopct=None,
            pctdistance=0.6,
            shadow=False
            ):
            
        labels = self.pdata.getLabels()
        values = [l[1] for l in labels]    
        x = numpy.array(values, numpy.float64)

        sx = float(numpy.sum(x))
        if sx>1: x = numpy.divide(x,sx)
            
        labels = [l[0] for l in labels] 
        if explode is None: explode = [0]*len(x)
        assert(len(x)==len(labels))
        assert(len(x)==len(explode))

        center = 0,0
        radius = 1
        theta1 = 0
        i = 0   
        texts = []
        slices = []
        autotexts = []
        self.legendData = []
        for frac, label, expl in zip(x,labels, explode):
            x, y = center 
            theta2 = theta1 + frac
            thetam = 2*math.pi*0.5*(theta1+theta2)
            x += expl*math.cos(thetam)
            y += expl*math.sin(thetam)
            color = self.palette.getColor(label)
            w = Wedge((x,y), radius, 360.*theta1, 360.*theta2,
                      facecolor=color,
                      lw = pixelToPoint(0.5,self.dpi) )
            slices.append(w)
            self.ax.add_patch(w)
            w.set_label(label)
            
            self.legendData.append( (label,0.,color) )
            
            if shadow:
                # make sure to add a shadow after the call to
                # add_patch so the figure and transform props will be
                # set
                shad = Shadow(w, -0.02, -0.02,
                              #props={'facecolor':w.get_facecolor()}
                              )
                shad.set_zorder(0.9*w.get_zorder())
                self.ax.add_patch(shad)

            
            xt = x + 1.05*radius*math.cos(thetam)
            yt = y + 1.05*radius*math.sin(thetam)
            
            thetam %= 2*math.pi
            
            if 0 <thetam and thetam < math.pi:
                valign = 'bottom'
            elif thetam == 0 or thetam == math.pi:
                valign = 'center'
            else:
                valign = 'top'
            
            if thetam > math.pi/2.0 and thetam < 3.0*math.pi/2.0:
                halign = 'right'
            elif thetam == math.pi/2.0 or thetam == 3.0*math.pi/2.0:
                halign = 'center'
            else:
                halign = 'left'
            
            t = self.ax.text(xt, yt, label,
                          size=pixelToPoint(self.prefs['subtitle_size'],self.dpi),
                          horizontalalignment=halign,
                          verticalalignment=valign)
            
            t.set_family( self.prefs['font_family'] )
            t.set_fontname( self.prefs['font'] )
            t.set_size( pixelToPoint(self.prefs['subtitle_size'],self.dpi) )
            
            texts.append(t)
            
            if autopct is not None:
                xt = x + pctdistance*radius*math.cos(thetam)
                yt = y + pctdistance*radius*math.sin(thetam)
                if is_string_like(autopct):
                    s = autopct%(100.*frac)
                elif callable(autopct):
                    s = autopct(100.*frac)
                else:                    raise TypeError('autopct must be callable or a format string')
                
                t = self.ax.text(xt, yt, s,
                              horizontalalignment='center',
                              verticalalignment='center')
                
                t.set_family( self.prefs['font_family'] )
                t.set_fontname( self.prefs['font'] )
                t.set_size( pixelToPoint(self.prefs['subtitle_size'],self.dpi) )
                
                autotexts.append(t)

            
            theta1 = theta2
            i += 1
        
        self.ax.set_xlim((-1.25, 1.25))
        self.ax.set_ylim((-1.25, 1.25))
        self.ax.set_axis_off()

        if autopct is None: return slices, texts
        else: return slices, texts, autotexts

  min_amount = .1

  def getLegendData(self):
  
    return self.legendData

  def setup( self ):
    super( PieGraph, self ).setup()

    results = self.results
    parsed_data = self.parsed_data

    column_units = getattr( self, 'column_units', self.metadata.get('column_units','') )
    column_units = column_units.strip()
    sql_vars = getattr( self, 'vars', {} )
    title = getattr( self, 'title', self.metadata.get('title','') )

    if len(column_units) > 0:
      title += ' (Sum: %i ' + column_units + ')'
    else:
      title += ' (Sum: %i)'
    title = expand_string( title, sql_vars )
  
    labels = []
    amt = [] 
    keys = self.sort_keys( parsed_data )
    for key in keys:
      labels.append( str(key) + (' (%i)' % round(float(parsed_data[key]))) )
      amt.append( float(parsed_data[key]) )
    self.labels = labels
    self.labels.reverse()
    self.title = title % int(float(sum(amt)))
    self.amt_sum = float(sum(amt))
    self.amt = amt

    #labels.reverse()

  def draw( self ):

    self.ylabel = ''
    self.prefs['square_axis'] = True
    self.prefs['watermark'] = False
    PlotBase.draw(self)
    def my_display( x ):
      if x > 100*self.min_amount:
        my_amt = int(x/100.0 * 1000. )
        return str(my_amt)
      else:
        return ""

    labels = self.pdata.getLabels()
    explode = [0. for i in range(len(labels))]   
    explode[0] = 0.1
    self.wedges, text_labels, percent = self.pie( explode=explode, autopct=my_display )
    
  def get_coords( self ):
    try:
      coords = self.coords
      height = self.prefs['height']
      wedges = self.wedges
      labels = self.labels
      wedges_len = len(wedges)
      for idx in range(wedges_len):
        my_label = labels[idx] 
        orig_label = my_label[:my_label.rfind(' ')]
        wedge = wedges[ wedges_len - idx - 1 ]
        v = wedge.get_verts()
        t = wedge.get_transform()
        my_coords = t.seq_xy_tups( v )
        coords[ orig_label ] = tuple( [(i[0],height-i[1]) for i in my_coords] )
      self.coords = coords
      return coords
    except:
      return None
