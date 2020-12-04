""" QualityGraph represents a Quality Map of entities as a special color schema

    The DIRAC Graphs package is derived from the GraphTool plotting package of the
    CMS/Phedex Project by ... <to be added>
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import datetime
from pylab import setp
from matplotlib.colors import Normalize
import matplotlib.cm as cm
from matplotlib.colorbar import make_axes, ColorbarBase
from matplotlib.dates import date2num

from DIRAC.Core.Utilities.Graphs.PlotBase import PlotBase
from DIRAC.Core.Utilities.Graphs.GraphData import GraphData
from DIRAC.Core.Utilities.Graphs.GraphUtilities import to_timestamp, pixelToPoint, PrettyDateLocator, \
    PrettyDateFormatter, PrettyScalarFormatter

__RCSID__ = "$Id$"


cdict = {'red': ((0.0, 1., 1.0),
                 (0.5, .0, .0),
                 (1.0, 0.0, 0.0)),
         'green': ((0.0, 0.1, 0.1),
                   (0.5, 0.9, 0.9),
                   (1.0, 0.7, 0.7)),
         'blue': ((0.0, 0.1, 0.1),
                  (0.5, 0.5, 0.5),
                  (1.0, 0.0, 0.0))}

# color blind
# cdict = {'red': ((0.0, .5, 0.5),
#                 (0.5, .56, 0.56),
#                 (1.0, 0.63, 0.63)),
#         'green': ((0.0, 0., 0.),
#                   (0.5, 0.5, 0.5),
#                   (1.0, 1., 1.)),
#         'blue': ((0.0, 0., 0.),
#                  (0.5, 0.315, 0.315),
#                 (1.0, 0.63, 0.63))}


class QualityMapGraph(PlotBase):

  """
  The BarGraph class is a straightforward bar graph; given a dictionary
  of values, it takes the keys as the independent variable and the values
  as the dependent variable.
  """

  def __init__(self, data, ax, prefs, *args, **kw):

    PlotBase.__init__(self, data, ax, prefs, *args, **kw)
    if isinstance(data, dict):
      self.gdata = GraphData(data)
    elif isinstance(data, type) and data.__class__ == GraphData:
      self.gdata = data
    if 'span' in self.prefs:
      self.width = self.prefs['span']
    else:
      self.width = 1.0
      if self.gdata.key_type == "time":
        nKeys = self.gdata.getNumberOfKeys()
        self.width = (max(self.gdata.all_keys) - min(self.gdata.all_keys)) / nKeys

    # redefine the look of the scale if requested
    if isinstance(self.prefs['scale_data'], dict):
      self.cbBoundaries = list()
      self.cbValues = list()

      # ColorbarBase needs sorted data
      for boundary in sorted(self.prefs['scale_data']):
        self.cbBoundaries.append(boundary)
        self.cbValues.append(self.prefs['scale_data'][boundary])
    else:
      self.cbBoundaries = None  # set default values
      self.cbValues = None

    if isinstance(self.prefs['scale_ticks'], list):
      self.cbTicks = self.prefs['scale_ticks']
    else:
      self.cbTicks = None  # set default value

    # Setup the colormapper to get the right colors
    self.cmap = None

    max_value = prefs.get('normalization')
    if max_value:
      self.cmap = cm.YlGnBu  # pylint: disable=no-member
    else:
      max_value = 100
      self.cmap = cm.RdYlGn  # pylint: disable=no-member

    self.norms = Normalize(0, max_value)
    mapper = cm.ScalarMappable(cmap=self.cmap, norm=self.norms)

    def get_alpha(*args, **kw):
      return 1.0
    mapper.get_alpha = get_alpha
    self.mapper = mapper

  def draw(self):

    PlotBase.draw(self)
    self.x_formatter_cb(self.ax)

    if self.gdata.isEmpty():
      return None

    # Evaluate the bar width
    width = float(self.width)
    offset = 0.
    if self.gdata.key_type == 'time':
      width = width / 86400.0

    start_plot = 0
    end_plot = 0
    if "starttime" in self.prefs and "endtime" in self.prefs:
      start_plot = date2num(datetime.datetime.fromtimestamp(to_timestamp(self.prefs['starttime'])))
      end_plot = date2num(datetime.datetime.fromtimestamp(to_timestamp(self.prefs['endtime'])))

    labels = self.gdata.getLabels()
    nKeys = self.gdata.getNumberOfKeys()
    tmp_b = []
    tmp_x = []
    tmp_y = []

    self.bars = []
    labels = self.gdata.getLabels()
    nLabel = 0
    labelNames = []
    colors = []
    xmin = None
    xmax = None
    for label, _num in labels:
      labelNames.append(label)
      for key, value, _error in self.gdata.getPlotNumData(label):

        if xmin is None or xmin > (key + offset):
          xmin = key + offset
        if xmax is None or xmax < (key + offset):
          xmax = key + offset

        if value is not None:
          colors.append(self.getQualityColor(value))
          tmp_x.append(key + offset)
          tmp_y.append(1.)
          tmp_b.append(float(nLabel))

      nLabel += 1

    self.bars += self.ax.bar(tmp_x, tmp_y, bottom=tmp_b, width=width, color=colors)

    dpi = self.prefs.get('dpi', 100)
    setp(self.bars, linewidth=pixelToPoint(0.5, dpi), edgecolor='#AAAAAA')

    # pivots = keys
    # for idx in range(len(pivots)):
    #    self.coords[ pivots[idx] ] = self.bars[idx]

    ymax = float(nLabel)
    self.ax.set_xlim(xmin=0., xmax=xmax + width + offset)
    self.ax.set_ylim(ymin=0., ymax=ymax)
    if self.gdata.key_type == 'time':
      if start_plot and end_plot:
        self.ax.set_xlim(xmin=start_plot, xmax=end_plot)
      else:
        self.ax.set_xlim(xmin=min(tmp_x), xmax=max(tmp_x))
    self.ax.set_yticks([i + 0.5 for i in range(nLabel)])
    self.ax.set_yticklabels(labelNames)
    setp(self.ax.get_xticklines(), markersize=0.)
    setp(self.ax.get_yticklines(), markersize=0.)

    cax, kw = make_axes(self.ax, orientation='vertical', fraction=0.07)
    cb = ColorbarBase(cax, cmap=self.cmap, norm=self.norms,
                      boundaries=self.cbBoundaries,
                      values=self.cbValues,
                      ticks=self.cbTicks)
    cb.draw_all()
    # cb = self.ax.colorbar( self.mapper, format="%d%%",
    #  orientation='horizontal', fraction=0.04, pad=0.1, aspect=40  )
    # setp( cb.outline, linewidth=.5 )
    # setp( cb.ax.get_xticklabels(), size=10 )
    # setp( cb.ax.get_xticklabels(), family=self.prefs['font_family'] )
    # setp( cb.ax.get_xticklabels(), fontname = self.prefs['font'] )

  def getQualityColor(self, value):

    if value is None or value < 0.:
      return "#FFFFFF"
    return self.mapper.to_rgba(value)

  def getLegendData(self):

    return None

  def x_formatter_cb(self, ax):
    if self.gdata.key_type == "string":
      smap = self.gdata.getStringMap()
      reverse_smap = {}
      for key, val in smap.items():
        reverse_smap[val] = key
      ticks = sorted(smap.values())
      ax.set_xticks([i + .5 for i in ticks])
      ax.set_xticklabels([reverse_smap[i] for i in ticks])
      # labels = ax.get_xticklabels()
      ax.grid(False)
      if self.log_xaxis:
        xmin = 0.001
      else:
        xmin = 0.
      ax.set_xlim(xmin=xmin, xmax=len(ticks))
    elif self.gdata.key_type == "time":

      # ax.set_xlim( xmin=self.begin_num,xmax=self.end_num )
      dl = PrettyDateLocator()
      df = PrettyDateFormatter(dl)
      ax.xaxis.set_major_locator(dl)
      ax.xaxis.set_major_formatter(df)
      ax.xaxis.set_clip_on(False)
      sf = PrettyScalarFormatter()
      ax.yaxis.set_major_formatter(sf)
      # labels = ax.get_xticklabels()

    else:
      return None
