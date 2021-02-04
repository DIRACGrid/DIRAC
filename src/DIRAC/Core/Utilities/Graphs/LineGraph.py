""" LineGraph represents line graphs both simple and stacked. It includes
    also cumulative graph functionality.

    The DIRAC Graphs package is derived from the GraphTool plotting package of the
    CMS/Phedex Project by ... <to be added>
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

from DIRAC.Core.Utilities.Graphs.PlotBase import PlotBase
from DIRAC.Core.Utilities.Graphs.GraphUtilities import to_timestamp, PrettyDateLocator, \
    PrettyDateFormatter, PrettyScalarFormatter
from matplotlib.patches import Polygon
from matplotlib.dates import date2num
import datetime


class LineGraph(PlotBase):

  """
  The LineGraph class is a straightforward line graph; given a dictionary
  of values, it takes the keys as the independent variable and the values
  as the dependent variable.
  """

  def __init__(self, data, ax, prefs, *args, **kw):

    PlotBase.__init__(self, data, ax, prefs, *args, **kw)

  def draw(self):

    PlotBase.draw(self)
    self.x_formatter_cb(self.ax)

    if self.gdata.isEmpty():
      return None

    tmp_x = []
    tmp_y = []

    labels = self.gdata.getLabels()
    nKeys = self.gdata.getNumberOfKeys()
    tmp_b = []
    for n in range(nKeys):
      if 'log_yaxis' in self.prefs:
        tmp_b.append(0.001)
      else:
        tmp_b.append(0.)

    start_plot = 0
    end_plot = 0
    if "starttime" in self.prefs and "endtime" in self.prefs:
      start_plot = date2num(datetime.datetime.fromtimestamp(to_timestamp(self.prefs['starttime'])))
      end_plot = date2num(datetime.datetime.fromtimestamp(to_timestamp(self.prefs['endtime'])))

    self.polygons = []
    seq_b = [(self.gdata.max_num_key, 0.0), (self.gdata.min_num_key, 0.0)]
    zorder = 0.0
    labels = self.gdata.getLabels()
    labels.reverse()

    # If it is a simple plot, no labels are used
    # Evaluate the most appropriate color in this case
    if self.gdata.isSimplePlot():
      labels = [('SimplePlot', 0.)]
      color = self.prefs.get('plot_color', 'Default')
      if color.find('#') != -1:
        self.palette.setColor('SimplePlot', color)
      else:
        labels = [(color, 0.)]

    for label, num in labels:

      color = self.palette.getColor(label)
      ind = 0
      tmp_x = []
      tmp_y = []
      plot_data = self.gdata.getPlotNumData(label)
      for key, value, error in plot_data:
        if value is None:
          value = 0.
        tmp_x.append(key)
        tmp_y.append(float(value) + tmp_b[ind])
        ind += 1
      seq_t = list(zip(tmp_x, tmp_y))
      seq = seq_t + seq_b
      poly = Polygon(seq, facecolor=color, fill=True, linewidth=.2, zorder=zorder)
      self.ax.add_patch(poly)
      self.polygons.append(poly)
      tmp_b = list(tmp_y)
      zorder -= 0.1

    ymax = max(tmp_b)
    ymax *= 1.1
    ymin = min(min(tmp_b), 0.)
    ymin *= 1.1
    if 'log_yaxis' in self.prefs:
      ymin = 0.001
    xmax = max(tmp_x)
    if self.log_xaxis:
      xmin = 0.001
    else:
      xmin = 0

    ymin = self.prefs.get('ymin', ymin)
    ymax = self.prefs.get('ymax', ymax)
    xmin = self.prefs.get('xmin', xmin)
    xmax = self.prefs.get('xmax', xmax)

    self.ax.set_xlim(xmin=xmin, xmax=xmax)
    self.ax.set_ylim(ymin=ymin, ymax=ymax)
    if self.gdata.key_type == 'time':
      if start_plot and end_plot:
        self.ax.set_xlim(xmin=start_plot, xmax=end_plot)
      else:
        self.ax.set_xlim(xmin=min(tmp_x), xmax=max(tmp_x))

  def x_formatter_cb(self, ax):
    if self.gdata.key_type == "string":
      smap = self.gdata.getStringMap()
      reverse_smap = {}
      for key, val in smap.items():
        reverse_smap[val] = key
      ticks = smap.values()
      ticks.sort()
      ax.set_xticks([i + .5 for i in ticks])
      ax.set_xticklabels([reverse_smap[i] for i in ticks])
      ax.grid(False)
      if self.log_xaxis:
        xmin = 0.001
      else:
        xmin = 0
      ax.set_xlim(xmin=xmin, xmax=len(ticks))
    elif self.gdata.key_type == "time":
      dl = PrettyDateLocator()
      df = PrettyDateFormatter(dl)
      ax.xaxis.set_major_locator(dl)
      ax.xaxis.set_major_formatter(df)
      ax.xaxis.set_clip_on(False)
      sf = PrettyScalarFormatter()
      ax.yaxis.set_major_formatter(sf)

    else:
      return None
