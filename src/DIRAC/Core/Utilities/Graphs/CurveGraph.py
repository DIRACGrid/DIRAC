""" CurveGraph represents simple line graphs with markers.

    The DIRAC Graphs package is derived from the GraphTool plotting package of the
    CMS/Phedex Project by ... <to be added>
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

from DIRAC.Core.Utilities.Graphs.PlotBase import PlotBase
from DIRAC.Core.Utilities.Graphs.GraphUtilities import darkenColor, to_timestamp, PrettyDateLocator, \
    PrettyDateFormatter, PrettyScalarFormatter
from matplotlib.lines import Line2D
from matplotlib.dates import date2num
import datetime


class CurveGraph(PlotBase):

  """
  The CurveGraph class is a straightforward line graph with markers
  """

  def __init__(self, data, ax, prefs, *args, **kw):

    PlotBase.__init__(self, data, ax, prefs, *args, **kw)

  def draw(self):

    PlotBase.draw(self)
    self.x_formatter_cb(self.ax)

    if self.gdata.isEmpty():
      return None

    start_plot = 0
    end_plot = 0
    if "starttime" in self.prefs and "endtime" in self.prefs:
      start_plot = date2num(datetime.datetime.fromtimestamp(to_timestamp(self.prefs['starttime'])))
      end_plot = date2num(datetime.datetime.fromtimestamp(to_timestamp(self.prefs['endtime'])))

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

    tmp_max_y = []
    tmp_min_y = []
    tmp_x = []
    for label, num in labels:
      xdata = []
      ydata = []
      xerror = []
      yerror = []
      color = self.palette.getColor(label)
      plot_data = self.gdata.getPlotNumData(label)
      for key, value, error in plot_data:
        if value is None:
          continue
        tmp_x.append(key)
        tmp_max_y.append(value + error)
        tmp_min_y.append(value - error)
        xdata.append(key)
        ydata.append(value)
        xerror.append(0.)
        yerror.append(error)

      linestyle = self.prefs.get('linestyle', '-')
      marker = self.prefs.get('marker', 'o')
      markersize = self.prefs.get('markersize', 8.)
      markeredgewidth = self.prefs.get('markeredgewidth', 1.)
      if not self.prefs.get('error_bars', False):
        line = Line2D(xdata, ydata, color=color, linewidth=1., marker=marker, linestyle=linestyle,
                      markersize=markersize, markeredgewidth=markeredgewidth,
                      markeredgecolor=darkenColor(color))
        self.ax.add_line(line)
      else:
        self.ax.errorbar(xdata, ydata, color=color, linewidth=2., marker=marker, linestyle=linestyle,
                         markersize=markersize, markeredgewidth=markeredgewidth,
                         markeredgecolor=darkenColor(color), xerr=xerror, yerr=yerror,
                         ecolor=color)

    ymax = max(tmp_max_y)
    ymax *= 1.1
    ymin = min(tmp_min_y, 0.)
    ymin *= 1.1
    if 'log_yaxis' in self.prefs:
      ymin = 0.001

    xmax = max(tmp_x) * 1.1
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
      ticks = sorted(smap.values())
      ax.set_xticks([i + .5 for i in ticks])
      ax.set_xticklabels([reverse_smap[i] for i in ticks])
      labels = ax.get_xticklabels()
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
