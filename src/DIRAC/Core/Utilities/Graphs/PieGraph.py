""" PieGraph represents a pie graph

    The DIRAC Graphs package is derived from the GraphTool plotting package of the
    CMS/Phedex Project by ... <to be added>
"""

from __future__ import print_function
from __future__ import absolute_import
from __future__ import division

import numpy
import math
import time
from matplotlib.patches import Wedge, Shadow
from DIRAC.Core.Utilities.Graphs.PlotBase import PlotBase
from DIRAC.Core.Utilities.Graphs.GraphData import GraphData
from DIRAC.Core.Utilities.Graphs.GraphUtilities import *

__RCSID__ = "$Id$"


class PieGraph(PlotBase):

  def __init__(self, data, ax, prefs, *args, **kw):

    PlotBase.__init__(self, data, ax, prefs, *args, **kw)
    self.pdata = data

  def pie(self, explode=None,
          colors=None,
          autopct=None,
          pctdistance=0.6,
          shadow=False):

    start = time.time()
    labels = self.pdata.getLabels()
    if labels[0][0] == "NoLabels":
      try:
        self.pdata.initialize(key_type='string')
        self.pdata.sortLabels()
        labels = self.pdata.getLabels()
        nLabels = self.pdata.getNumberOfLabels()
        explode = [0.] * nLabels
        if nLabels > 0:
          explode[0] = 0.1
      except Exception as x:
        print("PieGraph Error: can not interpret data for the plot")

    # labels.reverse()
    values = [l[1] for l in labels]
    x = numpy.array(values, numpy.float64)
    self.legendData = labels

    sx = float(numpy.sum(x))
    if sx > 0:
      x = numpy.divide(x, sx)

    labels = [l[0] for l in labels]
    if explode is None:
      explode = [0] * len(x)
    assert len(x) == len(labels)
    assert len(x) == len(explode)
    plot_axis_labels = self.prefs.get('plot_axis_labels', True)

    center = 0, 0
    radius = 1.1
    theta1 = 0
    i = 0
    texts = []
    slices = []
    autotexts = []

    for frac, label, expl in zip(x, labels, explode):
      x, y = center
      theta2 = theta1 + frac
      thetam = 2 * math.pi * 0.5 * (theta1 + theta2)
      x += expl * math.cos(thetam)
      y += expl * math.sin(thetam)
      color = self.palette.getColor(label)
      w = Wedge((x, y), radius, 360. * theta1, 360. * theta2,
                facecolor=color,
                lw=pixelToPoint(0.5, self.dpi),
                edgecolor='#999999')
      slices.append(w)
      self.ax.add_patch(w)
      w.set_label(label)

      if shadow:
        # make sure to add a shadow after the call to
        # add_patch so the figure and transform props will be
        # set
        shad = Shadow(w, -0.02, -0.02)
        shad.set_zorder(0.9 * w.get_zorder())
        self.ax.add_patch(shad)

      if plot_axis_labels:
        if frac > 0.03:
          xt = x + 1.05 * radius * math.cos(thetam)
          yt = y + 1.05 * radius * math.sin(thetam)

          thetam %= 2 * math.pi

          if 0 < thetam and thetam < math.pi:
            valign = 'bottom'
          elif thetam == 0 or thetam == math.pi:
            valign = 'center'
          else:
            valign = 'top'

          if thetam > math.pi / 2.0 and thetam < 3.0 * math.pi / 2.0:
            halign = 'right'
          elif thetam == math.pi / 2.0 or thetam == 3.0 * math.pi / 2.0:
            halign = 'center'
          else:
            halign = 'left'

          t = self.ax.text(xt, yt, label,
                           size=pixelToPoint(self.prefs['subtitle_size'], self.dpi),
                           horizontalalignment=halign,
                           verticalalignment=valign)

          t.set_fontname(self.prefs['font'])
          t.set_fontsize(pixelToPoint(self.prefs['text_size'], self.dpi))

          texts.append(t)

        if autopct is not None:
          xt = x + pctdistance * radius * math.cos(thetam)
          yt = y + pctdistance * radius * math.sin(thetam)
          if isinstance(autopct, str):
            s = autopct % (100. * frac)
          elif callable(autopct):
            s = autopct(100. * frac)
          else:
            raise TypeError('autopct must be callable or a format string')

          t = self.ax.text(xt, yt, s,
                           horizontalalignment='center',
                           verticalalignment='center')

          t.set_fontname(self.prefs['font'])
          t.set_fontsize(pixelToPoint(self.prefs['text_size'], self.dpi))

          autotexts.append(t)

      theta1 = theta2
      i += 1

    self.legendData.reverse()

    self.ax.set_xlim((-1.25, 1.25))
    self.ax.set_ylim((-1.25, 1.25))
    self.ax.set_axis_off()

    if autopct is None:
      return slices, texts
    else:
      return slices, texts, autotexts

  min_amount = .1

  def getLegendData(self):

    return self.legendData

  def draw(self):

    self.ylabel = ''
    self.prefs['square_axis'] = True
    PlotBase.draw(self)

    def my_display(x):
      if x > 100 * self.min_amount:
        return '%.1f' % x + '%'
      else:
        return ""

    nLabels = self.pdata.getNumberOfLabels()
    explode = [0.] * nLabels
    if nLabels > 0:
      explode[0] = 0.1
    self.wedges, text_labels, percent = self.pie(explode=explode, autopct=my_display)
