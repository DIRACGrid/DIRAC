""" BarGraph represents bar graphs with vertical bars both simple
    and stacked.

    The DIRAC Graphs package is derived from the GraphTool plotting package of the
    CMS/Phedex Project by ... <to be added>
"""
import datetime

from pylab import setp
from matplotlib.patches import Polygon
from matplotlib.dates import date2num

from DIRAC.Core.Utilities.Graphs.PlotBase import PlotBase
from DIRAC.Core.Utilities.Graphs.GraphUtilities import (
    to_timestamp,
    pixelToPoint,
    PrettyDateLocator,
    PrettyDateFormatter,
    PrettyScalarFormatter,
)


class BarGraph(PlotBase):

    """
    The BarGraph class is a straightforward bar graph; given a dictionary
    of values, it takes the keys as the independent variable and the values
    as the dependent variable.
    """

    def __init__(self, data, ax, prefs, *args, **kw):
        PlotBase.__init__(self, data, ax, prefs, *args, **kw)
        if "span" in self.prefs:
            self.width = self.prefs["span"]
        else:
            self.width = 1.0
            if self.gdata.key_type == "time":
                # Try to guess the time bin span
                nKeys = self.gdata.getNumberOfKeys()
                self.width = (max(self.gdata.all_keys) - min(self.gdata.all_keys)) / (nKeys - 1)

    def draw(self):
        PlotBase.draw(self)
        self.x_formatter_cb(self.ax)

        if self.gdata.isEmpty():
            return None

        tmp_x = []
        tmp_y = []

        # Evaluate the bar width
        width = float(self.width)
        if self.gdata.key_type == "time":
            # width = (1 - self.bar_graph_space) * width / 86400.0
            width = width / 86400.0
            offset = 0
        elif self.gdata.key_type == "string":
            self.bar_graph_space = 0.1
            width = (1 - self.bar_graph_space) * width
            offset = self.bar_graph_space / 2.0
        else:
            offset = 0

        start_plot = 0
        end_plot = 0
        if "starttime" in self.prefs and "endtime" in self.prefs:
            start_plot = date2num(datetime.datetime.utcfromtimestamp(to_timestamp(self.prefs["starttime"])))
            end_plot = date2num(datetime.datetime.utcfromtimestamp(to_timestamp(self.prefs["endtime"])))

        nKeys = self.gdata.getNumberOfKeys()
        tmp_b = []
        if "log_yaxis" in self.prefs:
            tmp_b = [0.001] * nKeys
            ymin = 0.001
        else:
            tmp_b = [0.0] * nKeys
            ymin = 0.0

        self.polygons = []
        self.lines = []
        labels = self.gdata.getLabels()
        labels.reverse()

        # If it is a simple plot, no labels are used
        # Evaluate the most appropriate color in this case
        if self.gdata.isSimplePlot():
            labels = [("SimplePlot", 0.0)]
            color = self.prefs.get("plot_color", "Default")
            if color.find("#") != -1:
                self.palette.setColor("SimplePlot", color)
            else:
                labels = [(color, 0.0)]

        seq_b = [(self.gdata.max_num_key + width, 0.0), (self.gdata.min_num_key, 0.0)]
        zorder = 0.0
        dpi = self.prefs.get("dpi", 100)
        for label, num in labels:
            color = self.palette.getColor(label)
            ind = 0
            tmp_x = []
            tmp_y = []
            tmp_t = []
            plot_data = self.gdata.getPlotNumData(label)
            for key, value, error in plot_data:
                if value is None:
                    value = 0.0

                tmp_x.append(offset + key)
                # tmp_y.append(ymin)
                tmp_y.append(0.001)
                tmp_x.append(offset + key)
                tmp_y.append(float(value) + tmp_b[ind])
                tmp_x.append(offset + key + width)
                tmp_y.append(float(value) + tmp_b[ind])
                tmp_x.append(offset + key + width)
                # tmp_y.append(ymin)
                tmp_y.append(0.001)
                tmp_t.append(float(value) + tmp_b[ind])
                ind += 1
            seq_t = list(zip(tmp_x, tmp_y))
            seq = seq_t + seq_b
            poly = Polygon(seq, facecolor=color, fill=True, linewidth=pixelToPoint(0.2, dpi), zorder=zorder)
            self.ax.add_patch(poly)
            self.polygons.append(poly)
            tmp_b = list(tmp_t)
            zorder -= 0.1

        tight_bars_flag = self.prefs.get("tight_bars", False)
        if tight_bars_flag:
            setp(self.polygons, linewidth=0.0)

        # pivots = keys
        # for idx in range(len(pivots)):
        #    self.coords[ pivots[idx] ] = self.bars[idx]

        ymax = max(tmp_b)
        ymax *= 1.1

        if "log_yaxis" in self.prefs:
            ymin = 0.001
        else:
            ymin = min(min(tmp_b), 0.0)
            ymin *= 1.1

        xmax = max(tmp_x)
        if self.log_xaxis:
            xmin = 0.001
        else:
            xmin = 0

        ymin = self.prefs.get("ymin", ymin)
        ymax = self.prefs.get("ymax", ymax)
        xmin = self.prefs.get("xmin", xmin)
        xmax = self.prefs.get("xmax", xmax)

        self.ax.set_xlim(xmin=xmin, xmax=xmax + offset)
        self.ax.set_ylim(ymin=ymin, ymax=ymax)
        if self.gdata.key_type == "time":
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
            ax.set_xticks([i + 0.5 for i in ticks])
            ax.set_xticklabels([reverse_smap[i] for i in ticks])
            labels = ax.get_xticklabels()
            ax.grid(False)
            if self.log_xaxis:
                xmin = 0.001
            else:
                xmin = 0
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
