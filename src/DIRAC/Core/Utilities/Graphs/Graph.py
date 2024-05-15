""" Graph is a class providing layouts for the complete plot images including
    titles multiple plots and a legend

    The DIRAC Graphs package is derived from the GraphTool plotting package of the
    CMS/Phedex Project by ... <to be added>
"""

import datetime
import importlib
import os
import time
from matplotlib.backends.backend_agg import FigureCanvasAgg
from matplotlib.figure import Figure

from DIRAC import gLogger
from DIRAC.Core.Utilities.Graphs.GraphData import GraphData
from DIRAC.Core.Utilities.Graphs.GraphUtilities import add_time_to_title, evalPrefs, pixelToPoint, to_timestamp
from DIRAC.Core.Utilities.Graphs.Legend import Legend

DEBUG = 0


class Graph:
    """Base class for all other Graphs"""

    def layoutFigure(self, legend):
        prefs = self.prefs
        nsublines = left = bottom = None

        # Get the main Figure object
        # self.figure = Figure()
        figure = self.figure
        self.canvas = FigureCanvasAgg(figure)

        dpi = prefs["dpi"]
        width = float(prefs["width"])
        height = float(prefs["height"])
        width_inch = width / dpi
        height_inch = height / dpi
        figure.set_size_inches(width_inch, height_inch)
        figure.set_dpi(dpi)
        figure.set_facecolor(prefs.get("background_color", "white"))

        figure_padding = float(prefs["figure_padding"])
        figure_left_padding = float(prefs.get("figure_left_padding", figure_padding))
        figure_right_padding = float(prefs.get("figure_right_padding", figure_padding))
        figure_top_padding = float(prefs.get("figure_top_padding", figure_padding))
        figure_bottom_padding = float(prefs.get("figure_bottom_padding", figure_padding))

        text_size = prefs.get("text_size", 8)
        text_padding = prefs.get("text_padding", 5)

        #######################################
        # Make the graph title

        title = prefs.get("title", "")
        subtitle = ""
        title_size = 0
        title_padding = 0
        if title:
            title_size = prefs.get("title_size", 1.5 * text_size)
            title_padding = float(prefs.get("title_padding", 1.5 * text_padding))
            figure.text(
                0.5,
                1.0 - (title_size + figure_padding) / height,
                title,
                ha="center",
                va="bottom",
                size=pixelToPoint(title_size, dpi),
            )

            subtitle = prefs.get("subtitle", "")
            if subtitle:
                sublines = subtitle.split("\n")
                nsublines = len(sublines)
                subtitle_size = prefs.get("subtitle_size", 1.2 * text_size)
                subtitle_padding = float(prefs.get("subtitle_padding", 1.2 * text_padding))
                top_offset = subtitle_size + subtitle_padding + title_size + figure_padding
                for subline in sublines:
                    figure.text(
                        0.5,
                        1.0 - (top_offset) / height,
                        subline,
                        ha="center",
                        va="bottom",
                        size=pixelToPoint(subtitle_size, dpi),
                        fontstyle="italic",
                    )
                    top_offset += subtitle_size + subtitle_padding

        ########################################
        # Evaluate the plot area dimensions
        graph_width = width - figure_left_padding - figure_right_padding
        graph_height = height - figure_top_padding - figure_bottom_padding
        if title:
            graph_height = graph_height - title_padding - title_size
        if subtitle:
            graph_height = graph_height - nsublines * (subtitle_size + subtitle_padding)
        graph_left = figure_left_padding
        graph_bottom = figure_bottom_padding

        #########################################
        # Make the plot time stamp if requested
        flag = prefs.get("graph_time_stamp", True)
        if flag:
            timeString = "Generated on " + datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S ") + "UTC"
            time_size = prefs["text_size"] * 0.8
            figure.text(
                0.995, 0.005, timeString, ha="right", va="bottom", size=pixelToPoint(time_size, dpi), fontstyle="italic"
            )

        #########################################
        # Make the graph Legend if requested

        legend_flag = prefs["legend"]
        legend_ax = None
        column_width = legend.column_width
        if legend_flag:
            legend_position = prefs["legend_position"]
            # legend_width = float(prefs['legend_width'])
            # legend_height = float(prefs['legend_height'])
            legend_width, legend_height, legend_max_height = legend.getLegendSize()
            legend_padding = float(prefs["legend_padding"])
            if legend_position in ["right", "left"]:
                # One column in case of vertical legend
                legend_width = column_width + legend_padding
                legend_height = min(graph_height, legend_max_height)
                bottom = (height - title_size - title_padding - legend_height) / 2.0 / height
                if legend_position == "right":
                    left = 1.0 - (figure_padding + legend_width) / width
                else:
                    left = figure_padding / width
                    graph_left = graph_left + legend_width
                graph_width = graph_width - legend_width - legend_padding
            elif legend_position == "bottom":
                bottom = figure_padding / height
                left = (width - legend_width) / 2.0 / width
                graph_height = graph_height - legend_height - legend_padding
                graph_bottom = graph_bottom + legend_height + legend_padding

            legend_rect = (left, bottom, legend_width / width, legend_height / height)
            legend_ax = figure.add_axes(legend_rect)

        ###########################################
        # Make the plot spots
        plot_grid = prefs["plot_grid"]
        nx = int(plot_grid.split(":")[0])
        ny = int(plot_grid.split(":")[1])

        plot_axes = []
        for j in range(ny - 1, -1, -1):
            for i in range(nx):
                plot_rect = (
                    (graph_left + graph_width * i / nx) / width,
                    (graph_bottom + graph_height * j / ny) / height,
                    graph_width / nx / width,
                    graph_height / ny / height,
                )

                plot_axes.append(figure.add_axes(plot_rect))

        return legend_ax, plot_axes

    def makeTextGraph(self, text="Empty image"):
        """Make an empty text image"""

        self.figure = Figure()
        figure = self.figure
        self.canvas = FigureCanvasAgg(figure)

        prefs = self.prefs
        dpi = prefs["dpi"]
        width = float(prefs["width"])
        height = float(prefs["height"])
        width_inch = width / dpi
        height_inch = height / dpi
        figure.set_size_inches(width_inch, height_inch)
        figure.set_dpi(dpi)
        figure.set_facecolor("white")

        text_size = prefs.get("text_size", 12)
        figure.text(0.5, 0.5, text, horizontalalignment="center", size=pixelToPoint(text_size, dpi))

    def makeGraph(self, data, *args, **kw):
        start = time.time()
        plot_type = None
        # Evaluate all the preferences
        self.prefs = evalPrefs(*args, **kw)
        prefs = self.prefs

        if DEBUG:
            print("makeGraph time 1", time.time() - start)
            start = time.time()

        if "text_image" in prefs:
            self.makeTextGraph(str(prefs["text_image"]))
            return

        # Evaluate the number of plots and their requested layout
        metadata = prefs.get("metadata", {})
        plot_grid = prefs.get("plot_grid", "1:1")
        nx = int(plot_grid.split(":")[0])
        ny = int(plot_grid.split(":")[1])
        nPlots = nx * ny
        if nPlots == 1:
            if not isinstance(data, list):
                data = [data]
            if not isinstance(metadata, list):
                metadata = [metadata]
        else:
            if not isinstance(data, list):
                # return S_ERROR('Single data for multiplot graph')
                print("Single data for multiplot graph")
                return
            if not isinstance(metadata, list):
                metaList = []
                for _ in range(nPlots):
                    metaList.append(metadata)
                metadata = metaList

        # Initialize plot data
        graphData = []
        plot_prefs = []
        for i in range(nPlots):
            plot_prefs.append(evalPrefs(prefs, metadata[i]))
            gdata = GraphData(data[i])
            if i == 0:
                plot_type = plot_prefs[i]["plot_type"]
            if "sort_labels" in plot_prefs[i]:
                reverse = plot_prefs[i].get("reverse_labels", False)
                gdata.sortLabels(plot_prefs[i]["sort_labels"], reverse_order=reverse)
            if "limit_labels" in plot_prefs[i]:
                if plot_prefs[i]["limit_labels"] > 0:
                    gdata.truncateLabels(plot_prefs[i]["limit_labels"])
            if "cumulate_data" in plot_prefs[i]:
                gdata.makeCumulativeGraph()
            plot_title = plot_prefs[i].get("plot_title", "")
            if plot_title != "NoTitle":
                begin = ""
                end = ""
                if "starttime" in plot_prefs[i] and "endtime" in plot_prefs[i]:
                    begin = to_timestamp(plot_prefs[i]["starttime"])
                    end = to_timestamp(plot_prefs[i]["endtime"])
                elif gdata.key_type == "time":
                    begin = gdata.min_key
                    end = gdata.max_key
                if begin and end:
                    time_title = add_time_to_title(begin, end)
                    if plot_title:
                        plot_title += ":"
                    plot_prefs[i]["plot_title"] = plot_title + " " + time_title
            graphData.append(gdata)

        # Do not make legend for the plot with non-string keys (except for PieGraphs)
        if not graphData[0].subplots and graphData[0].key_type != "string" and not plot_type == "PieGraph":
            prefs["legend"] = False
        if prefs["legend"] and graphData[0].key_type != "string" and plot_type == "PieGraph":
            graphData[0].initialize(key_type="string")

        legend = Legend(graphData[0], None, prefs)
        self.figure = Figure()

        # Make Water Mark
        image = prefs.get("watermark", None)
        self.drawWaterMark(image)

        legend_ax, plot_axes = self.layoutFigure(legend)

        if DEBUG:
            print("makeGraph time layout", time.time() - start)
            start = time.time()

        # Make plots
        for i in range(nPlots):
            plot_type = plot_prefs[i]["plot_type"]
            try:
                plotModule = importlib.import_module(f"DIRAC.Core.Utilities.Graphs.{plot_type}")
            except ModuleNotFoundError as x:
                print(f"Failed to import graph type {plot_type}: {str(x)}")
                return None

            ax = plot_axes[i]
            plot = getattr(plotModule, plot_type)(graphData[i], ax, plot_prefs[i])
            plot.draw()

        if DEBUG:
            print("makeGraph time plots", time.time() - start)
            start = time.time()

        # Make legend
        if legend_ax:
            legend.setAxes(legend_ax)
            legend.draw()

        if DEBUG:
            print("makeGraph time legend", time.time() - start)
            start = time.time()
        # return S_OK()

    def drawWaterMark(self, imagePath=None):
        """Make the figure water mark"""

        prefs = self.prefs

        try:
            from PIL import Image, ImageEnhance
        except ImportError:
            return

        if not imagePath:
            if "watermark" in prefs:
                imagePath = os.path.expandvars(os.path.expanduser(prefs["watermark"]))

        if not imagePath:
            return

        try:
            image = Image.open(imagePath)
            enh = ImageEnhance.Contrast(image)
            i = enh.enhance(0.1)
            img_size = i.size
            resize = 1.0
            if prefs["width"] < img_size[0]:
                resize = prefs["width"] / float(img_size[0])
            if prefs["height"] < img_size[1]:
                resize = min(resize, prefs["height"] / float(img_size[1]))
            box = (
                0.5 - img_size[0] / float(prefs["width"]) * resize / 2.0,
                0.5 - img_size[1] / float(prefs["height"]) * resize / 2.0,
                img_size[0] / float(prefs["width"]) * resize,
                img_size[1] / float(prefs["height"]) * resize,
            )
            # print box
            ax_wm = self.figure.add_axes(box)
            ax_wm.imshow(i, origin="lower", aspect="equal", zorder=-10)
            ax_wm.axis("off")
            ax_wm.set_frame_on(False)
            ax_wm.set_clip_on(False)
        except Exception:
            gLogger.exception("Caught exception")

    def writeGraph(self, fname, fileFormat="PNG"):
        """Write out the resulting graph to a file with fname in a given format"""

        self.canvas.draw()
        if fileFormat.lower() == "png":
            self.canvas.print_png(fname)
        else:
            gLogger.error(f"File format '{fileFormat}' is not supported!")
