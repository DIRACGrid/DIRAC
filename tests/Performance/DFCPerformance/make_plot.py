#!/usr/bin/env python

from __future__ import print_function
from __future__ import absolute_import
from __future__ import division
import make_dfc_plots_lib as dfcPlot


from optparse import OptionParser
usage = "usage: %prog -i <filename> [options]"
parser = OptionParser(usage=usage)
parser.add_option("-i", "--input", dest="filename", help = "File to analyze")
parser.add_option("-p", "--plot", dest="plot", help = "Destination of the plot file (default: plot.png)", default = "plot.png")
parser.add_option("-d", "--hist", dest="hist", help = "Destination of the histogram file (default: hist.png)", default = "hist.png")
parser.add_option("-t", "--title", dest="title", help = "Title of the plots", type="string", default = "")
parser.add_option("-b", "--bin", dest="binSize", default=10, type="int", help = "Size of the bin in sec (default: 10)")
parser.add_option("-m", "--no-max", dest="disableMax", action="store_true", help = "Disable the max line (default: False)", default = False)
parser.add_option("-v", "--verbose", dest="verbose", action="store_true", help = "Verbose print", default = False)
(options, args) = parser.parse_args()
print(options)
if not options.filename:   # if filename is not given
  parser.error( 'Filename not given' )

filename = options.filename
binSize = options.binSize # in seconds
verbose = options.verbose
plotTitle = options.title
plotFile = options.plot
histFile = options.hist
disableMax = options.disableMax


parsed_data = dfcPlot.parse_job_result_file( filename )
analyzed_data = dfcPlot.analyze_data( parsed_data, binSize = binSize )
dfcPlot.make_plot( analyzed_data, plot_title = plotTitle, disable_max = disableMax,
                    plot_filename = plotFile, hist_filename = histFile )

