#!/usr/bin/env python
from __future__ import print_function
from __future__ import absolute_import
from __future__ import division
import make_dfc_plots_lib as dfcPlot
import os
import sys


if len( sys.argv ) < 2 or sys.argv[1] in ( '-h', '--help' ):
  print("Usage: %s <jobName>" % sys.argv[0])
  print("This will generate all the plots for a given performance test")
  print("That is histograms and plots for read, write and remove, with")
  print("and without max.")
  print("It uses the output of the extract.sh script")

folder = sys.argv[1] 

op_types = ['list', 'insert', 'remove']

for op in op_types:
  bp =  os.path.join(folder, "%s_"%op)
  fn = "%sgood.txt"%bp
  print("fn %s" % fn)
  if os.path.exists(fn):
    parsed_data = dfcPlot.parse_job_result_file(fn)
    analyzed_data = dfcPlot.analyze_data(parsed_data, binSize = 60)
    dfcPlot.make_plot(analyzed_data, base_filename = bp , plot_title = op, disable_max = False )
    dfcPlot.make_plot(analyzed_data, base_filename = bp + 'no_max_' , plot_title = op, disable_max = True )
