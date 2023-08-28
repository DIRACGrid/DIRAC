#!/usr/bin/env python
import make_dfc_plots_lib as dfcPlot  # pylint: disable=import-error
import os
import sys


if len(sys.argv) < 2 or sys.argv[1] in ("-h", "--help"):
    print(f"Usage: {sys.argv[0]} <jobName>")
    print("This will generate all the plots for a given performance test")
    print("That is histograms and plots for read, write and remove, with")
    print("and without max.")
    print("It uses the output of the extract.sh script")

folder = sys.argv[1]

op_types = ["list", "insert", "remove"]

for op in op_types:
    bp = os.path.join(folder, f"{op}_")
    fn = f"{bp}good.txt"
    print(f"fn {fn}")
    if os.path.exists(fn):
        parsed_data = dfcPlot.parse_job_result_file(fn)
        analyzed_data = dfcPlot.analyze_data(parsed_data, binSize=60)
        dfcPlot.make_plot(analyzed_data, base_filename=bp, plot_title=op, disable_max=False)
        dfcPlot.make_plot(analyzed_data, base_filename=bp + "no_max_", plot_title=op, disable_max=True)
