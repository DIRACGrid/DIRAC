# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/Utilities/Plotting.py,v 1.5 2008/06/27 10:38:08 rgracian Exp $
__RCSID__ = "$Id: Plotting.py,v 1.5 2008/06/27 10:38:08 rgracian Exp $"
"""
   A simple set of wrapper functions for creating plots (based on the examples
   from the graph tool).
"""
from DIRAC import S_OK, S_ERROR, gLogger
from graphtool.graphs.common_graphs import PieGraph
from graphtool.graphs.common_graphs import BarGraph
from graphtool.graphs.common_graphs import CumulativeGraph
from graphtool.tools.common import expand_string
from pylab import *

import os,time

class HistogramGraph(BarGraph):

  def make_bottom_text(self):
    """
    Attempt to calculate the maximum, minimum, average, and current values
    for the graph.  These statistics will be printed on the bottom of the
    graph.
    """
    units = str(self.metadata.get('column_units','')).strip()
    data_max = float(self.metadata.get('max_value',None))
    data_min = float(self.metadata.get('min_value',None))
    data_mean = float(self.metadata.get('mean_value',None))
    data_std = float(self.metadata.get('std_value',None))
    retval = ''
    if data_max != None:
      try:
        retval += "Maximum: " + pretty_float( data_max ) + " " + units
      except Exception, e:
        pass
    if data_min != None:
      try:
        retval += ", Minimum: " + pretty_float( data_min ) + " " + units
      except Exception, e:
        pass
    if data_mean != None:
      try:
        retval += ", Mean: " + pretty_float( data_mean ) + " " + units
      except Exception, e:
        pass
    if data_std != None:
      try:
        retval += ", Standard deviation: " + pretty_float( data_std )
      except Exception, e:
        pass
    return retval

#############################################################################
def pieChart(path,data,metadata):
  """Plot a pie chart with
     Data {'Name':'Value'}
     Metadata {'title':'Name'}
     Path '/path/'
  """
  try:
    pie = PieGraph()
    coords = pie.run( data, path, metadata )
  except Exception,x:
    return errorReport(str(x),'Could not plot pie chart')

  if not os.path.exists(path):
    return S_ERROR('Requested file was not created')

  return S_OK(path)

#############################################################################
def barChart(path,data,metadata):
  """Plot a bar chart (see example commented below).
  """
  try:
    BG = BarGraph()
    BG(data, path, metadata)
  except Exception,x:
    return errorReport(str(x),'Could not plot bar chart')

  if not os.path.exists(path):
    return S_ERROR('Requested file was not created')

  return S_OK(path)

#############################################################################
def cumulativePlot(path,data,metadata):
  """Make a cumulative plot (see example commented below).
  """
  try:
    CG = CumulativeGraph()
    CG( data, path, metadata )
  except Exception,x:
    return errorReport(str(x),'Could not create cumulative plot')

  if not os.path.exists(path):
    return S_ERROR('Requested file was not created')

  return S_OK(path)

#############################################################################
def histogram(path,data,metadata):
  """ Plot a histogram of the supplied data
  """
  try:
    count, bins, patches = hist(data,100)
    try:
      metadata['max_value'] = max(values)
    except:
      pass
    try:
      metadata['min_value'] = min(values)
    except:
      pass
    try:
      metadata['mean_value'] = mean(values)
    except:
      pass
    try:
      metadata['std_value'] = std(values)
    except:
      pass

    histData = {}
    for i in range(len(bins)):
      histData[bins[i]] = count[i]
    hist = HistogramGraph()
    hist(histData, path, metadata)

  except Exception,x:
    return errorReport(str(x),'Could not create histogram')
  if not os.path.exists(path):
    return S_ERROR('Requested file was not created')
  return S_OK(path)

#############################################################################
def errorReport(error,message=None):
  """Internal function to report errors and exit with an S_ERROR()
  """
  if not message:
    message = error
  gLogger.warn(error)
  return S_ERROR(message)

#############################################################################
#path = os.getcwd()
#result = pieChart({'foo':45, 'bar':55},{'title':'Hello Graphing World!'},'%s/pie.png' %path)
#print result

#result = barChart({'Team A':4, 'Team B':7},{'title':'First Bar Example'},'%s/bar.png' %path)
#print result

#Official cumulative plot example
#import random, datetime
#span = 3600
#max_value = 40
# Generate our time series
#def make_time_data( ):
#    end_time = time.time(); end_time -= end_time % span
#    begin_time = end_time - 24*span
#    data = {}
#    for i in range(begin_time, end_time, span):
#        data[i] = random.random()*max_value
#    return begin_time, end_time, data

# Create and plot cumulative plot.
#CG = CumulativeGraph()
#begin_time, end_time, data1 = make_time_data()
#begin_time, end_time, data2 = make_time_data()
#print data1
#print data2
#result = cumulativePlot({'Team A': data1, 'Team B': data2},{'title':'Some Cumulative Data', 'starttime':begin_time, 'endtime':end_time, 'span':span, 'is_cumulative':True },'%s/cumulative.png' %path)

#############################################################################
#example for 'DIRAC' CPU time consumed

#from DIRAC.Interfaces.API.Dirac import Dirac
#from DIRAC.Core.Utilities.Time import fromString,toEpoch
#d = Dirac()
#jobID = 19302
#result = d.getJobCPUTime(jobID)
#data = result['Value'][jobID]
#newData = {}
#for dTime,value in data.items():
#  newData[toEpoch(fromString(dTime))]=float(value)

#data = {'CPU(s)': newData}
#times = newData.keys()
#times.sort()
#begin_time = times[0]
#end_time = times[-1]
#span = 30*60
#metadata = {'title':'CPU Time %s' %jobID, 'starttime':begin_time, 'endtime':end_time, 'span':span, 'is_cumulative':True }
#result = cumulativePlot(data,metadata,'%s/cumulativeCPU.png' %path)
#print result

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#