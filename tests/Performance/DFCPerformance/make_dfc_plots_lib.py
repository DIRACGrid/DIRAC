#!/usr/bin/env python

from __future__ import print_function
import matplotlib.pyplot as plt
import numpy as np
import os


def parse_job_result_file(filename):
  """ Read a file produced by extract.sh """
  f = open(filename, 'r')
  lines = f.readlines()
  f.close()
  input_data = [ map(float, line.split('\t')[0:3]) for line in lines ]
  return input_data

def analyze_data(input_data, binSize = 10):
  """ input_data = [ [startTime, endTime, queryTime] ]
      binSize in sec (default 10)
      returns 
  """  
  startTimes = []
  queryTimes = []
  intStartTimes = []
  
  for line in input_data:
    try:
      start, _end, query = line
      startTimes.append(start)
      queryTimes.append(query)
      intStartTimes.append(int(start))
    except Exception as e:
      print(e)

  
  curBinStart = intStartTimes[0]
  curBinCount = 0
  binCounts = []
  binStartTimes = [0]
  for start in intStartTimes:
    if curBinStart + binSize >= start:
      curBinCount += 1
    else:
      binCounts.append(curBinCount)
      curBinStart = start
      binStartTimes.append(curBinStart-intStartTimes[0])
      curBinCount = 1
  binCounts.append(curBinCount)
  
  minQueryTimes = []
  maxQueryTimes = []
  avgQueryTimes = []    
  medianQueryTimes = []
  
  curIndex = 0
  for binCount in binCounts:
    times = queryTimes[curIndex:curIndex + binCount]
    avgTime = sum( times ) / float( binCount )
    minTime = min( times )
    maxTime = max( times )
    medianTime = np.median( times )


    minQueryTimes.append( minTime )
    maxQueryTimes.append( maxTime )
    avgQueryTimes.append( avgTime )
    medianQueryTimes.append( medianTime )
    curIndex += binCount

  return { 'binCounts' : binCounts,
           'binStartTimes' : binStartTimes,
           'minQueryTimes' : minQueryTimes,
           'maxQueryTimes' : maxQueryTimes,
           'avgQueryTimes' : avgQueryTimes,
           'medianQueryTimes' : medianQueryTimes,
         }


def make_plot( analyzed_data, base_filename = "", plot_title = "", disable_max = False,
                plot_filename = None, hist_filename = None ):

  binStartTimes = analyzed_data['binStartTimes']
  minQueryTimes = analyzed_data['minQueryTimes']
  maxQueryTimes = analyzed_data['maxQueryTimes']
  avgQueryTimes = analyzed_data['avgQueryTimes']
  medianQueryTimes = analyzed_data['medianQueryTimes']
  binCounts = analyzed_data['binCounts']


  if os.path.isdir(base_filename):
    base_filename += '/'

  _fig, ax1 = plt.subplots()
  ax1.set_xlabel('Time elapsed (s)')
  ax1.set_ylabel('Query time (s)')
  l1 = ax1.plot(binStartTimes, minQueryTimes, label = "min query time")
  if not disable_max:
    l2 = ax1.plot( binStartTimes, maxQueryTimes, label = "max query time")
  l3 = ax1.plot(binStartTimes, avgQueryTimes, label = "avg query time")
  l5 = ax1.plot(binStartTimes, medianQueryTimes, label = "median query time")
  
  ax2 = ax1.twinx()
  l4 = ax2.plot(binStartTimes, binCounts, '+',  label = "concurent queries")
  ax2.set_ylabel("Query count")
  lns = l1
  if not disable_max:
    lns += l2
  lns +=  l3 + l4 + l5
  labs =  [l.get_label() for l in lns]
  plt.legend(lns, labs)
  plt.title(plot_title)
  plt.savefig( plot_filename if plot_filename else base_filename + 'plot.png' )
  plt.clf()
  plt.hist(binCounts, len(binCounts))
  plt.xlabel("Concurrent queries")
  plt.ylabel("Occurrences")
  plt.title(plot_title)
  plt.savefig( hist_filename if hist_filename else base_filename + 'hist.png' )
