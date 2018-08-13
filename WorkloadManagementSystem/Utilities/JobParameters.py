""" DIRAC Workload Management System utility module to get available memory and processors from mjf
"""
import os
import urllib2

__RCSID__ = "$Id$"


def getJobFeatures():
  features = {}
  if 'JOBFEATURES' not in os.environ:
    return features
  for item in ('allocated_cpu', 'hs06_job', 'shutdowntime_job', 'grace_secs_job', 'jobstart_secs',
               'job_id', 'wall_limit_secs',
               'cpu_limit_secs', 'max_rss_bytes', 'max_swap_bytes', 'scratch_limit_bytes'):
    fname = os.path.join(os.environ['JOBFEATURES'], item)
    try:
      val = urllib2.urlopen(fname).read()
    except BaseException:
      val = 0
    features[item] = val
  return features


def getProcessorFromMJF():
  return getJobFeatures().get('allocated_cpu')


def getMemoryFromMJF():
  return getJobFeatures().get('max_rss_bytes')


def getMemoryFromProc():
  meminfo = dict((i.split()[0].rstrip(':'), int(i.split()[1])) for i in open('/proc/meminfo').readlines())
  maxRAM = meminfo['MemTotal']
  if maxRAM:
    return maxRAM / 1024
