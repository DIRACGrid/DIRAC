""" DIRAC Workload Management System Client module to get available memory from mjf
"""
__RCSID__ = "$Id$"

import os
import urllib

def getJobFeatures():
  features = {}
  if 'JOBFEATURES' not in os.environ:
    return features
  for item in ( 'cpufactor_lrms', 'cpu_limit_secs_lrms', 'cpu_limit_secs', 'wall_limit_secs_lrms', 'wall_limit_secs', 'disk_limit_GB',
                'jobstart_secs', 'mem_limit_MB', 'allocated_CPU ', 'shutdowntime_job' ):
    fname = os.path.join( os.environ['JOBFEATURES'], item )
    try:
      val = urllib.urlopen( fname ).read()
    except:
      val = 0
    features[item] = val
  return features


def getMemoryFromMJF():
  features = getJobFeatures()
  MaxRAM = features.get( 'mem_limit_MB' )
  if MaxRAM:
    return MaxRAM
  else:
    return None

def getMemoryFromProc():
    meminfo = dict( ( i.split()[0].rstrip( ':' ), int( i.split()[1] ) ) for i in open( '/proc/meminfo' ).readlines() )
    MaxRAM = meminfo['MemTotal']
    if MaxRAM:
      return MaxRAM / 1024
    else:
      return None
