""" DIRAC Workload Management System utility module to get the WN number of processors
"""
__RCSID__ = "$Id$"

import os
import urllib

def getMachineFeatures():
  features = {}
  if 'MACHINEFEATURES' not in os.environ:
    return features
  for item in ( 'hs06', 'shutdowntime', 'jobslots', ' phys_cores ', 'log_cores ', 'shutdown_command' ):
    fname = os.path.join( os.environ['MACHINEFEATURES'], item )
    try:
      val = urllib.urlopen( fname ).read()
    except:
      val = 0
    features[item] = val
  return features


def getProcessorFromMJF():
  features = getMachineFeatures()
  NumberOfProcessor = features.get( ' log_cores ' )
  if NumberOfProcessor:
    return NumberOfProcessor
  else:
    return None

