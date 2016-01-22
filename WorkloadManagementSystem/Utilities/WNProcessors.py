""" DIRAC Workload Management System utility module to get the WN number of processors
"""
__RCSID__ = "$Id$"

import os

def getMachineFeatures():
  features = {}
  if 'MACHINEFEATURES' not in os.environ:
    return features
  for item in ( 'hs06', 'shutdowntime', 'jobslots', ' phys_cores ', 'log_cores ', 'shutdown_command' ):
    fname = os.path.join( os.environ['MACHINEFEATURES'], item )
    try:
      f = open( fname, 'r' )
      val = f.read()
      f.close()
    except:
      val = 0
    features[item] = val
  return features


def getProcessorFromMJF():
  features = getMachineFeatures()
  NumberOfProcessor = features.get( 'log_cores' )
  return NumberOfProcessor

