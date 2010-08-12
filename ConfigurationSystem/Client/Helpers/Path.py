########################################################################
# $HeadURL: svn+ssh://svn.cern.ch/reps/dirac/DIRAC/trunk/DIRAC/WorkloadManagementSystem/scripts/dirac-admin-submit-pilot-for-job.py $
# File :   Path.py
# Author : Ricardo Graciani
########################################################################
__RCSID__ = "$Id: dirac-admin-submit-pilot-for-job.py 18161 2009-11-11 12:07:09Z acasajus $"
"""
Some Helper class to build CFG paths from tuples
"""

cfgInstallSection = 'LocalInstallation'

def cfgPath( *args ):
  """
  Basic method to make a path out of a tuple of string, any of them can be already a path
  """
  return '/'.join( [str( k ) for k in args] )

def cfgInstallPath( *args ):
  """
  Path to Installation/Configuration Options
  """
  return cfgPath( cfgInstallSection, *args )

