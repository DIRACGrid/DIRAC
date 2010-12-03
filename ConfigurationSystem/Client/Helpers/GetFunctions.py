########################################################################
# $HeadURL: svn+ssh://svn.cern.ch/reps/dirac/DIRAC/trunk/DIRAC/WorkloadManagementSystem/scripts/dirac-admin-submit-pilot-for-job.py $
# File :   GetFunctions.py
# Author : Ricardo Graciani
########################################################################
"""
Some Helper functions to retrieve common location from the CS
"""
__RCSID__ = "$Id: dirac-admin-submit-pilot-for-job.py 18161 2009-11-11 12:07:09Z acasajus $"

from DIRAC import gConfig

def getCSExtensions():
  """
    Return list of extensions registered in the CS
    They do not include DIRAC
  """
  return gConfig.getValue( '/DIRAC/Extensions', [] )

def getInstalledExtensions():
  """
    Return list of extensions registered in the CS and available in local installation
  """
  extensions = []
  for extension in getCSExtensions():
    try:
      exec 'import %sDIRAC' % extension
      extensions.append( '%sDIRAC' % extension )
    except:
      pass
  extensions.append( 'DIRAC' )
  return exceptions
