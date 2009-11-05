########################################################################
# $Header: /local/reps/dirac/DIRAC3/scripts/DIRACEnvironment.py,v 1.3 2008/03/18 18:40:51 rgracian Exp $
# File :   DIRACEnvironment.py
# Author : Ricardo Graciani
########################################################################
__RCSID__   = "$Id: DIRACEnvironment.py,v 1.3 2008/03/18 18:40:51 rgracian Exp $"
__VERSION__ = "$Revision: 1.3 $"
"""
   Magic file to be imported by all python scripts to properly discover and 
   setup the DIRAC environment
"""

import os, sys

try:
  import DIRAC
except ImportError:
  """
     from the location of the script that import this one (this should only be
     possible if they are in the same directory) tries to setup the PYTHONPATH
  """
  scriptsPath = os.path.realpath( os.path.dirname( __file__ ) )
  rootPath = os.path.dirname( scriptsPath )  

  sys.path.insert( 0, rootPath )

  try:
    import DIRAC
  except ImportError:
    print "ERROR Can not import DIRAC."
    print "ERROR Check if %s contains a proper DIRAC distribution" % rootPath
    raise
    sys.exit(-1)

