# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/Utilities/Os.py,v 1.1 2007/03/09 15:33:19 rgracian Exp $
__RCSID__ = "$Id: Os.py,v 1.1 2007/03/09 15:33:19 rgracian Exp $"
"""
   Collection of DIRAC useful os related modules
   by default on Error they return None
"""

from types                          import StringTypes
from string                         import split,strip,join

from DIRAC.Core.Utilities import List

def uniquePath( path = None ):
  """ 
     Utility to squeeze the string containing a PATH-like value to
     leave only unique elements preserving the original order
  """     

  if not StringTypes.__contains__( type( path ) ):
    return None
  
  try:
    elements = List.uniqueElements( List.fromChar( path, ":" ) )
    return join( elements, ":" )
  except:
    return None
