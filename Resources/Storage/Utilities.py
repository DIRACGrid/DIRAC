""" Storage plug-ins related utilities
"""

__RCSID__ = "$Id$"

import errno

from DIRAC import S_OK, S_ERROR

def checkArgumentFormat( path ):
  """ returns {'/this/is/an/lfn.1':False, '/this/is/an/lfn.2':False ...}
  """

  if isinstance( path, basestring ):
    return S_OK( {path:False} )
  elif isinstance( path, list ):
    return S_OK( dict( [( url, False ) for url in path if isinstance( url, basestring )] ) )
  elif isinstance( path, dict ):
    returnDict = path.copy()
    return S_OK( returnDict )
  else:
    return S_ERROR( errno.EINVAL, "Utils.checkArgumentFormat: Supplied path is not of the correct format." )