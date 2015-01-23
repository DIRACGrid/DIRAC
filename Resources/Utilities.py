""" Just an utilities collector
"""

__RCSID__ = "$Id$"

import types
import errno
from  DIRAC import S_OK, S_ERROR_N as S_ERROR, DError


def checkArgumentFormat( path ):
  """ returns {'/this/is/an/lfn.1':False, '/this/is/an/lfn.2':False ...}
  """

  if type( path ) in types.StringTypes:
    return S_OK( {path:False} )
  elif type( path ) == types.ListType:
    return S_OK( dict( [( url, False ) for url in path if type( url ) in types.StringTypes] ) )
  elif type( path ) == types.DictType:
    returnDict = path.copy()
    return S_OK( returnDict )
  else:
    return S_ERROR( DError( errno.EINVAL, "Supplied path is not of the correct format." ) )
      
