""" Just an utilities collector
"""

import types
from  DIRAC import S_OK, S_ERROR

def checkArgumentFormat( path ):
  """ returns {'/this/is/an/lfn.1':False, '/this/is/an/lfn.2':False ...}
  """

  if type( path ) in types.StringTypes:
    return S_OK( {path:False} )
  elif type( path ) in ( types.ListType, types.DictType ):
    return S_OK( dict( [( url, False ) for url in path if type( url ) in types.StringTypes] ) )
  else:
    return S_ERROR( "%s.checkArgumentFormat: Supplied path is not of the correct format." % ( __class__.__name__ ) )
