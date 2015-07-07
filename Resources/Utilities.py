""" Just an utilities collector
"""

__RCSID__ = "$Id$"

from  DIRAC import S_OK, S_ERROR

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
    return S_ERROR( "Utils.checkArgumentFormat: Supplied path is not of the correct format." )

