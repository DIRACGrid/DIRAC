# $HeadURL$
"""
   DIRAC return dictionary
   Message values are converted to string
   keys are converted to string
"""

import types

def S_ERROR( messageString = '' ):
  """ return value on error confition
  :param string messageString: error description
  """
  return { 'OK' : False, 'Message' : str( messageString )  }

def S_OK( value = None ):
  return { 'OK' : True, 'Value' : value }

def isReturnStructure( unk ):
  if type( unk ) != types.DictType:
    return False
  if 'OK' not in unk:
    return False
  if unk[ 'OK' ]:
    if 'Value' not in unk:
      return False
  else:
    if 'Message' not in unk:
      return False
  return True
