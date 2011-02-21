# $HeadURL$
"""
   DIRAC return dictionary
   Message values are converted to string
   keys are converted to string
"""

__RCSID__ = "$Id$"

def S_ERROR( messageString = '' ):
  """ return value on error confition 
  :param string messageString: error description
  """
  return { 'OK' : False, 'Message' : str( messageString )  }
  
def S_OK( value = '' ):
  """ return value
  :param mixed value: value to be returned
  """
  return { 'OK' : True, 'Value' : value }

