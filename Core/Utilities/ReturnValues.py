# $HeadURL$
__RCSID__ = "$Id$"

"""
   DIRAC return dictionary
   Message values are converted to string
   keys are converted to string
"""
def S_ERROR( messageString = '' ):
  return { 'OK' : False, 'Message' : str( messageString )  }
  
def S_OK( value = ''  ):
  return { 'OK' : True, 'Value' : value }
