# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/Utilities/ReturnValues.py,v 1.1 2007/03/09 15:33:19 rgracian Exp $
__RCSID__ = "$Id: ReturnValues.py,v 1.1 2007/03/09 15:33:19 rgracian Exp $"

"""
   DIRAC return dictionary
   Message values are converted to string
   keys are converted to string
"""
def S_ERROR( messageString = '' ):
  return { 'OK' : False, 'Message' : str( messageString )  }
  
def S_OK( value = ''  ):
  return { 'OK' : True, 'Value' : value }
