########################################################################
# $HeadURL $
# File: RequestUtils.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2013/04/16 13:13:20
########################################################################

""" :mod: RequestUtils 
    =======================
 
    .. module: RequestUtils
    :synopsis: utilities for RMS
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    utilities for RMS

    TODO: not used, OBSOLETE

"""

__RCSID__ = "$Id $"

##
# @file RequestUtils.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2013/04/16 13:13:29
# @brief Definition of RequestUtils class.

## imports 


########################################################################
class RMSError(Exception):
  """
  .. class:: RMSError
  
  """
  def __init__( self, msg ):
    """c'tor

    :param self: self reference
    :param str msg: error message
    """
    self.msg = msg
  def __str__( self ):
    """ str() op """
    return str(self.msg)

def RMSSerialError( RMSError ):
  """ 
  .. class:: RMSSerialError

  thrown in ctors
  """
  def __init__(self, msg ):
    """ c'tor """
    RMSError.__init__( self, msg )
