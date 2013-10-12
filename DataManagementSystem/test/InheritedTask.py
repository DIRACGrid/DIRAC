# TODO: to be removed

###########################################################
# $HeadURL: $
###########################################################
"""
 :mod: InheritedTask
 ===================
 
 .. module:: InheritedTask
 
 Helper class for testing RequestTask.

 OBSOLETE
 K.C.
"""
## imports
import time
## from DIRAC
from DIRAC.DataManagementSystem.private.RequestTask import RequestTask

__RCSID__ = "$Id: "

class InheritedTask( RequestTask ):
  """
  .. class:: InheritedTask

  Helper class for testing RequestTask.
  """

  def __init__( self, *args, **kwargs ):
    """c'tor """
    RequestTask.__init__( self, args, *kwargs )
  
  def __call__( self ):
    """ dummy call """
    self.always( "in call of %s" % str(self ) )
    self.addMark( "akey", 10 )
    time.sleep(1)
    return S_OK( self.monitor() )

