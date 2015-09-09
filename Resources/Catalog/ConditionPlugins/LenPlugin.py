# Plugin that test the length of lfn
from DIRAC.Resources.Catalog.FCConditionParser import bcolors

class LenPlugin( object ):
  def __init__( self, conditions ):
    self.conditions = conditions

  def eval( self, **kwargs ):
    test = "%s%s" % ( len( kwargs.get( 'lfn', '' ) ), self.conditions )
    ret = eval( test )
    print bcolors.WARNING + "\t\ttesting %s : %s" % ( test, ret ) + bcolors.ENDC
    return ret
