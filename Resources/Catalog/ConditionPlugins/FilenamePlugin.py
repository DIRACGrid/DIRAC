# Plugin that test string method on the lfn
from DIRAC.Resources.Catalog.FCConditionParser import bcolors

class FilenamePlugin( object ):
  def __init__( self, conditions ):
    self.conditions = conditions

  def eval( self, **kwargs ):
    test = "'%s'.%s" % ( kwargs.get( 'lfn', '' ), self.conditions )
    ret = eval( test )
    print bcolors.WARNING + "\t\ttesting %s : %s" % ( test, ret ) + bcolors.ENDC
    return ret
