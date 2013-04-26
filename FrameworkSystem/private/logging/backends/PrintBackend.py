# $HeadURL$
__RCSID__ = "$Id$"
""" This backend just print the log messages through the standar output
"""
from DIRAC.FrameworkSystem.private.logging.backends.BaseBackend import BaseBackend
from DIRAC.Core.Utilities import LogColoring

class PrintBackend( BaseBackend ):

  def doMessage( self, messageObject ):
    msg = self.composeString( messageObject )
    if not self._optionsDictionary[ 'Color' ]:
      print( msg )
    else:
      print LogColoring.colorMessage( messageObject.getLevel(), msg )



