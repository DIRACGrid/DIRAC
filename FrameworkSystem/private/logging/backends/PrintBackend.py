# $HeadURL$
__RCSID__ = "$Id$"
""" This backend just print the log messages through the standar output
"""
from DIRAC.FrameworkSystem.private.logging.backends.BaseBackend import BaseBackend
import sys

class PrintBackend( BaseBackend ):

  COLOR_MAP = {
      'black'   : 0,
      'red'     : 1,
      'green'   : 2,
      'yellow'  : 3,
      'blue'    : 4,
      'magenta' : 5,
      'cyan'    : 6,
      'white'   : 7
    }


  LEVEL_MAP = {
      'ALWAYS' : ( 'black', 'white', False ),
      'NOTICE' : ( None, 'magenta', False ),
      'INFO'   : ( None, 'green', False ),
      'VERB'   : ( None, 'cyan', False),
      'DEBUG'  : ( None, 'blue', False ),
      'WARN'   : ( None, 'yellow', False ),
      'ERROR'  : ( None, 'red', False ),
      'EXCEPT' : ( 'red', 'white', False ),
      'FATAL'  : ( 'red', 'black', False )
    }

  def doMessage( self, messageObject ):
    msg = self.composeString( messageObject )
    if not self._optionsDictionary[ 'Color' ] or not sys.stdout.isatty():
      print( msg )
      return
    params = []
    bg, fg, bold = self.LEVEL_MAP[ messageObject.getLevel() ]
    if bg in self.COLOR_MAP:
      params.append( str( self.COLOR_MAP[ bg ] + 40 )  )
    if fg in self.COLOR_MAP:
      params.append( str( self.COLOR_MAP[ fg ] + 30 )  )
    if bold:
      params.append( '1' )
    print( "".join( ( '\x1b[', ";".join( params ), 'm', msg, '\x1b[0m') ) )



