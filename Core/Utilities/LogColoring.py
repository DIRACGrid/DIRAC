# $HeadURL$
__RCSID__ = "$Id$"
import sys

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
    'EXCEPT' : ( 'red', 'black', True ),
    'FATAL'  : ( 'red', 'black', True )
  }

def colorMessage( level, msg ):
  if not sys.stdout.isatty():
    return msg
  params = []
  bg, fg, bold = LEVEL_MAP[ level.upper() ]
  if bg in COLOR_MAP:
    params.append( str( COLOR_MAP[ bg ] + 40 )  )
  if fg in COLOR_MAP:
    params.append( str( COLOR_MAP[ fg ] + 30 )  )
  if bold:
    params.append( '1' )
  return "".join( ( '\x1b[', ";".join( params ), 'm', msg, '\x1b[0m') )



