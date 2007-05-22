# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/Utilities/ColorCLI.py,v 1.2 2007/05/22 18:21:32 acasajus Exp $
__RCSID__ = "$Id: ColorCLI.py,v 1.2 2007/05/22 18:21:32 acasajus Exp $"

import os
import types

gColors = { 'red':1, 'green':2, 'yellow':3, 'blue':4 }

def colorEnabled():
  if os.environ.has_key('TERM'):
    print os.environ[ 'TERM' ]
    if os.environ['TERM'] in ( 'xterm', 'xterm-color' ):
      return True
  return False

def colorize( text, color ):
  """Return colorized text"""
  global gColors
  if not colorEnabled():
    return text

  startCode = '\033[;3'
  endCode  = '\033[0m'
  if type( color ) == types.IntType:
    return "%s%sm%s%s" % ( startCode, color, text, endCode )
  try:
    return "%s%sm%s%s" % ( startCode, gColors[ color ], text, endCode )
  except:
    return text