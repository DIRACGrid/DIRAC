# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/LoggingSystem/private/backends/PrintBackend.py,v 1.4 2008/01/24 19:04:32 mseco Exp $
__RCSID__ = "$Id: PrintBackend.py,v 1.4 2008/01/24 19:04:32 mseco Exp $"
""" This backend just print the log messages through the standar output
"""
from DIRAC.LoggingSystem.private.backends.BaseBackend import BaseBackend

class PrintBackend( BaseBackend ):

  def doMessage( self, messageObject ):
    print self.composeString( messageObject )

