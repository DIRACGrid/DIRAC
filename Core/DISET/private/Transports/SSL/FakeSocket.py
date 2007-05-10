# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/DISET/private/Transports/SSL/FakeSocket.py,v 1.2 2007/05/10 18:44:58 acasajus Exp $
__RCSID__ = "$Id: FakeSocket.py,v 1.2 2007/05/10 18:44:58 acasajus Exp $"

import socket

##############################################################
#
#   Class to wrap Fake socket as if it was a real one
#
##############################################################

class FakeSocket:

  def __getattr__(self, name):
    return getattr( self.sock, name )

  def __init__(self, sock):
    self.iCopies = 0
    self.sock = sock

  def close(self):
    if self.iCopies == 0:
      self.sock.shutdown()
      self.sock.close()
    else:
      self.iCopies -= 1

  def makefile(self, mode, bufsize=None):
    self.iCopies += 1
    return socket._fileobject( self.sock, mode, bufsize)
