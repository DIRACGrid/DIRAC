# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/DISET/private/Transports/SSLTransport.py,v 1.1 2007/03/09 15:27:45 rgracian Exp $
__RCSID__ = "$Id: SSLTransport.py,v 1.1 2007/03/09 15:27:45 rgracian Exp $"

#Initialize when importing it
from DIRAC.Core.DISET.TransportLayers.BaseTransport import BaseTransport

class SSLTransport( BaseTransport ):
    
  sTransportName = "SSL"
    
  def __init__( self, oSocket ):
    self.oSocket = oSocket
    BaseTransport.__init__( self, oSocket )
    
  def _write( self, sBuffer ):
    self.oSocket.send( sBuffer )
    
  def _read( self ):
    return self.oSocket.recv( 8192 )
  
  def getUserInfo( self ):
    return {}
    
