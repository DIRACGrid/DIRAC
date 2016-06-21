# $HeadURL$
__RCSID__ = "$Id$"

import suds
import suds.client
import suds.transport
import urllib2
from DIRAC.Core.DISET.HTTPDISETConnection import HTTPDISETConnection

class DISETHandler( urllib2.HTTPSHandler ):
  
  def https_open(self, req):
    return self.do_open( HTTPDISETConnection, req)
    
class DISETHttpTransport( suds.transport.http.HttpTransport ):
  
  def __init__( self, **kwargs ):
    suds.transport.http.HttpTransport.__init__( self, **kwargs )
    self.handler = DISETHandler()
    self.urlopener = urllib2.build_opener( self.handler )

    
def getSOAPClient( wsdlLocation, **kwargs ):
  kwargs[ 'transport' ] = DISETHttpTransport()
  return suds.client.Client( wsdlLocation, **kwargs )
    
