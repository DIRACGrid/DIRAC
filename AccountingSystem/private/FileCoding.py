# $HeadURL$
__RCSID__ = "$Id$"

import base64
from DIRAC import S_OK, S_ERROR, gLogger

try:
  import zlib
  zCompressionEnabled = True
except:
  zCompressionEnabled = False
  
def codeRequestInFileId( plotRequest, compressIfPossible = True ):
  compress = compressIfPossible and zCompressionEnabled
  if compress:
    plotStub = "Z:%s" % base64.urlsafe_b64encode( zlib.compress( DEncode.encode( plotRequest ), 9 ) )
  elif not self.__forceRawEncoding:
    plotStub = "S:%s" % base64.urlsafe_b64encode( DEncode.encode( plotRequest ) )
  else:
    plotStub = "R:%s" % DEncode.encode( plotRequest )
  thbStub = False
  if 'thumbnail' in extraArgs and extraArgs[ 'thumbnail' ]:
    thbStub = plotStub
    extraArgs[ 'thumbnail' ] = False
    if compress:
      plotStub = "Z:%s" % base64.urlsafe_b64encode( zlib.compress( DEncode.encode( plotRequest ), 9 ) )
    elif not self.__forceRawEncoding:
      plotStub = "S:%s" % base64.urlsafe_b64encode( DEncode.encode( plotRequest ) )
    else:
      plotStub = "R:%s" % DEncode.encode( plotRequest )
  return S_OK( { 'plot' : plotStub, 'thumbnail' : thbStub } )

def extractRequestFromFileId( self, fileId ):
  stub = fileId[2:]
  type = fileId[0]
  if type == 'Z':
    gLogger.info( "Compressed request, uncompressing" )
    try:
      stub = base64.urlsafe_b64decode( stub )
    except Exception, e:
      gLogger.error( "Oops! Plot request is not properly encoded!", str( e ) )
      return S_ERROR( "Oops! Plot request is not properly encoded!: %s" % str( e ) )
    try:
      stub = zlib.decompress( stub )
    except Exception, e:
      gLogger.error( "Oops! Plot request is invalid!", str( e ) )
      return S_ERROR( "Oops! Plot request is invalid!: %s" % str( e ) )
  elif type == 'S':
    gLogger.info( "Base64 request, decoding" )
    try:
      stub = base64.urlsafe_b64decode( stub )
    except Exception, e:
      gLogger.error( "Oops! Plot request is not properly encoded!", str( e ) )
      return S_ERROR( "Oops! Plot request is not properly encoded!: %s" % str( e ) )
  elif type == 'R':
    #Do nothing, it's already uncompressed
    pass
  else:
    gLogger.error( "Oops! Stub type '%s' is unknown :P" % type )
    return S_ERROR( "Oops! Stub type '%s' is unknown :P" % type )
  plotRequest, stubLength = DEncode.decode( stub )
  if len( stub ) != stubLength:
    gLogger.error( "Oops! The stub is longer than the data :P" )
    return S_ERROR( "Oops! The stub is longer than the data :P" )
  return S_OK( plotRequest )