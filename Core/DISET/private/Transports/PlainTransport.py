# $HeadURL$
__RCSID__ = "$Id$"

import socket
import select
import time
import os
from DIRAC.Core.DISET.private.Transports.BaseTransport import BaseTransport
from DIRAC.FrameworkSystem.Client.Logger import gLogger
from DIRAC.Core.Utilities.ReturnValues import S_ERROR, S_OK

class PlainTransport( BaseTransport ):

  def initAsClient( self ):
    self.oSocket = socket.socket( socket.AF_INET, socket.SOCK_STREAM )
    if 'timeout' in self.extraArgsDict:
      self.oSocket.settimeout( self.extraArgsDict[ 'timeout' ] )
    try:
      self.oSocket.connect( self.stServerAddress )
    except socket.error , e:
      if e.args[0] != 115:
        return S_ERROR( "Can't connect: %s" % str( e ) )
      #Connect in progress
      oL = select.select( [], [ self.oSocket ], [], self.extraArgsDict[ 'timeout' ] )[1]
      if len( oL ) == 0:
        self.oSocket.close()
        return S_ERROR( "Connection timeout" )
      errno = self.oSocket.getsockopt( socket.SOL_SOCKET, socket.SO_ERROR )
      if errno != 0:
        return S_ERROR( "Can't connect: %s" % str( ( errno, os.strerror( errno ) ) ) )
    self.remoteAddress = self.oSocket.getpeername()
    return S_OK( self.oSocket )

  def initAsServer( self ):
    if not self.serverMode():
      raise RuntimeError( "Must be initialized as server mode" )
    self.oSocket = socket.socket( socket.AF_INET, socket.SOCK_STREAM )
    if self.bAllowReuseAddress:
      self.oSocket.setsockopt( socket.SOL_SOCKET, socket.SO_REUSEADDR, 1 )
    self.oSocket.bind( self.stServerAddress )
    self.oSocket.listen( self.iListenQueueSize )
    return S_OK( self.oSocket )

  def close( self ):
    gLogger.debug( "Closing socket" )
    try:
      self.oSocket.shutdown( socket.SHUT_RDWR )
    except:
      pass
    self.oSocket.close()

  def setClientSocket( self, oSocket ):
    if self.serverMode():
      raise RuntimeError( "Mustbe initialized as client mode" )
    self.oSocket = oSocket
    if 'timeout' in self.extraArgsDict:
      self.oSocket.settimeout( self.extraArgsDict[ 'timeout' ] )
    self.remoteAddress = self.oSocket.getpeername()

  def acceptConnection( self ):
    #HACK: Was = PlainTransport( self )
    oClientTransport = PlainTransport( self.stServerAddress )
    oClientSocket, stClientAddress = self.oSocket.accept()
    oClientTransport.setClientSocket( oClientSocket )
    return S_OK( oClientTransport )

  def _read( self, bufSize = 4096, skipReadyCheck = False ):
    start = time.time()
    timeout = False
    if 'timeout' in self.extraArgsDict:
      timeout = self.extraArgsDict[ 'timeout' ]
    while True:
      if timeout:
        if time.time() - start > timeout:
          return S_ERROR( "Socket read timeout exceeded" )
      try:
        data = self.oSocket.recv( bufSize )
        return S_OK( data )
      except socket.error, e:
        if e[0] == 11:
          time.sleep( 0.001 )
        else:
          return S_ERROR( "Exception while reading from peer: %s" % str( e ) )
      except Exception, e:
        return S_ERROR( "Exception while reading from peer: %s" % str( e ) )

  def _write( self, buffer ):
    sentBytes = 0
    timeout = False
    if 'timeout' in self.extraArgsDict:
      timeout = self.extraArgsDict[ 'timeout' ]
    if timeout:
      start = time.time()
    while sentBytes < len( buffer ):
      try:
        if timeout:
          if time.time() - start > timeout:
            return S_ERROR( "Socket write timeout exceeded" )
        sent = self.oSocket.send( buffer[ sentBytes: ] )
        if sent == 0:
          return S_ERROR( "Connection closed by peer" )
        if sent > 0:
          sentBytes += sent
      except socket.error, e:
        if e[0] == 11:
          time.sleep( 0.001 )
        else:
          return S_ERROR( "Exception while sending to peer: %s" % str( e ) )
      except Exception, e:
        return S_ERROR( "Error while sending: %s" % str( e ) )
    return S_OK( sentBytes )

def checkSanity( *args, **kwargs ):
  return S_OK()

def delegate( delegationRequest, kwargs ):
  """
  Check delegate!
  """
  return S_OK()
