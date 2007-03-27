# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/DISET/private/FileHelper.py,v 1.1 2007/03/27 10:56:47 acasajus Exp $
__RCSID__ = "$Id: FileHelper.py,v 1.1 2007/03/27 10:56:47 acasajus Exp $"

import os
import md5
import types
from DIRAC.Core.Utilities.ReturnValues import S_OK, S_ERROR
from DIRAC.LoggingSystem.Client.Logger import gLogger

gLogger = gLogger.getSubLogger( "FileTransmissionHelper" )

class FileHelper:

  def __init__( self, oTransport ):
    self.oTransport = oTransport
    self.oMD5 = md5.new()
    self.bFinishedTransmission = False
    self.bReceivedEOF = False
    
  def sendData( self, sBuffer ):
    self.oMD5.update( sBuffer )
    self.oTransport.sendData( ( True, sBuffer ) )
    dRetVal = self.oTransport.receiveData()
    return dRetVal
    
  def sendEOF( self ):
    self.oTransport.sendData( ( False, self.oMD5.hexdigest() ) )
    self.bFinishedTransmission = True
    
  def receiveData( self ):
    stBuffer = self.oTransport.receiveData()
    if stBuffer[0]:
      self.oMD5.update( stBuffer[1] )
      self.oTransport.sendData( S_OK() )
    else:
      self.bReceivedEOF = True
      if not self.oMD5.hexdigest() == stBuffer[1]:
        self.bErrorInMD5 = True
      self.bFinishedTransmission = True
    return stBuffer[1]

  def receivedEOF( self ):
    return self.bReceivedEOF
  
  def finishedTransmission( self ):
    return self.bFinishedTransmission
  
  def errorInTransmission( self ):
    return self.bErrorInMD5

  def networkToFD( self, iFD ):
    self.oMD5 = md5.new()
    self.bReceivedEOF = False
    self.bErrorInMD5 = False
    try:
      sBuffer = self.receiveData()
      while not self.receivedEOF():
        os.write( iFD, sBuffer )
        stBuffer = self.receiveData()
    except Exception, e:
      gLogger.exception()
      return S_ERROR( "Error while receiving file, %s" % str( e ) )
    if self.errorInTransmission():
      return S_ERROR( "Error in the file CRC" ) 
    return S_OK()
      
  
  def FDToNetwork( self, iFD ):
    self.oMD5 = md5.new()
    iPacketSize = 8192
    try:
      sBuffer = os.read( iFD, iPacketSize )
      while len( sBuffer ) > 0:
        dRetVal = self.sendData( sBuffer )
        if not dRetVal[ 'OK' ]:
          return dRetVal
        sBuffer = os.read( iFD, iPacketSize )
      self.sendEOF()
    except Exception, e:
      gLogger.exception()
      return S_ERROR( "Error while sending file: %s" % str( e ) )
    return S_OK()
  
  def getFileDescriptor( self, uFile, sFileMode ):
    if type( uFile ) == types.StringType:
      try:
        self.oFile = file( uFile, sFileMode )
      except IOError:
        return S_ERROR( "%s can't be opened" % uFile )
      iFD = self.oFile.fileno()
    elif type( uFile ) == types.FileType:
      iFD = uFile.fileno()
    elif type( uFile ) == types.IntType:
      iFD = uFile
    else:
      return S_ERROR( "%s is not a valid file." % uFile )
    return S_OK( iFD ) 