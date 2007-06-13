# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/DISET/private/FileHelper.py,v 1.7 2007/06/13 19:29:39 acasajus Exp $
__RCSID__ = "$Id: FileHelper.py,v 1.7 2007/06/13 19:29:39 acasajus Exp $"

import os
import md5
import types
import cStringIO
from DIRAC.Core.Utilities.ReturnValues import S_OK, S_ERROR
from DIRAC.LoggingSystem.Client.Logger import gLogger

gLogger = gLogger.getSubLogger( "FileTransmissionHelper" )

class FileHelper:

  def __init__( self, oTransport ):
    self.oTransport = oTransport
    self.oMD5 = md5.new()
    self.bFinishedTransmission = False
    self.bReceivedEOF = False
    self.packetSize = 1048576

  def sendData( self, sBuffer ):
    self.oMD5.update( sBuffer )
    self.oTransport.sendData( S_OK( ( True, sBuffer ) ) )
    dRetVal = self.oTransport.receiveData()
    return dRetVal

  def sendEOF( self ):
    self.oTransport.sendData( S_OK( ( False, self.oMD5.hexdigest() ) ) )
    self.bFinishedTransmission = True

  def receiveData( self ):
    retVal = self.oTransport.receiveData()
    if not retVal[ 'OK' ]:
      raise RuntimeException( retVal[ 'Message' ] )
    stBuffer = retVal[ 'Value' ]
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

  def networkToString( self ):
    """ Receive the input from a DISET client and return it as a string
    """

    stringIO = cStringIO.StringIO()

    self.oMD5 = md5.new()
    self.bReceivedEOF = False
    self.bErrorInMD5 = False

    try:
      strBuffer = self.receiveData()
      if self.receivedEOF():
        stringIO.write( strBuffer )
      else:
        while not self.receivedEOF():
          stringIO.write( strBuffer )
          strBuffer = self.receiveData()
    except Exception, e:
      gLogger.exception()
      return S_ERROR( "Error while receiving file, %s" % str( e ) )
    if self.errorInTransmission():
      return S_ERROR( "Error in the file CRC" )

    strValue = stringIO.getvalue()
    return S_OK( strValue  )

  def networkToFD( self, iFD ):
    self.oMD5 = md5.new()
    self.bReceivedEOF = False
    self.bErrorInMD5 = False
    try:
      strBuffer = self.receiveData()
      while not self.receivedEOF():
        os.write( iFD, strBuffer )
        strBuffer = self.receiveData()
    except Exception, e:
      gLogger.exception()
      return S_ERROR( "Error while receiving file, %s" % str( e ) )
    if self.errorInTransmission():
      return S_ERROR( "Error in the file CRC" )
    return S_OK()

  def stringToNetwork( self, stringVal ):
    """ Send a given string to the DISET client over the network
    """

    stringIO = cStringIO.StringIO( stringVal )

    iPacketSize = self.packetSize
    ioffset = 0
    strlen = len(stringVal)
    try:
      while (ioffset) < strlen:
        if (ioffset+iPacketSize) < strlen:
          result = self.sendData( stringVal[ioffset:ioffset+iPacketSize] )
        else:
          result = self.sendData( stringVal[ioffset:strlen] )
        if not result['OK']:
          return result
        ioffset += iPacketSize
      self.sendEOF()
    except Exception, e:
      gLogger.exception()
      return S_ERROR( "Error while sending string: %s" % str( e ) )
    stringIO.close()
    return S_OK()

  def FDToNetwork( self, iFD ):
    self.oMD5 = md5.new()
    iPacketSize = self.packetSize
    try:
      sBuffer = os.read( iFD, iPacketSize )
      while len( sBuffer ) > 0:
        dRetVal = self.sendData( sBuffer )
        if not dRetVal[ 'OK' ]:
          return dRetVal
        sBuffer = os.read( iFD, iPacketSize )
      self.sendEOF()
    except Exception, e:
      gLogger.exception( "Error while sending file" )
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
