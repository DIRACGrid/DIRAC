# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/DISET/private/FileHelper.py,v 1.10 2007/06/28 09:48:33 acasajus Exp $
__RCSID__ = "$Id: FileHelper.py,v 1.10 2007/06/28 09:48:33 acasajus Exp $"

import os
import md5
import types
import threading
import cStringIO
import tarfile
from DIRAC.Core.Utilities.ReturnValues import S_OK, S_ERROR
from DIRAC.LoggingSystem.Client.Logger import gLogger

gLogger = gLogger.getSubLogger( "FileTransmissionHelper" )

class FileHelper:

  def __init__( self, oTransport = None ):
    self.oTransport = oTransport
    self.oMD5 = md5.new()
    self.bFinishedTransmission = False
    self.bReceivedEOF = False
    self.packetSize = 1048576

  def setTransport( self, oTransport ):
    self.oTransport = oTransport

  def sendData( self, sBuffer ):
    self.oMD5.update( sBuffer )
    self.oTransport.sendData( S_OK( ( True, sBuffer ) ) )
    dRetVal = self.oTransport.receiveData()
    return dRetVal

  def sendEOF( self ):
    self.oTransport.sendData( S_OK( ( False, self.oMD5.hexdigest() ) ) )
    self.markAsTransferred()

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
      self.markAsTransferred()
    return stBuffer[1]

  def receivedEOF( self ):
    return self.bReceivedEOF

  def markAsTransferred( self ):
    self.bFinishedTransmission = True

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

  def __createTar( self, fileList, wPipe, compress ):
    filePipe = os.fdopen( wPipe, "w" )
    tarMode = "w|"
    if compress:
      tarMode = "w|bz2"
    tar = tarfile.open( mode = tarMode, fileobj = filePipe )
    for entry in fileList:
      tar.add( os.path.realpath( entry ), os.path.basename( entry ), recursive = True )
    tar.close()
    filePipe.close()

  def bulkToNetwork( self, fileList, compress = True ):
    rPipe, wPipe = os.pipe()
    thrd = threading.Thread( target = self.__createTar, args = ( fileList, wPipe, compress ) )
    thrd.start()
    response = self.FDToNetwork( rPipe )
    os.close( rPipe )
    return response

  def __extractTar( self, destDir, rPipe, compress ):
    filePipe = os.fdopen( rPipe, "r" )
    tarMode = "r|"
    if compress:
      tarMode = "r|bz2"
    tar = tarfile.open( mode = tarMode, fileobj = filePipe )
    for tarInfo in tar:
      tar.extract( tarInfo, destDir )
    tar.close()
    filePipe.close()

  def __receiveToPipe( self, wPipe, retList ):
    retList.append( self.networkToFD( wPipe ) )
    os.close( wPipe )

  def networkToBulk( self, destDir, compress = True ):
    retList = []
    rPipe, wPipe = os.pipe()
    thrd = threading.Thread( target = self.__receiveToPipe, args = ( wPipe, retList) )
    thrd.start()
    try:
      self.__extractTar( destDir, rPipe, compress )
    except Exception, e:
      return S_ERROR( "Error while extracting bulk: %s" % e)
    return retList[0]

  def bulkListToNetwork( self, iFD, compress = True ):
    filePipe = os.fdopen( iFD, "r" )
    try:
      tarMode = "r|"
      if compress:
        tarMode = "r|bz2"
      entries = []
      tar = tarfile.open( mode = tarMode, fileobj = filePipe )
      for tarInfo in tar:
        entries.append( tarInfo.name )
      tar.close()
      filePipe.close()
      self.oTransport.sendData( S_OK( entries ) )
    except tarfile.ReadError, v:
      self.oTransport.sendData( S_ERROR( "Error reading bulk: %s" % str( v ) ) )
    except tarfile.CompressionError, v:
      self.oTransport.sendData( S_ERROR( "Error in bulk compression setting: %s" % str( v ) ) )
    except Exception, v:
      self.oTransport.sendData( S_ERROR( "Error in listing bulk: %s" % str( v )  ) )

