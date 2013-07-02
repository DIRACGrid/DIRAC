# $HeadURL$
__RCSID__ = "$Id$"

import os
try:
  import hashlib
  md5 = hashlib
except:
  import md5
import types
import threading
import cStringIO
import tarfile
import tempfile
from DIRAC.Core.Utilities.ReturnValues import S_OK, S_ERROR
from DIRAC.FrameworkSystem.Client.Logger import gLogger

gLogger = gLogger.getSubLogger( "FileTransmissionHelper" )

class FileHelper:

  __validDirections = ( "toClient", "fromClient", 'receive', 'send' )
  __directionsMapping = { 'toClient' : 'send', 'fromClient' : 'receive' }

  def __init__( self, oTransport = None, checkSum = True ):
    self.oTransport = oTransport
    self.__checkMD5 = checkSum
    self.__oMD5 = md5.md5()
    self.bFinishedTransmission = False
    self.bReceivedEOF = False
    self.direction = False
    self.packetSize = 1048576
    self.__fileBytes = 0
    self.__log = gLogger.getSubLogger( "FileHelper" )

  def disableCheckSum( self ):
    self.__checkMD5 = False
    
  def enableCheckSum( self ):
    self.__checkMD5 = True  

  def setTransport( self, oTransport ):
    self.oTransport = oTransport

  def setDirection( self, direction ):
    if direction in FileHelper.__validDirections:
      if direction in FileHelper.__directionsMapping:
        self.direction = FileHelper.__directionsMapping[ direction ]
      else:
        self.direction = direction

  def getHash( self ):
    return self.__oMD5.hexdigest()

  def getTransferedBytes( self ):
    return self.__fileBytes

  def sendData( self, sBuffer ):
    if self.__checkMD5:
      self.__oMD5.update( sBuffer )
    retVal = self.oTransport.sendData( S_OK( ( True, sBuffer ) ) )
    if not retVal[ 'OK' ]:
      return retVal
    retVal = self.oTransport.receiveData()
    return retVal

  def sendEOF( self ):
    retVal = self.oTransport.sendData( S_OK( ( False, self.__oMD5.hexdigest() ) ) )
    if not retVal[ 'OK' ]:
      return retVal
    self.__finishedTransmission()
    return S_OK()

  def sendError( self, errorMsg ):
    retVal = self.oTransport.sendData( S_ERROR( errorMsg ) )
    if not retVal[ 'OK' ]:
      return retVal
    self.__finishedTransmission()
    return S_OK()

  def receiveData( self, maxBufferSize = 0 ):
    retVal = self.oTransport.receiveData( maxBufferSize = maxBufferSize )
    if 'AbortTransfer' in retVal and retVal[ 'AbortTransfer' ]:
      self.oTransport.sendData( S_OK() )
      self.__finishedTransmission()
      self.bReceivedEOF = True
      return S_OK( '' )
    if not retVal[ 'OK' ]:
      return retVal
    stBuffer = retVal[ 'Value' ]
    if stBuffer[0]:
      if self.__checkMD5:
        self.__oMD5.update( stBuffer[1] )
      self.oTransport.sendData( S_OK() )
    else:
      self.bReceivedEOF = True
      if self.__checkMD5 and not self.__oMD5.hexdigest() == stBuffer[1]:
        self.bErrorInMD5 = True
      self.__finishedTransmission()
      return S_OK( "" )
    return S_OK( stBuffer[1] )

  def receivedEOF( self ):
    return self.bReceivedEOF

  def markAsTransferred( self ):
    if not self.bFinishedTransmission:
      if self.direction == "receive":
        self.oTransport.receiveData()
        abortTrans = S_OK()
        abortTrans[ 'AbortTransfer' ] = True
        self.oTransport.sendData( abortTrans )
      else:
        abortTrans = S_OK( ( False, "" ) )
        abortTrans[ 'AbortTransfer' ] = True
        retVal = self.oTransport.sendData( abortTrans )
        if not retVal[ 'OK' ]:
          return retVal
        self.oTransport.receiveData()
    self.__finishedTransmission()

  def __finishedTransmission( self ):
    self.bFinishedTransmission = True

  def finishedTransmission( self ):
    return self.bFinishedTransmission

  def errorInTransmission( self ):
    return self.bErrorInMD5

  def networkToString( self, maxFileSize = 0 ):
    """ Receive the input from a DISET client and return it as a string
    """

    stringIO = cStringIO.StringIO()
    result = self.networkToDataSink( stringIO, maxFileSize = maxFileSize )
    if not result[ 'OK' ]:
      return result
    return S_OK( stringIO.getvalue() )

  def networkToFD( self, iFD, maxFileSize = 0 ):
    dataSink = os.fdopen( iFD, "w" )
    try:
      return self.networkToDataSink( dataSink, maxFileSize = maxFileSize )
    finally:
      try:
        dataSink.close()
      except Exception, e:
        pass

  def networkToDataSink( self, dataSink, maxFileSize = 0 ):
    if "write" not in dir( dataSink ):
      return S_ERROR( "%s data sink object does not have a write method" % str( dataSink ) )
    self.__oMD5 = md5.md5()
    self.bReceivedEOF = False
    self.bErrorInMD5 = False
    receivedBytes = 0
    try:
      result = self.receiveData( maxBufferSize = maxFileSize )
      if not result[ 'OK' ]:
        return result
      strBuffer = result[ 'Value' ]
      receivedBytes += len( strBuffer )
      while not self.receivedEOF():
        if maxFileSize > 0 and receivedBytes > maxFileSize:
          self.sendError( "Exceeded maximum file size" )
          return S_ERROR( "Received file exceeded maximum size of %s bytes" % ( maxFileSize ) )
        dataSink.write( strBuffer )
        result = self.receiveData( maxBufferSize = ( maxFileSize - len( strBuffer ) ) )
        if not result[ 'OK' ]:
          return result
        strBuffer = result[ 'Value' ]
        receivedBytes += len( strBuffer )
      if strBuffer:
        dataSink.write( strBuffer )
    except Exception, e:
      return S_ERROR( "Error while receiving file, %s" % str( e ) )
    if self.errorInTransmission():
      return S_ERROR( "Error in the file CRC" )
    self.__fileBytes = receivedBytes
    return S_OK()

  def stringToNetwork( self, stringVal ):
    """ Send a given string to the DISET client over the network
    """

    stringIO = cStringIO.StringIO( stringVal )

    iPacketSize = self.packetSize
    ioffset = 0
    strlen = len( stringVal )
    try:
      while ( ioffset ) < strlen:
        if ( ioffset + iPacketSize ) < strlen:
          result = self.sendData( stringVal[ioffset:ioffset + iPacketSize] )
        else:
          result = self.sendData( stringVal[ioffset:strlen] )
        if not result['OK']:
          return result
        if 'AbortTransfer' in result and result[ 'AbortTransfer' ]:
          self.__log.verbose( "Transfer aborted" )
          return S_OK()
        ioffset += iPacketSize
      self.sendEOF()
    except Exception, e:
      return S_ERROR( "Error while sending string: %s" % str( e ) )
    try:
      stringIO.close()
    except:
      pass
    return S_OK()

  def FDToNetwork( self, iFD ):
    self.__oMD5 = md5.md5()
    iPacketSize = self.packetSize
    self.__fileBytes = 0
    sentBytes = 0
    try:
      sBuffer = os.read( iFD, iPacketSize )
      while len( sBuffer ) > 0:
        dRetVal = self.sendData( sBuffer )
        if not dRetVal[ 'OK' ]:
          return dRetVal
        if 'AbortTransfer' in dRetVal and dRetVal[ 'AbortTransfer' ]:
          self.__log.verbose( "Transfer aborted" )
          return S_OK()
        sentBytes += len( sBuffer )
        sBuffer = os.read( iFD, iPacketSize )
      self.sendEOF()
    except Exception, e:
      gLogger.exception( "Error while sending file" )
      return S_ERROR( "Error while sending file: %s" % str( e ) )
    self.__fileBytes = sentBytes
    return S_OK()

  def BufferToNetwork( self, stringToSend ):
    sIO = cStringIO.StringIO( stringToSend )
    try:
      return self.DataSourceToNetwork( sIO )
    finally:
      sIO.close()

  def DataSourceToNetwork( self, dataSource ):
    if "read" not in dir( dataSource ):
      return S_ERROR( "%s data source object does not have a read method" % str( dataSource ) )
    self.__oMD5 = md5.md5()
    iPacketSize = self.packetSize
    try:
      sBuffer = dataSource.read( iPacketSize )
      while len( sBuffer ) > 0:
        dRetVal = self.sendData( sBuffer )
        if not dRetVal[ 'OK' ]:
          return dRetVal
        if 'AbortTransfer' in dRetVal and dRetVal[ 'AbortTransfer' ]:
          self.__log.verbose( "Transfer aborted" )
          return S_OK()
        sBuffer = dataSource.read( iPacketSize )
      self.sendEOF()
    except Exception, e:
      gLogger.exception( "Error while sending file" )
      return S_ERROR( "Error while sending file: %s" % str( e ) )
    return S_OK()

  def getFileDescriptor( self, uFile, sFileMode ):
    closeAfter = True
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
      closeAfter = False
    else:
      return S_ERROR( "%s is not a valid file." % uFile )
    result = S_OK( iFD )
    result[ 'closeAfterUse' ] = closeAfter
    return result

  def getDataSink( self, uFile ):
    closeAfter = True
    if type( uFile ) == types.StringType:
      try:
        oFile = file( uFile, "wb" )
      except IOError:
        return S_ERROR( "%s can't be opened" % uFile )
    elif type( uFile ) == types.FileType:
      oFile = uFile
      closeAfter = False
    elif type( uFile ) == types.IntType:
      oFile = os.fdopen( uFile, "wb" )
      closeAfter = True
    elif "write" in dir( uFile ):
      oFile = uFile
      closeAfter = False
    else:
      return S_ERROR( "%s is not a valid file." % uFile )
    result = S_OK( oFile )
    result[ 'closeAfterUse' ] = closeAfter
    return result

  def __createTar( self, fileList, wPipe, compress, autoClose = True ):
    if 'write' in dir( wPipe ):
      filePipe = wPipe
    else:
      filePipe = os.fdopen( wPipe, "w" )
    tarMode = "w|"
    if compress:
      tarMode = "w|bz2"

    tar = tarfile.open( name = "Pipe", mode = tarMode, fileobj = filePipe )
    for entry in fileList:
      tar.add( os.path.realpath( entry ), os.path.basename( entry ), recursive = True )
    tar.close()
    if autoClose:
      try:
        filePipe.close()
      except:
        pass

  def bulkToNetwork( self, fileList, compress = True, onthefly = True ):
    if not onthefly:
      try:
        filePipe, filePath = tempfile.mkstemp()
      except Exception, e:
        return S_ERROR( "Can't create temporary file to pregenerate the bulk: %s" % str( e ) )
      self.__createTar( fileList, filePipe, compress )
      try:
        fo = file( filePath, 'rb' )
      except Exception, e:
        return S_ERROR( "Can't read pregenerated bulk: %s" % str( e ) )
      result = self.DataSourceToNetwork( fo )
      try:
        fo.close()
        os.unlink( filePath )
      except:
        pass
      return result
    else:
      rPipe, wPipe = os.pipe()
      thrd = threading.Thread( target = self.__createTar, args = ( fileList, wPipe, compress ) )
      thrd.start()
      response = self.FDToNetwork( rPipe )
      try:
        os.close( rPipe )
      except:
        pass
      return response

  def __extractTar( self, destDir, rPipe, compress ):
    filePipe = os.fdopen( rPipe, "r" )
    tarMode = "r|*"
    if compress:
      tarMode = "r|bz2"
    tar = tarfile.open( mode = tarMode, fileobj = filePipe )
    for tarInfo in tar:
      tar.extract( tarInfo, destDir )
    tar.close()
    try:
      filePipe.close()
    except:
      pass

  def __receiveToPipe( self, wPipe, retList, maxFileSize ):
    retList.append( self.networkToFD( wPipe, maxFileSize = maxFileSize ) )
    try:
      os.close( wPipe )
    except:
      pass

  def networkToBulk( self, destDir, compress = True, maxFileSize = 0 ):
    retList = []
    rPipe, wPipe = os.pipe()
    thrd = threading.Thread( target = self.__receiveToPipe, args = ( wPipe, retList, maxFileSize ) )
    thrd.start()
    try:
      self.__extractTar( destDir, rPipe, compress )
    except Exception, e:
      return S_ERROR( "Error while extracting bulk: %s" % e )
    thrd.join()
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
      return S_OK( entries )
    except tarfile.ReadError, v:
      return S_ERROR( "Error reading bulk: %s" % str( v ) )
    except tarfile.CompressionError, v:
      return S_ERROR( "Error in bulk compression setting: %s" % str( v ) )
    except Exception, v:
      return S_ERROR( "Error in listing bulk: %s" % str( v ) )

