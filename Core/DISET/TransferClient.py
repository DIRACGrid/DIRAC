# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/DISET/TransferClient.py,v 1.12 2007/11/20 15:51:44 acasajus Exp $
__RCSID__ = "$Id: TransferClient.py,v 1.12 2007/11/20 15:51:44 acasajus Exp $"

import tarfile
import threading
import os
from DIRAC.Core.DISET.private.BaseClient import BaseClient
from DIRAC.Core.DISET.private.FileHelper import FileHelper
from DIRAC.Core.Utilities.ReturnValues import S_OK, S_ERROR
from DIRAC.Core.Utilities import File

class TransferClient( BaseClient ):

  def __sendTransferHeader( self, actionName, fileInfo ):
    """
    Send the header of the transfer

    @type action: string
    @param action: Action to execute
    @type fileInfo: tuple
    @param fileInfo: Information of the target file/bulk
    @return: S_OK/S_ERROR
    """
    retVal = self._connect()
    if not retVal[ 'OK' ]:
      return retVal
    #FFC -> File from Client
    retVal = self._proposeAction( ( "FileTransfer", actionName ) )
    if not retVal[ 'OK' ]:
      return retVal
    self.transport.sendData( S_OK( fileInfo ) )
    retVal = self.transport.receiveData()
    if not retVal[ 'OK' ]:
      return retVal
    return S_OK()

  def sendFile( self, filename, fileId, token = "" ):
    """
    Send a file to server

    @type filename : string / file descriptor / file object
    @param filename : File to send to server
    @type fileId : any
    @param fileId : Identification of the file being sent
    @type token : string
    @param token : Optional token for the file
    @return : S_OK/S_ERROR
    """
    fileHelper = FileHelper()
    retVal = fileHelper.getFileDescriptor( filename, "r" )
    if not retVal[ 'OK' ]:
      return retVal
    fd = retVal[ 'Value' ]
    retVal = self.__sendTransferHeader( "FromClient", ( fileId, token, File.getSize( filename ) ) )
    if not retVal[ 'OK' ]:
      return retVal
    fileHelper.setTransport( self.transport )
    retVal = fileHelper.FDToNetwork( fd )
    if not retVal[ 'OK' ]:
      return retVal
    retVal = self.transport.receiveData()
    self.transport.close()
    return retVal

  def receiveFile( self, filename, fileId, token = ""):
    """
    Receive a file from the server

    @type filename : string / file descriptor / file object
    @param filename : File to receive from server
    @type fileId : any
    @param fileId : Identification of the file being received
    @type token : string
    @param token : Optional token for the file
    @return : S_OK/S_ERROR
    """
    fileHelper = FileHelper()
    retVal = fileHelper.getFileDescriptor( filename, "w" )
    if not retVal[ 'OK' ]:
      return retVal
    fd = retVal[ 'Value' ]
    retVal = self.__sendTransferHeader( "ToClient", ( fileId, token ) )
    if not retVal[ 'OK' ]:
      return retVal
    fileHelper.setTransport( self.transport )
    retVal = fileHelper.networkToFD( fd )
    if not retVal[ 'OK' ]:
      return retVal
    retVal = self.transport.receiveData()
    self.transport.close()
    return retVal

  def __checkFileList( self, fileList ):
    bogusEntries = []
    for entry in fileList:
      if not os.path.exists( entry ):
        bogusEntries.append( entry )
    return bogusEntries

  def sendBulk( self, fileList, bulkId, token = "", compress = True, bulkSize = -1 ):
    """
    Send a bulk of files to server

    @type fileList : list of ( string / file descriptor / file object )
    @param fileList : Files to send to server
    @type bulkId : any
    @param bulkId : Identification of the files being sent
    @type token : string
    @param token : Token for the bulk
    @type compress : boolean
    @param compress : Enable compression for the bulk. By default its True
    @type bulkSize : integer
    @param bulkSize : Optional size of the bulk
    @return : S_OK/S_ERROR
    """
    bogusEntries = self.__checkFileList( fileList )
    if bogusEntries:
      return S_ERROR( "Some files or directories don't exist :\n\t%s" % "\n\t".join( bogusEntries ) )
    if compress:
      bulkId = "%s.tar.bz2" % bulkId
    else:
      bulkId = "%s.tar" % bulkId
    retVal = self.__sendTransferHeader( "BulkFromClient", ( bulkId, token, bulkSize ) )
    if not retVal[ 'OK' ]:
      return retVal
    fileHelper = FileHelper( self.transport )
    retVal = fileHelper.bulkToNetwork( fileList, compress )
    if not retVal[ 'OK' ]:
      return retVal
    retVal = self.transport.receiveData()
    self.transport.close()
    return retVal

  def receiveBulk( self, destDir, bulkId, token = "", compress = True ):
    """
    Receive a bulk of files from server

    @type fileList : list of ( string / file descriptor / file object )
    @param fileList : Files to receive from server
    @type bulkId : any
    @param bulkId : Identification of the files being received
    @type token : string
    @param token : Token for the bulk
    @type compress : boolean
    @param compress : Enable compression for the bulk. By default its True
    @return : S_OK/S_ERROR
    """
    if not os.path.isdir( destDir ):
      return S_ERROR( "%s is not a directory for bulk receival" % destDir )
    if compress:
      bulkId = "%s.tar.bz2" % bulkId
    else:
      bulkId = "%s.tar" % bulkId
    retVal = self.__sendTransferHeader( "BulkToClient", ( bulkId, token ) )
    if not retVal[ 'OK' ]:
      return retVal
    fileHelper = FileHelper( self.transport )
    retVal = fileHelper.networkToBulk( destDir, compress )
    if not retVal[ 'OK' ]:
      return retVal
    retVal = self.transport.receiveData()
    self.transport.close()
    return retVal

  def listBulk( self, bulkId, token = "", compress = True ):
    """
    List the contents of a bulk

    @type bulkId : any
    @param bulkId : Identification of the bulk to list
    @type token : string
    @param token : Token for the bulk
    @type compress : boolean
    @param compress : Enable compression for the bulk. By default its True
    @return : S_OK/S_ERROR
    """
    if compress:
      bulkId = "%s.tar.bz2" % bulkId
    else:
      bulkId = "%s.tar" % bulkId
    retVal = self.__sendTransferHeader( "ListBulk", ( bulkId, token ) )
    if not retVal[ 'OK' ]:
      return retVal
    response = self.transport.receiveData( 1048576 )
    self.transport.close()
    return response