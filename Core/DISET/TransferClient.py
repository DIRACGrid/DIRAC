# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/DISET/TransferClient.py,v 1.11 2007/10/05 07:40:22 acasajus Exp $
__RCSID__ = "$Id: TransferClient.py,v 1.11 2007/10/05 07:40:22 acasajus Exp $"

import tarfile
import threading
import os
from DIRAC.Core.DISET.private.BaseClient import BaseClient
from DIRAC.Core.DISET.private.FileHelper import FileHelper
from DIRAC.Core.Utilities.ReturnValues import S_OK, S_ERROR
from DIRAC.Core.Utilities import File

class TransferClient( BaseClient ):

  def __sendTransferHeader( self, actionName, fileInfo ):
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