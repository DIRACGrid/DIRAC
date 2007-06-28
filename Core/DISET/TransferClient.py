# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/DISET/TransferClient.py,v 1.8 2007/06/28 09:48:33 acasajus Exp $
__RCSID__ = "$Id: TransferClient.py,v 1.8 2007/06/28 09:48:33 acasajus Exp $"

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

  def sendFile( self, filename, fileId ):
    fileHelper = FileHelper()
    retVal = fileHelper.getFileDescriptor( filename, "r" )
    if not retVal[ 'OK' ]:
      return retVal
    fd = retVal[ 'Value' ]
    retVal = self.__sendTransferHeader( "FromClient", ( fileId, File.getSize( filename ) ) )
    if not retVal[ 'OK' ]:
      return retVal
    fileHelper.setTransport( self.transport )
    response = fileHelper.FDToNetwork( fd )
    self.transport.close()
    return response

  def receiveFile( self, filename, fileId ):
    fileHelper = FileHelper()
    retVal = fileHelper.getFileDescriptor( filename, "w" )
    if not retVal[ 'OK' ]:
      return retVal
    fd = retVal[ 'Value' ]
    retVal = self.__sendTransferHeader( "ToClient", ( fileId, ) )
    if not retVal[ 'OK' ]:
      return retVal
    fileHelper.setTransport( self.transport )
    response = fileHelper.networkToFD( fd )
    self.transport.close()
    return response

  def __checkFileList( self, fileList ):
    bogusEntries = []
    for entry in fileList:
      if not os.path.exists( entry ):
        bogusEntries.append( entry )
    return bogusEntries

  def sendBulk( self, fileList, bulkId, compress = True ):
    bogusEntries = self.__checkFileList( fileList )
    if bogusEntries:
      return S_ERROR( "Some files or directories don't exist :\n\t%s" % "\n\t".join( bogusEntries ) )
    if compress:
      bulkId = "%s.tar.bz2" % bulkId
    else:
      bulkId = "%s.tar" % bulkId
    retVal = self.__sendTransferHeader( "BulkFromClient", ( bulkId, ) )
    if not retVal[ 'OK' ]:
      return retVal
    fileHelper = FileHelper( self.transport )
    response = fileHelper.bulkToNetwork( fileList, compress )
    self.transport.close()
    return response

  def receiveBulk( self, destDir, bulkId, compress = True ):
    if not os.path.isdir( destDir ):
      return S_ERROR( "%s is not a directory for bulk receival" % destDir )
    if compress:
      bulkId = "%s.tar.bz2" % bulkId
    else:
      bulkId = "%s.tar" % bulkId
    retVal = self.__sendTransferHeader( "BulkToClient", ( bulkId, ) )
    if not retVal[ 'OK' ]:
      return retVal
    fileHelper = FileHelper( self.transport )
    response = fileHelper.networkToBulk( destDir, compress )
    self.transport.close()
    return response

  def listBulk( self, bulkId, compress = True ):
    if compress:
      bulkId = "%s.tar.bz2" % bulkId
    else:
      bulkId = "%s.tar" % bulkId
    retVal = self.__sendTransferHeader( "ListBulk", ( bulkId, ) )
    if not retVal[ 'OK' ]:
      return retVal
    response = self.transport.receiveData( 1048576 )
    self.transport.close()
    return response