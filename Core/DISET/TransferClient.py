# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/DISET/TransferClient.py,v 1.7 2007/06/27 18:22:09 acasajus Exp $
__RCSID__ = "$Id: TransferClient.py,v 1.7 2007/06/27 18:22:09 acasajus Exp $"

import tarfile
import threading
import os
from DIRAC.Core.DISET.private.BaseClient import BaseClient
from DIRAC.Core.DISET.private.FileHelper import FileHelper
from DIRAC.Core.Utilities.ReturnValues import S_OK, S_ERROR
from DIRAC.Core.Utilities import File

class TransferClient( BaseClient ):

  def sendFile( self, filename, fileId ):
    retVal = self._connect()
    if not retVal[ 'OK' ]:
      return retVal
    fileHelper = FileHelper( self.transport )
    retVal = fileHelper.getFileDescriptor( filename, "r" )
    if not retVal[ 'OK' ]:
      return retVal
    fd = retVal[ 'Value' ]
    #FFC -> File from Client
    retVal = self._proposeAction( ( "FileTransfer", "FromClient" ) )
    if not retVal[ 'OK' ]:
      return retVal
    self.transport.sendData( S_OK( ( fileId, File.getSize( filename ) ) ) )
    retVal = self.transport.receiveData()
    if not retVal[ 'OK' ]:
      return retVal
    response = fileHelper.FDToNetwork( fd )
    self.transport.close()
    return response

  def receiveFile( self, filename, fileId ):
    retVal = self._connect()
    if not retVal[ 'OK' ]:
      return retVal
    fileHelper = FileHelper( self.transport )
    retVal = fileHelper.getFileDescriptor( filename, "w" )
    if not retVal[ 'OK' ]:
      return retVal
    fd = retVal[ 'Value' ]
    #FTC -> File To Client
    retVal = self._proposeAction( ( "FileTransfer", "ToClient" ) )
    if not retVal[ 'OK' ]:
      return retVal
    self.transport.sendData( S_OK( ( fileId, ) ) )
    retVal = self.transport.receiveData()
    if not retVal[ 'OK' ]:
      return retVal
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
    retVal = self._connect()
    if not retVal[ 'OK' ]:
      return retVal
    bogusEntries = self.__checkFileList( fileList )
    if bogusEntries:
      return S_ERROR( "Some files or directories don't exist :\n\t%s" % "\n\t".join( bogusEntries ) )
    #FFC -> File from Client
    retVal = self._proposeAction( ( "FileTransfer", "BulkFromClient" ) )
    if not retVal[ 'OK' ]:
      return retVal
    if compress:
      bulkId = "%s.tar.bz2" % bulkId
    else:
      bulkId = "%s.tar" % bulkId
    self.transport.sendData( S_OK( ( bulkId, ) ) )
    retVal = self.transport.receiveData()
    if not retVal[ 'OK' ]:
      return retVal
    fileHelper = FileHelper( self.transport )
    response = fileHelper.tarToNetwork( fileList, compress )
    self.transport.close()
    return response

  def receiveBulk( self, destDir, bulkId, compress = True ):
    retVal = self._connect()
    if not retVal[ 'OK' ]:
      return retVal
    if not os.path.isdir( destDir ):
      return S_ERROR( "%s is not a directory for bulk receival" % destDir )
    #FFC -> File from Client
    retVal = self._proposeAction( ( "FileTransfer", "BulkToClient" ) )
    if not retVal[ 'OK' ]:
      return retVal
    if compress:
      bulkId = "%s.tar.bz2" % bulkId
    else:
      bulkId = "%s.tar" % bulkId
    self.transport.sendData( S_OK( ( bulkId, ) ) )
    retVal = self.transport.receiveData()
    if not retVal[ 'OK' ]:
      return retVal
    fileHelper = FileHelper( self.transport )
    response = fileHelper.networkToTar( destDir, compress )
    self.transport.close()
    return response