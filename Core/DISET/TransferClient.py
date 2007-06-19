# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/DISET/TransferClient.py,v 1.6 2007/06/19 13:29:54 acasajus Exp $
__RCSID__ = "$Id: TransferClient.py,v 1.6 2007/06/19 13:29:54 acasajus Exp $"

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



