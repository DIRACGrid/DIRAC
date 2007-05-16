# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/DISET/TransferClient.py,v 1.3 2007/05/16 15:58:46 acasajus Exp $
__RCSID__ = "$Id: TransferClient.py,v 1.3 2007/05/16 15:58:46 acasajus Exp $"

from DIRAC.Core.DISET.private.BaseClient import BaseClient
from DIRAC.Core.DISET.private.FileHelper import FileHelper
from DIRAC.Core.Utilities.ReturnValues import S_OK, S_ERROR

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
    retVal = self._proposeAction( "FFC" )
    if not retVal[ 'OK' ]:
      return retVal
    self.transport.sendData( fileId )
    retVal = self.transport.receiveData()
    if not retVal[ 'OK' ]:
      return retVal
    return fileHelper.FDToNetwork( fd )

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
    retVal = self._proposeAction( "FTC" )
    if not retVal[ 'OK' ]:
      return retVal
    self.transport.sendData( fileId )
    retVal = self.transport.receiveData()
    if not retVal[ 'OK' ]:
      return retVal
    return fileHelper.networkToFD( fd )



