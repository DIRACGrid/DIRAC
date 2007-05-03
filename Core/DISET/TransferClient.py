# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/DISET/TransferClient.py,v 1.2 2007/05/03 18:59:47 acasajus Exp $
__RCSID__ = "$Id: TransferClient.py,v 1.2 2007/05/03 18:59:47 acasajus Exp $"

from DIRAC.Core.DISET.private.BaseClient import BaseClient
from DIRAC.Core.DISET.private.FileHelper import FileHelper
from DIRAC.Core.Utilities.ReturnValues import S_OK, S_ERROR

class TransferClient( BaseClient ):

  def sendFile( self, uFile, sFileId ):
    retVal = self._connect()
    if not retVal[ 'OK' ]:
      return retVal
    oFH = FileHelper( self.oTranspor )
    dRetVal = oFH.getFileDescriptor( uFile, "r" )
    if not dRetVal[ 'OK' ]:
      return dRetVal
    iFD = dRetVal[ 'Value' ]
    #FFC -> File from Client
    dRetVal = self._proposeAction( "FFC" )
    if not dRetVal[ 'OK' ]:
      return dRetVal
    self.oTranspor.sendData( sFileId )
    dRetVal = self.oTranspor.receiveData()
    if not dRetVal[ 'OK' ]:
      return dRetVal
    return oFH.FDToNetwork( iFD )

  def receiveFile( self, uFile, sFileId ):
    retVal = self._connect()
    if not retVal[ 'OK' ]:
      return retVal
    oFH = FileHelper( self.oTranspor )
    dRetVal = oFH.getFileDescriptor( uFile, "w" )
    if not dRetVal[ 'OK' ]:
      return dRetVal
    iFD = dRetVal[ 'Value' ]
    #FTC -> File To Client
    dRetVal = self._proposeAction( "FTC" )
    if not dRetVal[ 'OK' ]:
      return dRetVal
    self.oTranspor.sendData( sFileId )
    dRetVal = self.oTranspor.receiveData()
    if not dRetVal[ 'OK' ]:
      return dRetVal
    return oFH.networkToFD( iFD )



