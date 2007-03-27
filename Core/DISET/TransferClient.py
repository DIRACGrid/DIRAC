# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/DISET/TransferClient.py,v 1.1 2007/03/27 10:56:46 acasajus Exp $
__RCSID__ = "$Id: TransferClient.py,v 1.1 2007/03/27 10:56:46 acasajus Exp $"

from DIRAC.Core.DISET.private.BaseClient import BaseClient
from DIRAC.Core.DISET.private.FileHelper import FileHelper
from DIRAC.Core.Utilities.ReturnValues import S_OK, S_ERROR
      
class TransferClient( BaseClient ):
  
  def sendFile( self, uFile, sFileId ):
    self._connect()
    oFH = FileHelper( self.oServerTransport )
    dRetVal = oFH.getFileDescriptor( uFile, "r" )
    if not dRetVal[ 'OK' ]:
      return dRetVal
    iFD = dRetVal[ 'Value' ]
    #FFC -> File from Client
    dRetVal = self._proposeAction( "FFC" )
    if not dRetVal[ 'OK' ]:
      return dRetVal
    self.oServerTransport.sendData( sFileId )
    dRetVal = self.oServerTransport.receiveData()
    if not dRetVal[ 'OK' ]:
      return dRetVal  
    return oFH.FDToNetwork( iFD )
  
  def receiveFile( self, uFile, sFileId ):
    self._connect()
    oFH = FileHelper( self.oServerTransport )
    dRetVal = oFH.getFileDescriptor( uFile, "w" )
    if not dRetVal[ 'OK' ]:
      return dRetVal
    iFD = dRetVal[ 'Value' ]
    #FTC -> File To Client
    dRetVal = self._proposeAction( "FTC" )
    if not dRetVal[ 'OK' ]:
      return dRetVal
    self.oServerTransport.sendData( sFileId )
    dRetVal = self.oServerTransport.receiveData()
    if not dRetVal[ 'OK' ]:
      return dRetVal
    return oFH.networkToFD( iFD )
  


