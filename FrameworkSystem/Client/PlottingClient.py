# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/FrameworkSystem/Client/PlottingClient.py,v 1.1 2009/02/22 23:58:23 atsareg Exp $

""" PlottingClient is a client of the Plotting Service
"""

__RCSID__   = "$Id: PlottingClient.py,v 1.1 2009/02/22 23:58:23 atsareg Exp $"
__VERSION__ = "$Revision: 1.1 $"

import re
import types, tempfile
from DIRAC import S_OK, S_ERROR
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.Core.DISET.TransferClient import TransferClient

class PlottingClient:
  
  def __init__( self, rpcClient=False, transferClient=False ):
    self.serviceName = "Framework/Plotting"
    self.rpcClient = rpcClient
    self.transferClient = transferClient

  def __getRPCClient(self):
    if self.rpcClient:
      return self.rpcClient    
    return RPCClient(self.serviceName)
  
  def __getTransferClient(self):
    if self.transferClient:
      return self.transferClient
    return TransferClient(self.serviceName)
  
  def getPlotToMemory( self, plotName ):
    transferClient = self.__getTransferClient()
    tmpFile = tempfile.TemporaryFile()
    retVal = transferClient.receiveFile( tmpFile, plotName )
    if not retVal[ 'OK' ]:
      return retVal
    tmpFile.seek( 0 )
    data = tmpFile.read()
    tmpFile.close()
    return S_OK( data )

  def getPlotToFile( self, plotName, fileName ):
    transferClient = self.__getTransferClient()
    try:
      destFile = file( fileName, "wb" )
    except Exception, e:
      return S_ERROR( "Can't open file %s for writing: %s" % ( fileName, str(e) ) )
    retVal = transferClient.receiveFile( destFile, plotName )
    if not retVal[ 'OK' ]:
      return retVal
    destFile.close()
    return S_OK()  
  
  def getPieChat(self,data,metadata,fname=False):
    
    client = self.__getRPCClient()
    result = client.generatePieChat(data,metadata)
    if not result['OK']:
      return result
    
    plotName = result['Value']
    if fname:
      result = self.getPlotToFile(plotName,fname)
    else:
      result = self.getPlotToMemory(plotName)
      
    return result    
     