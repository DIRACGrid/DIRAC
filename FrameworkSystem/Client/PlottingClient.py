# $HeadURL$

""" PlottingClient is a client of the Plotting Service
"""

__RCSID__   = "$Id$"
__VERSION__ = "$Revision: 1.2 $"

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
  
  def getPlot(self,data,plotMetadata={},fname=False,**kw):
    
    client = self.__getRPCClient()
    plotMetadata.update(kw)
    subplotMetadata = plotMetadata.get('metadata',[])
    result = client.generatePlot(data,plotMetadata,subplotMetadata)
    
    print result
    
    if not result['OK']:
      return result
    
    plotName = result['Value']
    if fname:
      result = self.getPlotToFile(plotName,fname)
    else:
      result = self.getPlotToMemory(plotName)
      
    return result    
     
     