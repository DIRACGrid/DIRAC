# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/AccountingSystem/Client/ReportsClient.py,v 1.3 2008/07/24 17:41:31 acasajus Exp $
__RCSID__ = "$Id: ReportsClient.py,v 1.3 2008/07/24 17:41:31 acasajus Exp $"

import tempfile
from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Utilities import Time
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.Core.DISET.TransferClient import TransferClient

class ReportsClient:

  def __init__( self, rpcClient = None, transferClient = None ):
    self.serviceName = "Accounting/ReportGenerator"
    self.rpcClient = rpcClient
    self.transferClient = transferClient

  def __getRPCClient(self):
    if not self.rpcClient:
      return RPCClient( self.serviceName )
    else:
      return self.rpcClient

  def __getTransferClient(self):
    if not self.transferClient:
      return TransferClient( self.serviceName )
    else:
      return self.transferClient

  def pingService(self):
    rpcClient = self.__getRPCClient()
    return rpcClient.ping()

  def listSummaries(self):
    rpcClient = self.__getRPCClient()
    return rpcClient.listSummaries()

  def listPlots( self, typeName ):
    rpcClient = self.__getRPCClient()
    return rpcClient.listPlots( typeName )

  def getSummary( self, summaryName, startTime, endTime, argsDict ):
    rpcClient = self.__getRPCClient()
    return rpcClient.generateSummary( summaryName, startTime, endTime, argsDict )

  def generatePlot( self, typeName, plotName, startTime, endTime, argsDict, grouping, extraArgs = {} ):
    rpcClient = self.__getRPCClient()
    return rpcClient.generatePlot( typeName, plotName, startTime, endTime, argsDict, grouping, extraArgs )

  def getPlotToMem( self, plotName ):
    transferClient = self.__getTransferClient()
    tmpFile = tempfile.TemporaryFile()
    retVal = transferClient.receiveFile( tmpFile, plotName )
    if not retVal[ 'OK' ]:
      return retVal
    tmpFile.seek( 0 )
    data = tmpFile.read()
    tmpFile.close()
    return S_OK( data )

  def getPlotToDirectory( self, plotName, dirDestination ):
    transferClient = self.__getTransferClient()
    try:
      destFilename = "%s/%s" % ( dirDestination, plotName )
      destFile = file( destFilename, "wb" )
    except Exception, e:
      return S_ERROR( "Can't open file %s for writing: %s" % ( destFilename, str(e) ) )
    retVal = transferClient.receiveFile( destFile, plotName )
    if not retVal[ 'OK' ]:
      return retVal
    destFile.close()
    return S_OK()