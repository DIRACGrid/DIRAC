# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/AccountingSystem/Client/ReportsClient.py,v 1.1 2008/04/04 16:24:05 acasajus Exp $
__RCSID__ = "$Id: ReportsClient.py,v 1.1 2008/04/04 16:24:05 acasajus Exp $"

import tempfile
from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Utilities import Time
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.Core.DISET.TransferClient import TransferClient

class ReportsClient:

  def __init__(self):
    self.serviceName = "Accounting/ReportGenerator"

  def pingService(self):
    rpcClient = RPCClient( self.serviceName )
    return rpcClient.ping()

  def listSummaries(self):
    rpcClient = RPCClient( self.serviceName )
    return rpcClient.listSummaries()

  def listViews(self):
    rpcClient = RPCClient( self.serviceName )
    return rpcClient.listViews()

  def getSummary( self, summaryName, startTime, endTime, argsDict ):
    rpcClient = RPCClient( self.serviceName )
    return rpcClient.generateSummary( summaryName, startTime, endTime, argsDict )

  def plotView( self, viewName, startTime, endTime, argsDict ):
    rpcClient = RPCClient( self.serviceName )
    return rpcClient.plotView( viewName, startTime, endTime, argsDict )

  def getPlotToMem( self, plotName ):
    transferClient = TransferClient( self.serviceName )
    tmpFile = tempfile.TemporaryFile()
    retVal = transferClient.receiveFile( tmpFile, plotName )
    if not retVal[ 'OK' ]:
      return retVal
    tmpFile.seek( 0 )
    data = tmpFile.read()
    tmpFile.close()
    return S_OK( data )

  def getPlotToDirectory( self, plotName, dirDestination ):
    transferClient = TransferClient( self.serviceName )
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