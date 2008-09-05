# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/AccountingSystem/Client/ReportsClient.py,v 1.4 2008/09/05 11:44:44 acasajus Exp $
__RCSID__ = "$Id: ReportsClient.py,v 1.4 2008/09/05 11:44:44 acasajus Exp $"

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

  def listReports( self, typeName ):
    rpcClient = self.__getRPCClient()
    return rpcClient.listReports( typeName )

  def getReport( self, typeName, reportName, startTime, endTime, condDict, grouping, extraArgs = {} ):
    rpcClient = self.__getRPCClient()
    plotRequest = { 'typeName' : typeName,
                    'reportName' : reportName,
                    'startTime' : startTime,
                    'endTime' : endTime,
                    'argsDict' : argsDict,
                    'grouping' : grouping,
                    'condDict' : condDict }
    return rpcClient.getReport( plotRequest )

  def generatePlot( self, typeName, reportName, startTime, endTime, condDict, grouping, extraArgs = {} ):
    rpcClient = self.__getRPCClient()
    plotRequest = { 'typeName' : typeName,
                    'reportName' : reportName,
                    'startTime' : startTime,
                    'endTime' : endTime,
                    'condDict' : condDict,
                    'grouping' : grouping,
                    'extraArgs' : extraArgs }
    return rpcClient.generatePlot( plotRequest )

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