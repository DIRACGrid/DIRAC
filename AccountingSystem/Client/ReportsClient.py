# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/AccountingSystem/Client/ReportsClient.py,v 1.8 2009/06/03 12:30:35 acasajus Exp $
__RCSID__ = "$Id: ReportsClient.py,v 1.8 2009/06/03 12:30:35 acasajus Exp $"

import tempfile
import base64

try:
  import zlib
  zCompressionEnabled = True
except:
  zCompressionEnabled = False

from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Utilities import Time, DEncode
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.Core.DISET.TransferClient import TransferClient

class ReportsClient:

  def __init__( self, rpcClient = None, transferClient = None ):
    self.serviceName = "Accounting/ReportGenerator"
    self.rpcClient = rpcClient
    self.transferClient = transferClient
    self.__forceRawEncoding = False

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
    result = rpcClient.listReports( typeName )
    if 'rpcStub' in result:
      del( result[ 'rpcStub' ] )
    return result

  def getReport( self, typeName, reportName, startTime, endTime, condDict, grouping, extraArgs = {} ):
    rpcClient = self.__getRPCClient()
    plotRequest = { 'typeName' : typeName,
                    'reportName' : reportName,
                    'startTime' : startTime,
                    'endTime' : endTime,
                    'condDict' : condDict,
                    'grouping' : grouping,
                    'extraArgs' : extraArgs }
    result = rpcClient.getReport( plotRequest )
    if 'rpcStub' in result:
      del( result[ 'rpcStub' ] )
    return result

  def generatePlot( self, typeName, reportName, startTime, endTime, condDict, grouping, extraArgs = {} ):
    rpcClient = self.__getRPCClient()
    plotRequest = { 'typeName' : typeName,
                    'reportName' : reportName,
                    'startTime' : startTime,
                    'endTime' : endTime,
                    'condDict' : condDict,
                    'grouping' : grouping,
                    'extraArgs' : extraArgs }
    result = rpcClient.generatePlot( plotRequest )
    if 'rpcStub' in result:
      del( result[ 'rpcStub' ] )
    return result

  def generateDelayedPlot( self, typeName, reportName, startTime, endTime, condDict, grouping, extraArgs = {}, compress = True ):
    plotRequest = { 'typeName' : typeName,
                    'reportName' : reportName,
                    'startTime' : startTime,
                    'endTime' : endTime,
                    'condDict' : condDict,
                    'grouping' : grouping,
                    'extraArgs' : extraArgs }
    compress = compress and zCompressionEnabled
    if compress:
      plotStub = "Z:%s" % base64.urlsafe_b64encode( zlib.compress( DEncode.encode( plotRequest ), 9 ) )
    elif not self.__forceRawEncoding:
      plotStub = "S:%s" % base64.urlsafe_b64encode( DEncode.encode( plotRequest ) )
    else:
      plotStub = "R:%s" % DEncode.encode( plotRequest )
    thbStub = False
    if 'thumbnail' in extraArgs and extraArgs[ 'thumbnail' ]:
      thbStub = plotStub
      extraArgs[ 'thumbnail' ] = False
      if compress:
        plotStub = "Z:%s" % base64.urlsafe_b64encode( zlib.compress( DEncode.encode( plotRequest ), 9 ) )
      elif not self.__forceRawEncoding:
        plotStub = "S:%s" % base64.urlsafe_b64encode( DEncode.encode( plotRequest ) )
      else:
        plotStub = "R:%s" % DEncode.encode( plotRequest )
    return S_OK( { 'plot' : plotStub, 'thumbnail' : thbStub } )

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