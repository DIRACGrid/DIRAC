# $HeadURL$
__RCSID__ = "$Id$"

import tempfile, types

from DIRAC                                     import S_OK, S_ERROR
from DIRAC.Core.DISET.RPCClient                import RPCClient
from DIRAC.Core.DISET.TransferClient           import TransferClient
from DIRAC.Core.Utilities.Plotting.FileCoding  import codeRequestInFileId

class ReportsClient:

  def __init__( self, rpcClient = None, transferClient = None ):
    self.serviceName = "Accounting/ReportGenerator"
    self.rpcClient = rpcClient
    self.transferClient = transferClient

  def __getRPCClient( self ):
    if not self.rpcClient:
      return RPCClient( self.serviceName )
    else:
      return self.rpcClient

  def __getTransferClient( self ):
    if not self.transferClient:
      return TransferClient( self.serviceName )
    else:
      return self.transferClient

  def pingService( self ):
    rpcClient = self.__getRPCClient()
    return rpcClient.ping()

  def listReports( self, typeName ):
    rpcClient = self.__getRPCClient()
    result = rpcClient.listReports( typeName )
    if 'rpcStub' in result:
      del( result[ 'rpcStub' ] )
    return result

  def getReport( self, typeName, reportName, startTime, endTime, condDict, grouping, extraArgs = None ):
    rpcClient = self.__getRPCClient()
    if type( extraArgs ) != types.DictType:
      extraArgs = {}
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

  def generatePlot( self, typeName, reportName, startTime, endTime, condDict, grouping, extraArgs = None ):
    rpcClient = self.__getRPCClient()
    if type( extraArgs ) != types.DictType:
      extraArgs = {}
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

  def generateDelayedPlot( self, typeName, reportName, startTime, endTime, condDict, grouping, extraArgs = None, compress = True ):
    if type( extraArgs ) != types.DictType:
      extraArgs = {}
    plotRequest = { 'typeName' : typeName,
                    'reportName' : reportName,
                    'startTime' : startTime,
                    'endTime' : endTime,
                    'condDict' : condDict,
                    'grouping' : grouping,
                    'extraArgs' : extraArgs }
    return codeRequestInFileId( plotRequest, compress )

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
    except Exception as e:
      return S_ERROR( "Can't open file %s for writing: %s" % ( destFilename, str( e ) ) )
    retVal = transferClient.receiveFile( destFile, plotName )
    if not retVal[ 'OK' ]:
      return retVal
    destFile.close()
    return S_OK()
