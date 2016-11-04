#!/bin/env python
""" create and put 'ReplicateAndRegister' request """
__RCSID__ = "$Id: $"
import os
from DIRAC.Core.Base import Script
Script.setUsageMessage( '\n'.join( [ __doc__,
                                     'Usage:',
                                     ' %s [option|cfgfile] requestName LFNs targetSE1 [targetSE2 ...]' % Script.scriptName,
                                     'Arguments:',
                                     ' requestName: a request name',
                                     '        LFNs: single LFN or file with LFNs',
                                     '    targetSE: target SE' ] ) )

def getLFNList( arg ):
  """ get list of LFNs """
  lfnList = []
  if os.path.exists( arg ):
     lfnList = [line.split()[0] for line in open( arg ).read().splitlines()]
  else:
    lfnList = [ arg ]
  return list( set ( lfnList ) )

# # execution
if __name__ == "__main__":

  from DIRAC.Core.Base.Script import parseCommandLine
  parseCommandLine()

  import DIRAC
  from DIRAC import gLogger

  args = Script.getPositionalArgs()

  requestName = None
  targetSEs = None
  if len( args ) < 3:
    Script.showHelp()
    DIRAC.exit( 1 )

  requestName = args[0]
  lfnList = getLFNList( args[1] )
  targetSEs = list( set( [ se for targetSE in args[2:] for se in targetSE.split( ',' ) ] ) )

  gLogger.info( "Will create request '%s' with 'ReplicateAndRegister' "\
                "operation using %s lfns and %s target SEs" % ( requestName, len( lfnList ), len( targetSEs ) ) )

  from DIRAC.RequestManagementSystem.Client.Request import Request
  from DIRAC.RequestManagementSystem.Client.Operation import Operation
  from DIRAC.RequestManagementSystem.Client.File import File
  from DIRAC.RequestManagementSystem.Client.ReqClient import ReqClient
  from DIRAC.Resources.Catalog.FileCatalog import FileCatalog
  from DIRAC.Core.Utilities.List import breakListIntoChunks

  lfnChunks = breakListIntoChunks( lfnList, 100 )
  multiRequests = len( lfnChunks ) > 1

  error = 0
  count = 0
  reqClient = ReqClient()
  fc = FileCatalog()
  requestIDs = []
  for lfnChunk in lfnChunks:
    metaDatas = fc.getFileMetadata( lfnChunk )
    if not metaDatas["OK"]:
      gLogger.error( "unable to read metadata for lfns: %s" % metaDatas["Message"] )
      error = -1
      continue
    metaDatas = metaDatas["Value"]
    for failedLFN, reason in metaDatas["Failed"].items():
      gLogger.error( "skipping %s: %s" % ( failedLFN, reason ) )
    lfnChunk = set( metaDatas["Successful"] )

    if not lfnChunk:
      gLogger.error( "LFN list is empty!!!" )
      error = -1
      continue

    if len( lfnChunk ) > Operation.MAX_FILES:
      gLogger.error( "too many LFNs, max number of files per operation is %s" % Operation.MAX_FILES )
      error = -1
      continue

    count += 1
    request = Request()
    request.RequestName = requestName if not multiRequests else '%s_%d' % ( requestName, count )

    replicateAndRegister = Operation()
    replicateAndRegister.Type = "ReplicateAndRegister"
    replicateAndRegister.TargetSE = ",".join( targetSEs )

    for lfn in lfnChunk:
      metaDict = metaDatas["Successful"][lfn]
      opFile = File()
      opFile.LFN = lfn
      opFile.Size = metaDict["Size"]

      if "Checksum" in metaDict:
        # # should check checksum type, now assuming Adler32 (metaDict["ChecksumType"] = 'AD'
        opFile.Checksum = metaDict["Checksum"]
        opFile.ChecksumType = "ADLER32"
      replicateAndRegister.addFile( opFile )

    request.addOperation( replicateAndRegister )

    putRequest = reqClient.putRequest( request )
    if not putRequest["OK"]:
      gLogger.error( "unable to put request '%s': %s" % ( request.RequestName, putRequest["Message"] ) )
      error = -1
      continue
    requestIDs.append( str( putRequest["Value"] ) )
    if not multiRequests:
      gLogger.always( "Request '%s' has been put to ReqDB for execution." % request.RequestName )

  if multiRequests:
    gLogger.always( "%d requests have been put to ReqDB for execution, with name %s_<num>" % ( count, requestName ) )
  if requestIDs:
    gLogger.always( "RequestID(s): %s" % " ".join( requestIDs ) )
  gLogger.always( "You can monitor requests' status using command: 'dirac-rms-show-request <requestName/ID>'" )
  DIRAC.exit( error )







