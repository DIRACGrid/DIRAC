#!/bin/env python
""" create and put 'ReplicateAndRegister' request """
__RCSID__ = "$Id: $"
import os
from DIRAC.Core.Base import Script
Script.setUsageMessage( '\n'.join( [ __doc__,
                                     'Usage:',
                                     ' %s [option|cfgfile] requestName LFNs targetSE [targetSE ...]' % Script.scriptName,
                                     'Arguments:',
                                     ' requestName: a request name',
                                     '        LFNs: single LFN or file with LFNs',
                                     '    targetSE: target SE' ] ) )

def getLFNList( LFNs ):
  """ get list of LFNs """
  lfnList = []
  if os.path.exists( LFNs ):
     for line in open( LFNs ).readlines():
       lfnList.append( line.strip() )
  else:
    lfnList = [ LFNs ]
  return list( set ( lfnList ) )

# # execution
if __name__ == "__main__":

  from DIRAC.Core.Base.Script import parseCommandLine
  parseCommandLine()

  import DIRAC
  from DIRAC import gLogger

  args = Script.getPositionalArgs()

  requestName = None
  LFNs = None
  targetSEs = None
  if not len( args ) < 3:
    Script.showHelp()
    DIRAC.exit( 0 )
  else:
    requestName = args[0]
    LFNs = args[1]
    targetSEs = list( set( [ targetSE.strip() for targetSE in args[2:] ] ) )

  lfnList = getLFNList( LFNs )
  gLogger.info( "will create request '%s' with 'ReplicateAndRegister' "\
                "operation using %s lfns and %s target SEs" % ( requestName, len( lfnList ), len( targetSEs ) ) )

  from DIRAC.RequestManagementSystem.Client.Request import Request
  from DIRAC.RequestManagementSystem.Client.Operation import Operation
  from DIRAC.RequestManagementSystem.Client.File import File
  from DIRAC.RequestManagementSystem.Client.ReqClient import ReqClient
  from DIRAC.Resources.Catalog.FileCatalog import FileCatalog

  metaDatas = FileCatalog().getFileMetadata( lfnList )
  if not metaDatas["OK"]:
    gLogger.error( "unable to read metadata for lfns: %s" % metaDatas["Message"] )
    DIRAC.exit( -1 )
  metaDatas = metaDatas["Value"]
  for failedLFN, reason in metaDatas["Failed"].items():
    gLogger.error( "skipping %s: %s" % ( failedLFN, reason ) )
  lfnList = [ lfn for lfn in lfnList if lfn not in metaDatas["Failed"] ]

  if not lfnList:
    gLogger.error( "LFN list is empty!!!" )
    DIRAC.exit( -1 )

  if len( lfnList ) > Operation.MAX_FILES:
    gLogger.error( "too many LFNs, max number of files per operation is %s" % Operation.MAX_FILES )
    DIRAC.exit( -1 )

  request = Request()
  request.RequestName = requestName

  replicateAndRegister = Operation()
  replicateAndRegister.Type = "ReplicateAndRegister"
  replicateAndRegister.TargetSE = ",".join( targetSEs )

  for lfn in lfnList:
    metaDict = metaDatas["Successful"][lfn]
    opFile = File()
    opFile.LFN = lfn
    opFile.Size = metaDict["Size"]

    if "Checksum" in metaDict:
      # # should check checksum type, now assuming Adler32 (metaDict["ChecksumType"] = 'AD'
      opFile.Checksum = metaDict["Checksum"]
      opFile.ChecksumType = "ADLER32"
    replicateAndRegister.addFile( opFile )

  reqClient = ReqClient()
  putRequest = reqClient.putRequest( request )
  if not putRequest["OK"]:
    gLogger.error( "unable to put request '%s': %s" % ( requestName, putRequest["Message"] ) )
    DIRAC.exit( -1 )

  gLogger.always( "Request '%s' has been put to ReqDB for execution." )
  gLogger.always( "You can monitor its status using command: 'dirac-rms-show-request %s'" % requestName )
  DIRAC.exit( 0 )







