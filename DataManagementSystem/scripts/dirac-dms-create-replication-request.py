#!/usr/bin/env python
""" Create a DIRAC transfer/replicateAndRegister request to be executed
    by the DMS Transfer Agent
"""
__RCSID__ = "$Id$"

import os
from hashlib import md5
import time
from DIRAC.Core.Base import Script
from DIRAC.Core.Utilities.List import breakListIntoChunks

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[0],
                                     __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ... DestSE LFN ...' % Script.scriptName,
                                     'Arguments:',
                                     '  DestSE:   Destination StorageElement',
                                     '  LFN:      LFN or file containing a List of LFNs' ] ) )

Script.parseCommandLine( ignoreErrors = False )

monitor = False

args = Script.getPositionalArgs()
if len( args ) < 2:
  Script.showHelp()

targetSE = args.pop( 0 )

lfns = []
for inputFileName in args:
  if os.path.exists( inputFileName ):
    inputFile = open( inputFileName, 'r' )
    string = inputFile.read()
    inputFile.close()
    lfns.extend( [ lfn.strip() for lfn in string.splitlines() ] )
  else:
    lfns.append( inputFileName )

from DIRAC.Resources.Storage.StorageElement import StorageElement
import DIRAC
# Check is provided SE is OK
se = StorageElement( targetSE )
if not se.valid:
  print se.errorReason
  print
  Script.showHelp()

from DIRAC.RequestManagementSystem.Client.ReqClient         import ReqClient
from DIRAC.RequestManagementSystem.Client.Request           import Request
from DIRAC.RequestManagementSystem.Client.Operation         import Operation
from DIRAC.RequestManagementSystem.Client.File              import File
from DIRAC.RequestManagementSystem.private.RequestValidator import gRequestValidator
from DIRAC.Resources.Catalog.FileCatalog import FileCatalog

reqClient = ReqClient()
fc = FileCatalog()

for lfnList in breakListIntoChunks( lfns, 100 ):

  oRequest = Request()
  oRequest.RequestName = "%s_%s" % ( md5( repr( time.time() ) ).hexdigest()[:16], md5( repr( time.time() ) ).hexdigest()[:16] )

  replicateAndRegister = Operation()
  replicateAndRegister.Type = 'ReplicateAndRegister'
  replicateAndRegister.TargetSE = targetSE

  res = fc.getFileMetadata( lfnList )
  if not res['OK']:
    print "Can't get file metadata: %s" % res['Message']
    DIRAC.exit( 1 )
  if res['Value']['Failed']:
    print "Could not get the file metadata of the following, so skipping them:"
    for fFile in res['Value']['Failed']:
      print fFile

  lfnMetadata = res['Value']['Successful']

  for lfn in lfnMetadata:
    rarFile = File()
    rarFile.LFN = lfn
    rarFile.Size = lfnMetadata[lfn]['Size']
    rarFile.Checksum = lfnMetadata[lfn]['Checksum']
    rarFile.GUID = lfnMetadata[lfn]['GUID']
    rarFile.ChecksumType = 'ADLER32'
    replicateAndRegister.addFile( rarFile )

  oRequest.addOperation( replicateAndRegister )
  isValid = gRequestValidator.validate( oRequest )
  if not isValid['OK']:
    print "Request is not valid: ", isValid['Message']
    DIRAC.exit( 1 )

  result = reqClient.putRequest( oRequest )
  if result['OK']:
    print "Request %d submitted successfully" % result['Value']
  else:
    print "Failed to submit Request: ", result['Message']

