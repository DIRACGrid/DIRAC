#!/usr/bin/env python
########################################################################
# $HeadURL$
########################################################################
""" Create a DIRAC transfer/replicateAndRegister request to be executed
    by the DMS Transfer Agent
"""
__RCSID__ = "$Id$"

import os
from hashlib import md5
import time
from DIRAC.Core.Base import Script
from DIRAC.Core.Utilities.List import breakListIntoChunks

Script.registerSwitch( "m", "Monitor", "Monitor the execution of the Request (default: print request ID and exit)" )

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[0],
                                     __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ... DestSE LFN ...' % Script.scriptName,
                                     'Arguments:',
                                     '  DestSE:   Destination StorageElement',
                                     '  LFN:      LFN or file containing a List of LFNs' ] ) )

Script.parseCommandLine( ignoreErrors = False )

monitor = False

for switch in Script.getUnprocessedSwitches():
  if switch[0].lower() == "m" or switch[0].lower() == "Monitor":
    monitor = True

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
    lfns.extend( string.splitlines() )
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

from DIRAC.RequestManagementSystem.Client.Request           import Request
from DIRAC.RequestManagementSystem.Client.Operation         import Operation
from DIRAC.RequestManagementSystem.Client.File              import File
from DIRAC.RequestManagementSystem.Client.ReqClient         import ReqClient
from DIRAC.RequestManagementSystem.private.RequestValidator import gRequestValidator

requestClient = ReqClient()
requestOperation = 'replicateAndRegister'

for lfnList in breakListIntoChunks( lfns, 100 ):

  oRequest = Request()
  oRequest.RequestName = "%s_%s" % ( md5( repr( time.time() ) ).hexdigest()[:16], md5( repr( time.time() ) ).hexdigest()[:16] )

  replicateAndRegister = Operation()
  replicateAndRegister.Type = 'ReplicateAndRegister'
  replicateAndRegister.TargetSE = targetSE

  for lfn in lfnList:
    rarFile = File()
    rarFile.LFN = lfn
    replicateAndRegister.addFile( rarFile )

  oRequest.addOperation( replicateAndRegister )
  isValid = gRequestValidator.validate( oRequest )
  if not isValid['OK']:
    print "Request is not valid: ", isValid['Message']
    DIRAC.exit( 1 )

  DIRAC.gLogger.info( oRequest.toJSON()['Value'] )

  result = requestClient.putRequest( oRequest )
  if result['OK']:
    print 'Submitted Request:', result['Value']
  else:
    print 'Failed to submit Request', result['Message']
