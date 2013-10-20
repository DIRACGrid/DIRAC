#!/usr/bin/env python
########################################################################
# $HeadURL$
########################################################################
""" Create a DIRAC removal/replicaRemoval|removeFile request to be executed 
    by the DMS Removal Agent
"""
__RCSID__ = "ea64b42 (2012-07-29 16:45:05 +0200) ricardo <Ricardo.Graciani@gmail.com>"

import os
from hashlib import md5
import time
from DIRAC.Core.Base import Script
from DIRAC.Core.Utilities.List import breakListIntoChunks

Script.registerSwitch( "m", "Monitor", "Monitor the execution of the Request (default: print request ID and exit)" )

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[0],
                                     __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ... SE LFN ...' % Script.scriptName,
                                     'Arguments:',
                                     '  SE:       StorageElement|All',
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
    lfns.extend( [ lfn.strip() for lfn in string.splitlines() ] )
  else:
    lfns.append( inputFileName )

from DIRAC.Resources.Storage.StorageElement import StorageElement
import DIRAC
# Check is provided SE is OK
if targetSE != 'All':
  se = StorageElement( targetSE )
  if not se.valid:
    print se.errorReason
    print
    Script.showHelp()

from DIRAC.RequestManagementSystem.Client.RequestContainer      import RequestContainer
from DIRAC.RequestManagementSystem.Client.ReqClient             import ReqClient

reqClient = ReqClient()
requestType = 'removal'
requestOperation = 'replicaRemoval'
if targetSE == 'All':
  requestOperation = 'removeFile'

for lfnList in breakListIntoChunks( lfns, 100 ):

  oRequest = RequestContainer()
  subRequestIndex = oRequest.initiateSubRequest( requestType )['Value']
  attributeDict = {'Operation':requestOperation, 'TargetSE':targetSE}
  oRequest.setSubRequestAttributes( subRequestIndex, requestType, attributeDict )
  files = []
  for lfn in lfnList:
    files.append( {'LFN':lfn} )
  oRequest.setSubRequestFiles( subRequestIndex, requestType, files )
  requestName = "%s_%s" % ( md5( repr( time.time() ) ).hexdigest()[:16], md5( repr( time.time() ) ).hexdigest()[:16] )
  oRequest.setRequestAttributes( {'RequestName':requestName} )

  DIRAC.gLogger.info( oRequest.toXML()['Value'] )

  result = reqClient.setRequest( requestName, oRequest.toXML()['Value'] )
  if result['OK']:
    print 'Submitted Request:', result['Value']
  else:
    print 'Failed to submit Request', result['Message']
  if monitor:
    requestID = result['Value']
    while True:
      result = reqClient.getRequestStatus( requestID )
      if not result['OK']:
        Script.gLogger.error( result['Message'] )
        break
      Script.gLogger.notice( result['Value']['RequestStatus'] )
      if result['Value']['RequestStatus'] == 'Done':
        break
      import time
      time.sleep( 10 )
