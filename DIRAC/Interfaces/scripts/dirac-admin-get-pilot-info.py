#!/usr/bin/env python
########################################################################
# $HeadURL: svn+ssh://svn.cern.ch/reps/dirac/DIRAC/trunk/DIRAC/Interfaces/scripts/dirac-admin-get-pilot-output.py $
# File :    dirac-admin-get-pilot-output
# Author :  Ricardo Graciani
########################################################################
"""
  Retrieve available info about the given pilot
"""
__RCSID__ = "$Id: dirac-admin-get-pilot-output.py 18161 2009-11-11 12:07:09Z acasajus $"

from DIRAC.Core.Base import Script

extendedPrint = False

def setExtendedPrint( arg ):
  global extendedPrint
  extendedPrint = True

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ... PilotID ...' % Script.scriptName,
                                     'Arguments:',
                                     '  PilotID:  Grid ID of the pilot' ] ) )
Script.registerSwitch( 'e', 'extended', 'Get extended printout', setExtendedPrint )
Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()

if len( args ) < 1:
  Script.showHelp()

from DIRAC import exit as DIRACExit
from DIRAC.Interfaces.API.DiracAdmin                         import DiracAdmin
from DIRAC.Interfaces.API.Dirac                              import Dirac
diracAdmin = DiracAdmin()
dirac = Dirac()
exitCode = 0
errorList = []

for gridID in args:

  result = diracAdmin.getPilotInfo( gridID )
  if not result['OK']:
    errorList.append( ( gridID, result['Message'] ) )
    exitCode = 2
  else:
    res = result['Value'][ gridID ]
    if extendedPrint:
      tab = ''
      for key in ['PilotJobReference', 'Status', 'OwnerDN', 'OwnerGroup', 'SubmissionTime', 'DestinationSite', 'GridSite', ]:
        if key in res:
          diracAdmin.log.notice( '%s%s: %s' % ( tab, key, res[key] ) )
          if not tab:
            tab = '  '
      diracAdmin.log.notice( '' )
      for jobID in res['Jobs']:
        tab = '  '
        result = dirac.attributes( int( jobID ) )
        if not result['OK']:
          errorList.append( ( gridID, result['Message'] ) )
          exitCode = 2
        else:
          job = result['Value']
          diracAdmin.log.notice( '%sJob ID: %s' % ( tab, jobID ) )
          tab += '  '
          for key in [ 'OwnerDN', 'OwnerGroup', 'JobName', 'Status', 'StartExecTime', 'LastUpdateTime', 'EndExecTime' ]:
            if key in job:
              diracAdmin.log.notice( '%s%s:' % ( tab, key ) , job[key] )
      diracAdmin.log.notice( '' )
    else:
      print diracAdmin.pPrint.pformat( { gridID: res } )


for error in errorList:
  print "ERROR %s: %s" % error

DIRACExit( exitCode )
