#! /usr/bin/env python
########################################################################
# $HeadURL$
# File :    dirac-stager-show-requests
# Author :  Daniela Remenska
########################################################################
"""
  Report the summary of the staging progress of jobs
"""
_RCSID__ = "$Id$"
import DIRAC
from DIRAC.Core.Base import Script
from DIRAC                                     import gConfig, gLogger, exit as DIRACExit, S_OK, version

subLogger  = None

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s  [--status=<Status>] [--se=<SE>] [--limit=<integer>] ...' % Script.scriptName,
                                     'Arguments:',
                                     '  status: file status=(New, Offline, Waiting, Failed, StageSubmitted, Staged). \n',
                                     '  se: storage element \n',
                                     ' WARNING: Query may be heavy, please use --limit switch!'
                                       ] ) )
  
def registerSwitches():
  '''
    Registers all switches that can be used while calling the script from the
    command line interface.
  '''
  
  switches = (
    ( 'status=',     'Filter per file status=(New, Offline, Waiting, Failed, StageSubmitted, Staged). If not used, all status values will be taken into account' ),
    ( 'se=',        'Filter per Storage Element. If not used, all storage elements will be taken into account.' ),
    ( 'limit=',        'Limit the number of entries returned.' ),
             )
  
  for switch in switches:
    Script.registerSwitch( '', switch[ 0 ], switch[ 1 ] )

def parseSwitches():
  '''
    Parses the arguments passed by the user
  '''
  
  Script.parseCommandLine( ignoreErrors = True )
  args = Script.getPositionalArgs()
  if args:
    subLogger.error( "Found the following positional args '%s', but we only accept switches" % args )
    subLogger.error( "Please, check documentation below" )
    Script.showHelp()
    DIRACExit( 1 )
  
  switches = dict( Script.getUnprocessedSwitches() )  
  
  for key in ( 'status', 'se','limit'):

    if not key in switches:
      print "You're not using switch --%s, query may take long!" % key
      
    
  if not switches[ 'status' ] in ( 'New', 'Offline', 'Waiting','Failed','StageSubmitted','Staged' ):
    subLogger.error( "Found \"%s\" as Status value. Incorrect value used!" % switches[ 'status' ] )
    subLogger.error( "Please, check documentation below" )
    Script.showHelp()
    DIRACExit( 1 )
  
  subLogger.debug( "The switches used are:" )
  map( subLogger.debug, switches.iteritems() )

  return switches  

#...............................................................................

def run():
  
  from DIRAC.StorageManagementSystem.Client.StorageManagerClient import StorageManagerClient
  client = StorageManagerClient()
  queryDict = {}

  dictKeys = switchDict.keys()
  
  if 'status' in dictKeys:
    queryDict['Status'] = str(switchDict['status']) 
  
  
  if 'se' in dictKeys:
    queryDict['SE'] = str(switchDict['se']);
  
  if 'limit' in dictKeys:
    print "Query limited to %s entries" %switchDict['limit']   
    res = client.getCacheReplicas(queryDict, None, None, None, None, int(switchDict['limit']))
  else:
    res = client.getCacheReplicas(queryDict)
  
  
  print res
  
if __name__ == "__main__":
  
  subLogger  = gLogger.getSubLogger( __file__ )
  
  registerSwitches()

  switchDict = parseSwitches()
  run()
   
  #Bye
  DIRACExit( 0 )

################################################################################

