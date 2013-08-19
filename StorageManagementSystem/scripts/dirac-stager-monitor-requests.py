#! /usr/bin/env python
########################################################################
# $HeadURL$
# File :    dirac-stager-monitor-requests
# Author :  Daniela Remenska
########################################################################
"""
  Report the details of file staging requests, based on selection filters
"""
_RCSID__ = "$Id$"
from DIRAC.Core.Base import Script
from DIRAC                                     import gConfig, gLogger, exit as DIRACExit, S_OK, version

subLogger  = None

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s  [--status=<Status>] [--se=<SE>] [--limit=<integer>] [--showJobs=YES] ...' % Script.scriptName,
                                     'Arguments:',
                                     '  status: file status=(New, Offline, Waiting, Failed, StageSubmitted, Staged). \n',
                                     '  se: storage element \n',
                                     '  showJobs: whether to ALSO list the jobs asking for these files to be staged'
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
    (  'showJobs=', 'Whether to ALSO list the jobs asking for these files to be staged'),
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
      
  if 'status' in  switches.keys():  
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
  
  # weird: if there are no switches (dictionary is empty), then the --limit is ignored!!
  # must FIX that in StorageManagementDB.py!
  # ugly fix:
  newer = '1903-08-02 06:24:38' # select newer than 
  if 'limit' in dictKeys:
    print "Query limited to %s entries" %switchDict['limit']   
    res = client.getCacheReplicas(queryDict, None, newer, None, None, int(switchDict['limit']))
  else:
    res = client.getCacheReplicas(queryDict)
  
  if not res['OK']:
    print res['Message']
  outStr ="\n"
  if res['Records']:
    replicas = res['Value']
    outStr = "%s %s" %(outStr, "Status".ljust(15)) 
    outStr = "%s %s" %(outStr, "LastUpdate".ljust(20))  
    outStr = "%s %s" %(outStr, "LFN".ljust(80))   
    outStr = "%s %s" %(outStr, "SE".ljust(10))  
    outStr = "%s %s" %(outStr, "Reason".ljust(10))
    if 'showJobs' in dictKeys:  
      outStr = "%s %s" %(outStr, "Jobs".ljust(10))  
    outStr = "%s %s" %(outStr, "PinExpiryTime".ljust(15))  
    outStr = "%s %s" %(outStr, "PinLength(sec)".ljust(15))  
    outStr = "%s\n" % outStr  
    
    for crid in replicas.keys():
      outStr = "%s %s" %(outStr, replicas[crid]['Status'].ljust( 15 ))
      outStr = "%s %s" %(outStr, str(replicas[crid]['LastUpdate']).ljust( 20 ))
      outStr = "%s %s" %(outStr, replicas[crid]['LFN'].ljust( 30 ))
      outStr = "%s %s" %(outStr, replicas[crid]['SE'].ljust( 15 ))              
      outStr = "%s %s" %(outStr, str(replicas[crid]['Reason']).ljust( 10 ))
 
      # Task info
      if 'showJobs' in dictKeys:
        resTasks = client.getTasks({'ReplicaID':crid})
        if resTasks['OK']:
          if resTasks['Value']:
            tasks = resTasks['Value']
            jobs = []
            for tid in tasks.keys():
              jobs.append(tasks[tid]['SourceTaskID'])      
            outStr = '%s %s ' % (outStr, str(jobs).ljust(10))
        else:
          outStr = '%s %s ' % (outStr, " --- ".ljust(10))     
      # Stage request info
      # what if there's no request to the site yet?
      resStageRequests = client.getStageRequests({'ReplicaID':crid})
      if not resStageRequests['OK']:
        print resStageRequests['Message']
      if resStageRequests['Records']:
        stageRequests = resStageRequests['Value']        
        for srid in stageRequests.keys():
          outStr = "%s %s" %(outStr, str(stageRequests[srid]['PinExpiryTime']).ljust( 20 ))
          outStr = "%s %s" %(outStr, str(stageRequests[srid]['PinLength']).ljust( 10 ))
           
 
      outStr = "%s\n" % outStr  
    print outStr
  else:
    print "No entries"    
    
if __name__ == "__main__":
  
  subLogger  = gLogger.getSubLogger( __file__ )
  
  registerSwitches()

  switchDict = parseSwitches()
  run()
   
  #Bye
  DIRACExit( 0 )
''' Example:
dirac-stager-show-requests.py --status=Staged --se=GRIDKA-RDST --limit=10 --showJobs=YES
Query limited to 10 entries

 Status          LastUpdate                     LFN                                                               SE       Reason     Jobs       PinExpiryTime   PinLength      
 Staged        2013-06-05 20:10:50 /lhcb/LHCb/Collision12/FULL.DST/00020846/0005/00020846_00054816_1.full.dst GRIDKA-RDST None    ['48498752']  2013-06-05 22:10:50  86400     
 Staged        2013-06-06 15:54:29 /lhcb/LHCb/Collision12/FULL.DST/00020846/0001/00020846_00013202_1.full.dst GRIDKA-RDST None    ['48516851']  2013-06-06 16:54:29  43200     
 Staged        2013-06-07 02:35:41 /lhcb/LHCb/Collision12/FULL.DST/00020846/0003/00020846_00032726_1.full.dst GRIDKA-RDST None    ['48520736']  2013-06-07 03:35:41  43200     
 Staged        2013-06-06 04:16:50 /lhcb/LHCb/Collision12/FULL.DST/00020846/0003/00020846_00030567_1.full.dst GRIDKA-RDST None    ['48510852']  2013-06-06 06:16:50  86400     
 Staged        2013-06-07 03:44:04 /lhcb/LHCb/Collision12/FULL.DST/00020846/0003/00020846_00032699_1.full.dst GRIDKA-RDST None    ['48520737']  2013-06-07 04:44:04  43200     
 Staged        2013-06-05 23:37:46 /lhcb/LHCb/Collision12/FULL.DST/00020846/0003/00020846_00032576_1.full.dst GRIDKA-RDST None    ['48508687']  2013-06-06 01:37:46  86400     
 Staged        2013-06-10 08:50:09 /lhcb/LHCb/Collision12/FULL.DST/00020846/0005/00020846_00056424_1.full.dst GRIDKA-RDST None    ['48518896']  2013-06-10 09:50:09  43200     
 Staged        2013-06-06 11:03:25 /lhcb/LHCb/Collision12/FULL.DST/00020846/0002/00020846_00022161_1.full.dst GRIDKA-RDST None    ['48515583']  2013-06-06 12:03:25  43200     
 Staged        2013-06-06 11:11:50 /lhcb/LHCb/Collision12/FULL.DST/00020846/0002/00020846_00029215_1.full.dst GRIDKA-RDST None    ['48515072']  2013-06-06 12:11:50  43200     
 Staged        2013-06-07 03:19:26 /lhcb/LHCb/Collision12/FULL.DST/00020846/0002/00020846_00022323_1.full.dst GRIDKA-RDST None    ['48515600']  2013-06-07 04:19:26  43200     
 '''
################################################################################

