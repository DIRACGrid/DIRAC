#!/bin/env python

import re, sys, tempfile, commands, os, urllib

def submitJob( executable, nJobs, jobDir, submitOptions ):
  """ Submit nJobs to the condor batch system
  """

  jdlFile = tempfile.NamedTemporaryFile( dir=jobDir, suffix=".jdl" )
  jdlFile.write("""
  Executable = %s
  Universe = vanilla
  Requirements   = OpSys == "LINUX"
  Initialdir = %s
  Output = $(Cluster).$(Process).out
  Error = $(Cluster).$(Process).err
  Log = test.log
  Environment = CONDOR_JOBID=$(Cluster).$(Process)
  Getenv = True
  
  Queue %s
  
  """ % (executable,jobDir,nJobs)
  )
  
  jdlFile.flush()
  
  status,output = commands.getstatusoutput( 'condor_submit %s %s' % ( submitOptions, jdlFile.name ) )
  
  jdlFile.close()
   
  submittedJobs = 0
  cluster = ''
  if status == 0:
    lines = output.split( '\n' )
    for line in lines:
      if 'cluster' in line:
        result = re.match( '(\d+) job.*cluster (\d+)\.', line )
        if result:
          submittedJobs, cluster = result.groups()
          try:
            submittedJobs = int( submittedJobs )
          except:
            submittedJobs = 0   
  else:
    print status
    print output
    return status    
  
  if submittedJobs > 0 and cluster:
    print status
    for i in range( submittedJobs ):
      print '.'.join( [cluster,str(i)] )
    return status  
  else:
    print 1
    print "No jobs were submitted to the local batch system"     
    return 1   

def killJob( jobList ):
  """ Kill jobs in the given list
  """
  
  result = 0
  successful = []
  failed = []
  for job in jobList:
    status,output = commands.getstatusoutput( 'condor_rm %s' % job )
    if status != 0:
      result += 1
      failed.append( job )
    else:
      successful.append( job )  
  
  print result
  for job in successful:
    print job
  return result

def getJobStatus( jobIDList, user ):
  """ Get status of the jobs in the given list
  """
  jobIDs = list ( jobIDList )
  resultDict = {}
  
  status,stdout_q = commands.getstatusoutput( 'condor_q -submitter %s' % user )
  if status != 0:
    print status
    print stdout_q
    return status
    
  status_history,stdout_history = commands.getstatusoutput( 'condor_history | grep %s' % user )  
  
  stdout = stdout_q
  if status_history == 0:
    stdout = '\n'.join( [stdout_q,stdout_history] )

  if len( stdout ):
    lines = stdout.split( '\n' )
    for line in lines:
      l = line.strip()
      for job in jobIDList:
        if l.startswith( job ):
          if " I " in line:
            resultDict[job] = 'Waiting'
          elif " R " in line:
            resultDict[job] = 'Running'
          elif " C " in line:
            resultDict[job] = 'Done'
          elif " X " in line:
            resultDict[job] = 'Aborted'    

  if len( resultDict ) != len( jobIDList ):
    for job in jobIDList:
      if not job in resultDict:
        resultDict[job] = 'Unknown'
        
  # Final output
  status = 0
  print status
  for job,status in resultDict.items():
    print ':::'.join( [job,status] )  
        
  return status    

def getCEStatus( user ):
  """  Get the overall status of the CE 
  """ 

  waitingJobs = 0
  runningJobs = 0

  status,stdout = commands.getstatusoutput( 'condor_q -submitter %s' % user )
  if status != 0:
    if "no record" in stdout:
      status = 0
      print status
      print ":::".join( ["Waiting",str(waitingJobs)] )
      print ":::".join( ["Running",str(runningJobs)] )  
      return status
    print status
    print stdout
    return status
  
  if "no record" in stdout:
    status = 0
    print status
    print ":::".join( ["Waiting",str(waitingJobs)] )
    print ":::".join( ["Running",str(runningJobs)] )  
    return status

  if len( stdout ):
    lines = stdout.split( '\n' )
    for line in lines:
      if not line.strip():
        continue
      if " I " in line:
        waitingJobs += 1
      elif " R " in line:
        runningJobs += 1  

  # Final output
  status = 0
  print status
  print ":::".join( ["Waiting",str(waitingJobs)] )
  print ":::".join( ["Running",str(runningJobs)] )  
  return status

#####################################################################################

# Get standard arguments and pass to the interface implementation functions

command = sys.argv[1]
print "============= Start output ==============="
if command == "submit_job":
  executable,outputDir,errorDir,workDir,nJobs,infoDir,jobStamps,queue,submitOptions = sys.argv[2:]
  submitOptions = urllib.unquote(submitOptions)
  if submitOptions == '-':
    submitOptions = ''
  status = submitJob( executable, nJobs, outputDir, submitOptions )
elif command == "kill_job":
  jobStamps,infoDir = sys.argv[2:]
  jobList = jobStamps.split('#')
  status = killJob( jobList )
elif command == "job_status":
  jobStamps,infoDir,user = sys.argv[2:]
  jobList = jobStamps.split('#')
  status = getJobStatus( jobList, user )  
elif command == "status_info":
  infoDir,workDir,user,queue = sys.argv[2:]
  status = getCEStatus( user )   
