#########################################################################################
#
# Script to populate the DIRAC FileCatalog with the information from the LFC
# FileCatalog using multiple LFC sources
#
# Author: A.Tsaregorodtsev
# Last Modified: 9.01.2012 
#
#########################################################################################

from DIRAC.Core.Base import Script
Script.parseCommandLine()

import DIRAC.Resources.Catalog.LcgFileCatalogClient as LcgFileCatalogClient
from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getUsernameForDN, getGroupsWithVOMSAttribute
from DIRAC.Core.Utilities.ThreadPool import ThreadPool, ThreadedJob
from DIRAC.Core.Utilities.ProcessPool import ProcessPool
from DIRAC import gConfig, S_OK, S_ERROR

import time, sys, random

dirCount = 0
fileCount = 0
globalStart = time.time()

dnCache = {}
roleCache = {}

def getUserNameAndGroup(info):
  """ Get the user name and group from the DN and VOMS role
  """

  global dnCache, roleCache

  owner = {}
  if not "OwnerDN" in info:
    return owner
 
  username = dnCache.get(info.get('OwnerDN'))
  if not username:
    result = getUsernameForDN(info.get('OwnerDN','Unknown'))
    if result['OK']:
      username = result['Value']
      dnCache[info['OwnerDN']] = username
    elif "No username" in result['Message']:
      username = 'Unknown'
      dnCache[info['OwnerDN']] = username

  if username and username != 'Unknown':
    groups = roleCache.get('/'+info.get('OwnerRole'))
    if not groups:
      groups = getGroupsWithVOMSAttribute('/'+info['OwnerRole'])
      roleCache['/'+info['OwnerRole']] = groups
    if groups:
      owner['username'] = username
      owner['group'] = groups[0]

  return owner

def processDir(initPath,recursive=False,host=None,fcInit=None,dfcInit=None):
  """ Process one directory,  possibly recursively 
  """

  global  globalStart, dnCache, roleCache

  #print "AT >>> processDir initPath", initPath

  fc = fcInit
  if not fc:
    fc = LcgFileCatalogClient.LcgFileCatalogClient( host=host )
  dfc = dfcInit
  if not dfc:
    dfc = FileCatalogClient()

  start = time.time()

  resultList = fc.listDirectory(initPath,True)
  if not resultList['OK']:
    result = S_ERROR("Failed LFC lookup for %s" % initPath)
    result['Path'] = initPath
    return result

  lfc_time = (time.time() - start)

  s = time.time()

  if resultList['OK']:
  # Add directories

    if resultList['Value']['Failed']:
      return S_ERROR("Path %s failed: %s" % (initPath,resultList['Value']['Failed'][initPath]))

    dirDict = resultList['Value']['Successful'][initPath]['SubDirs']
    paths = {}
    for path,info in dirDict.items():
      paths[path] = {}
      paths[path]['Mode'] = info['Mode']
      owner = getUserNameAndGroup( info )
      if owner:
        paths[path]['Owner'] = owner

    p_dirs = time.time() - s
    s = time.time()
    nDir = len(paths)
    if nDir:
      #print "Adding %d directories in %s" % (nDir,initPath)
      result = dfc.createDirectory(paths)
      if not result['OK']:
        print "Error adding directories:",result['Message']


    e_dirs = time.time() - s

    # Add files

    s = time.time()

    fileDict = resultList['Value']['Successful'][initPath]['Files']
    lfns = {}
    for lfn,info in fileDict.items():

      #print info['MetaData']

      lfns[lfn] = {}
      lfns[lfn]['Size'] = info['MetaData']['Size']
      lfns[lfn]['Checksum'] = info['MetaData'].get('Checksum','')
      lfns[lfn]['GUID'] = info['MetaData']['GUID']
      lfns[lfn]['Mode'] = info['MetaData']['Mode']
      lfns[lfn]['PFN'] = ''
      owner = getUserNameAndGroup( info['MetaData'] )
      if owner:
        lfns[lfn]['Owner'] = owner

      if info['Replicas']:
        seList = info['Replicas'].keys()
        lfns[lfn]['SE'] = seList

    p_files = time.time() - s
    s = time.time()

    nFile = len(lfns)
    nRep = 0
    if nFile:

      for lfn in lfns:
        if 'SE' in lfns[lfn]:
          nRep += len(lfns[lfn]['SE'])

      #print "Adding %d files in %s" % (nFile,initPath)
      
      done = False
      count = 0
      error = False
      while not done:
        count += 1
        result = dfc.addFile(lfns)
        if not result['OK']:
          print "Error adding files %d:" % count,result['Message']
          if count > 10:
            print "Completely failed path", initPath
            break
          error = True
          time.sleep(2)
        elif error:
          print "Successfully added files on retry %d" % count
          done = True
        else:
          done = True


    e_files = time.time() - s

    dfc_time = time.time() - start - lfc_time
    total_time = time.time() - globalStart

    format = "== %s: time lfc/dfc %.2f/%.2f, files %d, dirs %d, reps %d, time: %.2f/%.2f/%.2f/%.2f %.2f \n"
    outputFile = open('lfc_dfc.out','a')
    outputFile.write( format % (initPath,lfc_time,dfc_time,nFile,nDir,nRep,p_dirs,e_dirs,p_files,e_files,total_time) )
    outputFile.close()

#    print format % (initPath,lfc_time,dfc_time,nFile,fileCount,nDir,dirCount,p_dirs,e_dirs,p_files,e_files,total_time)

    # Go into directories
    if recursive:
      for path in paths:
        result = processDir(path,True,host=host,fcInit=fc,dfcInit=dfc)
        if result['OK']:
          nFile += result['Value'].get('NumberOfFiles',0)
          nDir += result['Value'].get('NumberOfDirectories',0)
          nRep += result['Value'].get('NumberOfReplicas',0)

    resultDict = {}
    resultDict['NumberOfFiles'] = nFile
    resultDict['NumberOfDirectories'] = nDir
    resultDict['NumberOfReplicas'] = nRep
    resultDict['Path'] = initPath
    resultDict['Directories'] = dirDict.keys()

    #print "AT >>> processDir",initPath,"done %.2f" % (time.time()-start)

    return S_OK(resultDict)

def finalizeDirectory(task,result):

  global lfcHosts, pPool

  if result['OK']:
    print "Finished directory %(Path)s, dirs: %(NumberOfDirectories)s, files: %(NumberOfFiles)s, replicas: %(NumberOfReplicas)s" % result['Value']
    print "%d active tasks remaining" % pPool.getNumWorkingProcesses()


    if "Directories" in result['Value']:
      for path in result['Value']['Directories']:
        random.shuffle(lfcHosts)
        #print pPool.getNumWorkingProcesses(), pPool.hasPendingTasks()
        print "Queueing task for directory %s, lfc %s" % (path,lfcHosts[0])
        result = pPool.createAndQueueTask( processDir,[path,False,lfcHosts[0]],callback=finalizeDirectory ) 
        if not result['OK']:
          print "Failed queueing", path
  else:
    print "Task failed: %s" % result['Message']
    if 'Path' in result:
      random.shuffle(lfcHosts)
      print "Requeueing task for directory %s, lfc %s" % (result['Path'],lfcHosts[0]) 

#########################################################################

pPool = ProcessPool(30,40,0)
#pPool.daemonize()

lfcHosts = ['lfc-lhcb-ro.cern.ch',
            'lfc-lhcb-ro.cr.cnaf.infn.it',
            'lhcb-lfc-fzk.gridka.de',
            'lfc-lhcb-ro.in2p3.fr',
            'lfc-lhcb.grid.sara.nl',
            'lfclhcb.pic.es',
            'lhcb-lfc.gridpp.rl.ac.uk']

#for user in users:
#  path = "/lhcb/user/%s/%s" % (user[0],user)
#  random.shuffle(lfcHosts)
#  print "Queueing user", user, pPool.getFreeSlots(),pPool.getNumWorkingProcesses(),pPool.hasPendingTasks(),pPool.getNumIdleProcesses(), lfcHosts[0]
#  result = pPool.createAndQueueTask( processDir,[path,True,lfcHosts[0]],callback=finalizeDirectory ) 
#  if not result['OK']:
#    print "Failed queueing", path


path = "/lhcb/LHCb"
print "Queueing task for directory", path, lfcHosts[0]
result = pPool.createAndQueueTask( processDir,[path,False,lfcHosts[0]],callback=finalizeDirectory ) 
if not result['OK']:
  print "Failed queueing", path

for i in range(20):
  pPool.processResults()
  time.sleep(1)

pPool.processAllResults()
