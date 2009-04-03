#!/usr/bin/env python
from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/DataManagementSystem/scripts/dirac-dms-check-file-integrity.py,v 1.3 2009/04/03 14:34:56 acsmith Exp $
########################################################################
__RCSID__   = "$Id: dirac-dms-check-file-integrity.py,v 1.3 2009/04/03 14:34:56 acsmith Exp $"
__VERSION__ = "$Revision: 1.3 $"

from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.Core.Utilities.List import sortList
from DIRAC.DataManagementSystem.Client.FileCatalog import FileCatalog
from DIRAC.DataManagementSystem.Client.StorageElement import StorageElement
import sys,os

if len(sys.argv) < 2:
  print 'Usage: dirac-dms-check-file-integrity <lfn | fileContainingLfns>'
  sys.exit()
else:
  inputFileName = sys.argv[1]

if os.path.exists(inputFileName):
  inputFile = open(inputFileName,'r')
  string = inputFile.read()
  lfns = string.splitlines()
  inputFile.close()
else:
  lfns = [inputFileName]

#########################################################################
# Get the state of the provided files in the BK
#########################################################################

bkClient = RPCClient('Bookkeeping/BookkeepingManager')
res = bkClient.getFileMetadata(lfns)
if not res['OK']:
  print 'Failed to retrieve file metadata from the BKK: %s' % res['Message']
  sys.exit(2)

bkMetadata = res['Value']
bkMissing = []
bkNoReplica = []
bkGood = []
for lfn in lfns:
  if not bkMetadata.has_key(lfn):
    bkMissing.append(lfn)
  else:
    if bkMetadata[lfn]['GotReplica'] != 'Yes':
      bkNoReplica.append(lfn) 
    else:
      bkGood.append(lfn)

print '\n################### %s ########################\n' % 'Bookkeeping contents'.center(20)
print '%s : %s' % ('Supplied files'.ljust(20),str(len(lfns)).ljust(20))
print '%s : %s' % ('With replicas'.ljust(20),str(len(bkGood)).ljust(20))
print '%s : %s' % ('Without replicas'.ljust(20),str(len(bkNoReplica)).ljust(20))
print '%s : %s' % ('Completely missing'.ljust(20),str(len(bkMissing)).ljust(20))
if bkNoReplica:
  print '\nThe following %s files are marked without replicas in the BK.' % len(bkNoReplica)
  for lfn in sortList(bkNoReplica):
    print lfn
if bkMissing:
  print '\nThe following %s files were missing from the BK completely.' % len(bkMissing)
  for lfn in sortList(bkMissing):
    print lfn
     
#########################################################################
# Check the files exist in the LFC
#########################################################################

lfcClient = FileCatalog('LcgFileCatalogCombined')
res = lfcClient.exists(lfns)
if not res['OK']:
  print res['Message']
  sys.exit()

bkGoodMissingFromLFC = []
bkNoReplicaMissingFromLFC = []
bkMissingMissingFromLFC = []
bkGoodPresentInLFC = []
bkNoReplicaPresentInLFC = []
bkMissingPresentInLFC = []

for lfn,exists in res['Value']['Successful'].items():
  if not exists:
    if lfn in bkGood:
      bkGoodMissingFromLFC.append(lfn)
    elif lfn in bkNoReplica:
      bkNoReplicaMissingFromLFC.append(lfn)
    elif lfn in bkMissing:
      bkMissingMissingFromLFC.append(lfn)
  else:
    if lfn in bkGood:
      bkGoodPresentInLFC.append(lfn)
    elif lfn in bkNoReplica:
      bkNoReplicaPresentInLFC.append(lfn)
    elif lfn in bkMissing:
      bkMissingPresentInLFC.append(lfn)

print '\n################### %s ########################\n' % 'LFC contents'.center(20)
print '%s : %s' % ('Supplied files'.ljust(20),str(len(lfns)).ljust(20))
print '%s : %s' % ('Exist in the LFC'.ljust(20),str(len(bkGoodPresentInLFC+bkNoReplicaPresentInLFC+bkMissingPresentInLFC)).ljust(20))
print '%s : %s' % ('Missing from LFC'.ljust(20),str(len(bkGoodMissingFromLFC+bkNoReplicaMissingFromLFC+bkMissingMissingFromLFC)).ljust(20))

if bkMissingPresentInLFC:
  print '\nThe following %s files were missing in the BK but present in the LFC.' % len(bkMissingPresentInLFC)
  for lfn in sortList(bkMissingPresentInLFC):
    print lfn
if bkNoReplicaPresentInLFC:
  print '\nThe following %s files were marked without replicas in teh BK but present in the LFC.' % len(bkNoReplicaPresentInLFC)
  for lfn in sortList(bkNoReplicaPresentInLFC):
    print lfn
if bkGoodMissingFromLFC:
  print '\nThe following %s files were good in the BK but missing from the LFC.' % len(bkGoodMissingFromLFC)
  for lfn in sortList(bkGoodMissingFromLFC):
    print lfn

#########################################################################
# Check the file metadata against the LFC 
#########################################################################
filesPresentInLFC = bkGoodPresentInLFC+bkNoReplicaPresentInLFC+bkMissingPresentInLFC
sizeMismatches = []
guidMismatches = []
zeroReplicaFiles = []
numberOfMissingReplicas = 0
numberOfBkSESizeMismatch = 0
pfnsLost = 0
pfnsUnavailable = 0
if filesPresentInLFC:
  res = lfcClient.getFileMetadata(filesPresentInLFC) 
  if not res['OK']:
    print res['Message']
    sys.exit()  

  for lfn,fileMetadata in res['Value']['Successful'].items():
    size = fileMetadata['Size']
    guid = fileMetadata['GUID']
    if bkMetadata[lfn]['FileSize'] != size:
      sizeMismatches.append(lfn)
    if bkMetadata[lfn]['GUID'] != guid:
      guidMismatches.append(lfn)

  print '\n################### %s ########################\n' % 'LFC metadata'.center(20)
  print '%s : %s' % ('Supplied files'.ljust(20),str(len(filesPresentInLFC)).ljust(20))
  print '%s : %s' % ('BK-LFC size mismatch'.ljust(20),str(len(sizeMismatches)).ljust(20))
  print '%s : %s' % ('BK-LFC guid mismatch'.ljust(20),str(len(guidMismatches)).ljust(20))
  if sizeMismatches:
    print 'The following %s files found with LFC-BK size mismatches.' % len(sizeMismatches)
    for lfn in sortList(sizeMismatches):
      print lfn
  if guidMismatches:
    print 'The following %s files found with LFC-BK guid mismatches.' % len(guidMismatches)
    for lfn in sortList(guidMismatches):
      print lfn

#########################################################################
# Check the location of the file replicas
#########################################################################

  res = lfcClient.getReplicas(filesPresentInLFC)
  if not res['OK']:
    print res['Message']
    sys.exit()

  sePfns = {}
  pfnLfns = {}
  for lfn,replicaDict in res['Value']['Successful'].items():
    if not replicaDict:
      zeroReplicaFiles.append(lfn)
    else:
      for se,pfn in replicaDict.items():
        if not sePfns.has_key(se):
          sePfns[se] = []
        sePfns[se].append(pfn)
        pfnLfns[pfn] = lfn

  print '\n################### %s ########################\n' % 'LFC Replicas'.center(20)
  print '%s : %s' % ('Supplied files'.ljust(20),str(len(filesPresentInLFC)).ljust(20))
  print '%s : %s' % ('Zero replica files'.ljust(20),str(len(zeroReplicaFiles)).ljust(20))
  if zeroReplicaFiles:
    print '\nThe following %s files found with zero replicas.\n' % len(zeroReplicaFiles)
    for lfn in sortList(zeroReplicaFiles):
      print lfn

  print '\n################### %s ########################\n' % 'SE Files'.center(20)
  if sePfns:
    print '%s %s' % ('Site'.ljust(20), 'Files'.rjust(20))
    for site in sortList(sePfns.keys()):
      files = len(sePfns[site])
      print '%s %s' % (site.ljust(20), str(files).rjust(20))

#########################################################################
# Check the physical files exist for all the replicas
#########################################################################

  missingReplicas = {}
  bkSESizeMismatch = {}
  sePfnsLost = {}
  sePfnsUnavailable = {}
  for se,pfns in sePfns.items():
    storageElement = StorageElement(se)
    res = storageElement.getFileMetadata(pfns)
    if not res['OK']:
      print 'Failed to get file sizes for pfns: %s' % res['Message']
    else:
      for pfn,reason in res['Value']['Failed'].items():
        if not missingReplicas.has_key(se):
          missingReplicas[se] = []
        missingReplicas[se].append(pfnLfns[pfn])
        numberOfMissingReplicas+=1
      for pfn,metadata in res['Value']['Successful'].items():
        size = metadata['Size']
        if not size == bkMetadata[pfnLfns[pfn]]['FileSize']:
          if not bkSESizeMismatch.has_key(se):
            bkSESizeMismatch[se] = []
          bkSESizeMismatch[se].append(pfnLfns[pfn])
          numberOfBkSESizeMismatch +=1
        if metadata['Lost']:
          if not sePfnsLost.has_key(se):
            sePfnsLost[se] = []
          sePfnsLost[se].append(pfnLfns[pfn])
          pfnsLost += 1
        if metadata['Unavailable']:
          if not sePfnsUnavailable.has_key(se):
            sePfnsUnavailable[se] = []
          sePfnsUnavailable[se].append(pfnLfns[pfn])
          pfnsUnavailable += 1

  print '\n################### %s ########################\n' % 'SE physical files'.center(20)
  if missingReplicas:
    print '\nThe following files were missing at %s SEs' % len(missingReplicas.keys())
    for se in sortList(missingReplicas.keys()):
      lfns = missingReplicas[se]
      print '%s : %s' % (se.ljust(10),str(len(lfns)).ljust(10))
      for lfn in sortList(lfns):
        print lfn

  if bkSESizeMismatch:
    print '\nThe following files had size mis-matches at %s SEs' % len(bkSESizeMismatch.keys())
    for se in sortList(bkSESizeMismatch.keys()):
      lfns = bkSESizeMismatch[se]
      print '%s : %s' % (se.ljust(10),str(len(lfns)).ljust(10))
      for lfn in sortList(lfns):
        print lfn

  if sePfnsLost:
    print '\nThe following files are reported lost by %s SEs' % len(sePfnsLost.keys())
    for se in sortList(sePfnsLost.keys()):
      lfns = sePfnsLost[se]
      print '%s : %s' % (se.ljust(10),str(len(lfns)).ljust(10))
      for lfn in sortList(lfns):
        print lfn

  if sePfnsUnavailable:
    print '\nThe following files are reported unavailable by %s SEs' % len(sePfnsUnavailable.keys())
    for se in sortList(sePfnsUnavailable.keys()):
      lfns = sePfnsUnavailable[se]
      print '%s : %s' % (se.ljust(10),str(len(lfns)).ljust(10))
      for lfn in sortList(lfns):
        print lfn

  if not (sePfnsUnavailable or sePfnsLost or missingReplicas or bkSESizeMismatch):
    print 'All registered replicas existed with the correct size and are accessible.'


print '\n################### %s ########################\n' % 'Summary'.center(20)

if bkMissing:
  print 'There were %s files completely missing from the BK.' % len(bkMissing)

if bkNoReplica:
  print 'There were %s files without replicas in the BK.' % len(bkNoReplica)

if bkMissingPresentInLFC:
  print 'There were %s files missing in the BK but present in the LFC.' % len(bkMissingPresentInLFC)

if bkNoReplicaPresentInLFC:   
  print 'There were %s files marked without replicas in the BK but present in the LFC.' % len(bkNoReplicaPresentInLFC)

if bkGoodMissingFromLFC:
  print 'There were %s files good in the BK but missing from the LFC.' % len(bkGoodMissingFromLFC)

if sizeMismatches:
  print 'There were %s files with mis-matched sizes in the BK and LFC.' % len(sizeMismatches)

if guidMismatches:
  print 'There were %s files with mis-matched guids in the BK and LFC.' % len(guidMismatches)

if zeroReplicaFiles:
  print 'There were %s files with zero replicas in the LFC.' % len(zeroReplicaFiles)

if numberOfMissingReplicas:
  print 'There were %s missing physical files.' % numberOfMissingReplicas

if numberOfBkSESizeMismatch:
  print 'There were %s physical files with mis-matched size in the BK and SE.' % numberOfBkSESizeMismatch

if pfnsLost:
  print 'There were %s physical files which are reported lost by the SE.' % pfnsLost

if pfnsUnavailable:
  print 'There were %s physical files which are reported unavailable by the SE.' % pfnsUnavailable
   
print '\n'
