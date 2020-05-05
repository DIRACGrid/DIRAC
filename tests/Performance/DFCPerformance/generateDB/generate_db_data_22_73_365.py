"""This prints the SQL statements necessary to fill in a DB with approximately
    * directories: 22 M
    * files: 73M
    * replicas: 365M

   It also simulates production files (10000 per directory at depth 6) and user files (2 per directory at depth 12)
   This sums up to 73M files (I let you do the math)

   And we have between 3 and 7 replicas per files (random), which sums up to around 365M replicas.

"""
 

from __future__ import print_function
from __future__ import absolute_import
from __future__ import division
import os
import random
import config

# At each level, we have 4 subdirectories
# With a depth of 12, it makes 22 M directories
#4(1 + 4( 1 + 4(1+4(1+4(1+4(1+4(1+4(1+4(1+4(1+4(1+4)))))))))) ) = 22 369 620
d = [ 4 ]*12


checksumType = 'Adler32'
mode = 777
dirId = 2
fileId = 1
repId = 1
# we start from depth = 5 so we need an average of 2740 files per folder


print("SET FOREIGN_KEY_CHECKS = 0;")
print("SET UNIQUE_CHECKS = 0;")
print("SET AUTOCOMMIT = 0;")
print("START TRANSACTION;")

for uId in config.users:
  print("INSERT INTO FC_Users (UserName) values ('%s');" % (uId))
  
for gId in config.groups:
  print("INSERT INTO FC_Groups (GroupName) values ('%s');" % (gId))

for se in config.storageElements:
  print("INSERT INTO FC_StorageElements (SEName) values ('%s');" % (se))

for st in config.status:
  print("INSERT INTO FC_Statuses (Status) values ('%s');" % (st))




def proc_insert_dir(parent_id, child_name, UID, GID, Mode, Status):
  """ Print the insertion statement for Directory with the directory closure"""

  print("INSERT INTO FC_DirectoryList (Name, UID, GID, CreationDate, ModificationDate, Mode, Status)\
       values ('%s', %s, %s, UTC_TIMESTAMP(), UTC_TIMESTAMP(), %s, %s);" % (child_name, UID, GID, Mode, Status))
  print("SELECT LAST_INSERT_ID() INTO @dir_id;")
  
  print("INSERT INTO FC_DirectoryClosure (ParentID, ChildID, Depth ) VALUES (@dir_id, @dir_id, 0);")

  if parent_id:
    print("""INSERT INTO FC_DirectoryClosure(ParentID, ChildID, depth)
       SELECT p.ParentID, @dir_id, p.depth + 1
       FROM FC_DirectoryClosure p
       WHERE p.ChildID = %s;""" % parent_id)
    
    
def proc_insert_file(dir_id, size, UID, GID, status_id, filename, GUID, checksum, checksumtype, mode):
  """ print the insert file statement """

  print("INSERT INTO FC_Files (DirID, Size, UID, GID, Status, FileName, GUID, Checksum, CheckSumType,\
   CreationDate, ModificationDate, Mode) VALUES (%s, %s, %s, %s, %s, '%s', '%s', '%s', '%s',\
    UTC_TIMESTAMP(), UTC_TIMESTAMP(), %s);" % ( dir_id, size, UID, GID, status_id, filename,
                                                GUID, checksum, checksumtype, mode))

def proc_insert_replica(file_id, se_id, status_id, rep_type, pfn):
  """ Print the insert replica statement """

  print("INSERT INTO FC_Replicas (FileID, SEID, Status, RepType, CreationDate, ModificationDate, PFN)\
   VALUES (%s, %s, %s,'%s', UTC_TIMESTAMP(), UTC_TIMESTAMP(), '%s');" % (file_id, se_id, status_id, rep_type, pfn))

# Insert the root directory
proc_insert_dir(0, '/', 1, 1, 755, 1)

def loop ( index, cur, parentId ):
  global dirId
  global fileId
  global repId

  # Issue a commit every 10000 directory
  if (dirId % 10000) == 0:
    print("COMMIT;")
    print("START TRANSACTION;")

  if index >= len (d):
    return
  for i in xrange(d[index]):
    uid = random.randint(1, len(config.users) )
    gid = random.randint(1, len(config.groups) )
    next = cur + [i]
    dirName = '/%s'%('/'.join(map(str, next)))
    myDirId = dirId
    proc_insert_dir(parentId, dirName, uid, gid, 755, 1)
   
    nbFiles = 0
    # prod files, tot = 41M
    if len(next) == 6:
      nbFiles = 10000
    # user tot = 32M
    elif len(next) == 12:
      nbFiles = 2

     

    #generate files
    if nbFiles:
      for f in xrange(nbFiles):
        filename = "%s.txt"%(f)
        size = random.randint(1,1000)
        statusid = 2
        guid = "%s"%fileId
        checksum = guid
        proc_insert_file(myDirId, size, uid, gid, statusid, filename, guid, checksum, checksumType, mode)

        #Average of 5 
        nbRep = random.randrange(3,7)
        ses = random.sample(range(2, len(config.storageElements) + 2), nbRep)
        firstRep = True
        for seid in ses:
          statusid = random.randint(2, len(config.status) +1)
          rep_type = 'Replica'
          if firstRep:
            rep_type = 'Master'
            firstRep = False
          pfn = "%s_%s.rep"%(f, seid)
          proc_insert_replica(fileId, seid, statusid, rep_type, pfn)
          repId += 1
          
        fileId += 1      
 
    dirId += 1
    loop(index +1, next, myDirId )

loop(0,[], 1)

print("CALL ps_rebuild_directory_usage;")
print("COMMIT;")
print("SET FOREIGN_KEY_CHECKS = 1;")
print("SET UNIQUE_CHECKS = 1;")
print("SET AUTOCOMMIT = 1;")
