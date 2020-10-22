""" WATCH OUT !!! TOTALLY UNTESTED ! I just slightly modified an existing script that was working to make it
    more generic, but I did not test it
   This prints the SQL statements necessary to fill in a DB using the config file numbers
   It also simulates production files and user files by putting the two types at a different depth
   and with a different amount of files per directories.


"""


from __future__ import print_function
from __future__ import absolute_import
from __future__ import division
import os
import random
import config  # pylint: disable=import-error


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




def proc_insert_dir( parent_id, child_name, UID, GID, Mode, Status ):
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


def proc_insert_file( dir_id, size, UID, GID, status_id, filename, GUID, checksum, checksumtype, mode ):
  """ print the insert file statement """

  print("INSERT INTO FC_Files (DirID, Size, UID, GID, Status, FileName, GUID, Checksum, CheckSumType,\
   CreationDate, ModificationDate, Mode) VALUES (%s, %s, %s, %s, %s, '%s', '%s', '%s', '%s',\
    UTC_TIMESTAMP(), UTC_TIMESTAMP(), %s);" % ( dir_id, size, UID, GID, status_id, filename,
                                                GUID, checksum, checksumtype, mode))

def proc_insert_replica( file_id, se_id, status_id, rep_type, pfn ):
  """ Print the insert replica statement """

  print("INSERT INTO FC_Replicas (FileID, SEID, Status, RepType, CreationDate, ModificationDate, PFN)\
   VALUES (%s, %s, %s,'%s', UTC_TIMESTAMP(), UTC_TIMESTAMP(), '%s');" % (file_id, se_id, status_id, rep_type, pfn))

# Insert the root directory
proc_insert_dir( 0, '/', 1, 1, 755, 1 )

def loop ( index, cur, parentId ):
  global dirId
  global fileId
  global repId

  # Issue a commit every 10000 directory
  if ( dirId % 10000 ) == 0:
    print("COMMIT;")
    print("START TRANSACTION;")

  if index >= len ( config.hierarchySize ):
    return
  for i in range(config.hierarchySize[index]):
    uid = random.randint( 1, len( config.users ) )
    gid = random.randint( 1, len( config.groups ) )
    next = cur + [i]
    dirName = '/%s' % ( '/'.join( map( str, next ) ) )
    myDirId = dirId
    proc_insert_dir( parentId, dirName, uid, gid, 755, 1 )

    nbFiles = 0
    # prod files, tot = 41M
    if len( next ) == config.prodFileDepth:
      nbFiles = config.prodFilesPerDir
    # user tot = 32M
    elif len( next ) == config.userFileDepth:
      nbFiles = config.userFilesPerDir



    # generate files
    if nbFiles:
      for f in range(nbFiles):
        filename = "%s.txt" % ( f )
        size = random.randint( 1, 1000 )
        statusid = 2
        guid = "%s" % fileId
        checksum = guid
        proc_insert_file( myDirId, size, uid, gid, statusid, filename, guid, checksum, checksumType, mode )

        # Generate replicas
        nbRep = random.randrange( config.minReplicasPerFile, config.maxReplicasPerFile )
        ses = random.sample( xrange( 2, len( config.storageElements ) + 2 ), nbRep )
        firstRep = True
        for seid in ses:
          statusid = random.randint( 2, len( config.status ) + 1 )
          rep_type = 'Replica'
          if firstRep:
            rep_type = 'Master'
            firstRep = False
          pfn = "%s_%s.rep" % ( f, seid )
          proc_insert_replica( fileId, seid, statusid, rep_type, pfn )
          repId += 1

        fileId += 1

    dirId += 1
    loop( index + 1, next, myDirId )

loop( 0, [], 1 )

print("CALL ps_rebuild_directory_usage;")
print("COMMIT;")
print("SET FOREIGN_KEY_CHECKS = 1;")
print("SET UNIQUE_CHECKS = 1;")
print("SET AUTOCOMMIT = 1;")
