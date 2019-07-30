"""
(This script is independent from lfc_dfc_copy)

This script is used to migrate the content of the LFC DB to the DFC DB when used with Stored procedure and Foreign keys.
It won't work with the other schema. It is the central component of the migration.
  * Please read the doc of each method in this script, there are important informations.
  * You need to have a "clean" LFC db. That is, the LFC does not enforce consistency, and for example a file
    can belong to a user which is not in the DB. This will not work in the DFC. Hence, you must make sure you
    have no inconsistencies what so ever in the LFC.
  * This script assumes that you already created the DFC DB, with the schema.
  * While performing the migration, the LFC must be in read-only. I am speaking here about the LFC service, not only
    in the DIRAC service. There should be *zero* insertion in the LFC or the DFC dbs while running this script.
    Read further the migration explanations for more details.
  * This script must be executed with a user that can execute the DIRAC method getUsernameForDN
    and getGroupsWithVOMSAttribute.
  * The script is doing 2 consistency checks at the end. One checks real inconsistencies within the DFC
   (see 'databaseIntegrityCheck'), while the second one compares the amount of entities in the LFC with respect
   to the DFC (see 'compareNumbers'). Please pay attention to the doc of these methods.
  * A final step is to be done once the migration is over. You need to call the procedure 'ps_rebuild_directory_usage'.
    Indeed, during the migration, the storage usage is not updated. So if you are happy with the report of the
    consistency checks, go for it.
  * I strongly recommend to do a snapshot of your LFC DB to try the migration script multiple times before hand

The global idea of the migration is as follow:
  * Put the LFC service in read-only mode (it doesn't harm, while write do...)
  * use this script to copy the data
  * Put again the LFC service in read-write, and set the LFC Write in the CS
  * The DFC should be put in Read-Write, so LFC and DFC will be in sync, but we
    read only from the DFC.
When after few weeks, you are sure that the migration was successful, get rid of the LFC.
Having 2 master catalogs is not a good idea...

As for the migration itself. Here are some tips and recommendations.
  * While it is possible to keep all the system running, we STRONGLY recommend to have a deep downtime.
  * Declare ahead of time the DFC DB and services in the CS.
  * One day before, start draining the system (avoid new jobs submission and so on).
  * Just before starting the migration:
      - Stop the FTSAgent and the RequestExecutingAgent.
      - Set the LFC service in read only
      - go for the deep downtime, stop all the agents basically. (Caution, don't stop the ReqManager service,
        it is important to have it !)
  * Perform the migration:
      - run this script
      - if happy with the result, rebuild the storage usage
  * Restarting phase:
    - Mark the LFC in Read only in the CS
    - Declare the DFC as Read-Write catalog and Master. Also, it should be the first master catalog.
    - start the DFC services
    - Hotfix the ReqManager (see bellow)
    - restart *all* the services that interact with the DMS, so that they pick up the new configuration
    - you can restart all the agents

If the migration is done this way:
  - the read action are never perturbed
  - the synchronous writes (with script) will fail
  - the jobs that are still running during the migration and that will attempt a write
    will end up creating a Request.

A hotfix in the ReqManager is necessary though. During the migration, some RMS Requests might have been created,
that concerns registration for the master catalog. For the jobs running during the migration, the master catalog
was only the DFC, however you want that when these Requests are executed, they are also done in the DFC.
The best for that is to hotfix the ReqManager, so that if an operation is targeted at the LFC, it will be modified to be
targeted on both the LFC and the DFC. Providing you called your LFC 'LcgFileCatalogCombined', the fix is to be put
in the Catalog setter of RequestManagementSystem/Client/Operation.py and would look something like:

    if value == "LcgFileCatalogCombined":
      value = "FileCatalog,LcgFileCatalogCombined"

Note that for jobs that already have the new configuration, they will create 2 operations, one targeted at the DFC, the other
one at the LFC. But with this fix, you will end up with one targeted at the DFC, the other one target at the LFC and the DFC.
However, it should do no harm. But remember it when debugging... Anyway, this is only useful for Requests that were created
during or before the downtime, or by job started with the old configuration. So a relatively limited amount of time...

GOOD LUCK !

"""
# source /afs/cern.ch/project/oracle/script/setoraenv.sh  setoraenv -s 11203
# source /afs/cern.ch/project/oracle/script/setoraenv.csh  setoraenv -s 11203

from __future__ import print_function
import time
import os
import multiprocessing
from datetime import datetime

import MySQLdb as mdb
import cx_Oracle  # pylint:disable=import-error


from DIRAC.Core.Base import Script
from functools import reduce
Script.parseCommandLine()

from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getUsernameForDN, getGroupsWithVOMSAttribute  # noqa


prodDbAccount = 'USERNAME'
prodDbAccountPassword = 'PASSWORD'
prodDbTNS = 'DB'

dfc_db_host = 'HOST'
dfc_db_port = 3306
dfc_db_user = 'USER'
dfc_db_passwd = 'PASSWORD'
dfc_db_name = 'FileCatalogDB'
dfc_unix_socket = '/opt/dirac/mysql/db/mysql.sock'

# Directory were all the logs are written
logDir = '/tmp/migration'


# Set this flag to True if you just want to test the beginning
# of the migration procedure, and see if things are actually
# being copied over
earlyStopEnabled = False
# How many files + directories you will insert before stopping
earlyStop = 10000


# We are not committing every insert for performance reasons
# This tells you how many queries each thread will execute before
# issuing a commit
commitThreshold = 10000


# These tables are used to convert the LFC files/replicas status
# To the DFC one.
# In the DFC DB, FakeStatus already exists, so these status
# start to be numbered at 2
dirac_status = ['AprioriGood', 'Trash', 'Removing', 'Probing']
lfc_dfc_status = {'P': dirac_status.index('Trash') + 2}


# This is a cache that contains the mapping between
# the uid found in the LFC to a dirac username
uid_name = {}


# Converts the mode to a posix mode permission
def S_IMODE(mode):
  return mode & 0o7777


# Utilities to make the distinction between files and dir from the LFC

def S_IFMT(mode):
  return mode & 0o170000


S_IFDIR = 0o040000


def isDir(mode):
  """ True if mode indicates a directory"""
  return S_IFMT(mode) == S_IFDIR


def getUsersGroupsAndSEs(queryQueue, name_id_se):
  """ This dumps the users, groups and SEs from the LFC, converts them into DIRAC group, and
      create the query to insert them into the DFC.

      WATCH OUT !!
      The DIRAC group is evaluated from the VOMS attribute, since this is what is used in the LFC.
      So if you have several DIRAC groups that have the same voms role, the assigned group will
      be the first one...

      :param queryQueue : queue in which to put the SQL statement to be executed against the DFC
      :param name_id_se : cache to be filled in with the mapping {seName:id in the DFC}

  """

  connection = cx_Oracle.Connection(prodDbAccount, prodDbAccountPassword, prodDbTNS, threaded=True)
  fromdbR_ = connection.cursor()
  fromdbR_.arraysize = 1000

  # First, fetch all the UID and DN from the LFC
  if fromdbR_.execute("SELECT USERID, USERNAME from CNS_USERINFO"):
    rows = fromdbR_.fetchall()
    for row in rows:

      uid = row[0]
      dn = row[1]

      name = uid_name.get(uid)
      # If the name is not in cache,
      if not name:

        # We do some mangling of the DN to try to have sensitive name whatever happen
        # We also include the UID in case the name is not unique, or several DN are associated
        namePart = 'name'
        idPart = ''
        dnSplit = dn.split('CN=')
        try:
          namePart = dnSplit[-1].replace("'", " ")
        except Exception as e:
          pass
        try:
          idPart = dnSplit[2].replace("/", "")
        except Exception as e:
          pass
        idPart += ' uid_%s' % uid
        name = "Unknown (%s %s)" % (namePart, idPart)

      # Now, we do try to convert the DN into a DIRAC username
      if "Unknown" in name:
        result = getUsernameForDN(dn)
        if result['OK']:
          name = result['Value']
        uid_name[uid] = name

    # Now we prepare the SQL statement to insert them into the DFC
    for uid in uid_name:
      username = uid_name[uid]
      queryQueue.put("INSERT INTO FC_Users(UID, UserName) values (%s, '%s');\n" % (uid, username))

  # Now, same principle on the group
  if fromdbR_.execute("SELECT GID, GROUPNAME from CNS_GROUPINFO"):
    rows = fromdbR_.fetchall()
    for row in rows:
      gid = row[0]
      gn = row[1]

      groupname = gn

      # CAUTION: as explained in the docstring, if multiple groups share the same voms role
      # we take the first one
      groups = getGroupsWithVOMSAttribute('/' + gn)

      if groups:
        groupname = groups[0]

      queryQueue.put("INSERT INTO FC_Groups(GID, GroupName) values (%s, '%s');\n" % (gid, groupname))

  # We build a cache that contains the mapping between the name and its ID in the DFC DB
  # The ID starts at 2 because ID=1 is taken by FakeSe
  seCounter = 2
  # Fetch the name from the LFC
  if fromdbR_.execute("select unique HOST from CNS_FILE_REPLICA"):
    rows = fromdbR_.fetchall()
    for row in rows:
      seName = row[0]
      # Populate the SE cache
      name_id_se[seName] = seCounter
      # Create the query for the DFC
      queryQueue.put("INSERT INTO FC_StorageElements(SEID, SEName) values (%s, '%s');\n" % (seCounter, seName))
      seCounter += 1

  # Also here we just insert all the statuses defined above
  for status in dirac_status:
    queryQueue.put("INSERT INTO FC_Statuses (Status) values ('%s');\n" % status)

  fromdbR_.close()
  connection.close()

  # Set the poison pill in the queue
  queryQueue.put(None)

  return


def getDirAndFileData(fileQueue, dirQueue, dirClosureQueryQueue):
  """
      This fetches the Files and Directories from the LFC, and prepare the necessary
      SQL statement to insert them into the DFC.
      The File and Directory ids are kept consistent between the LFC and the DFC.

      Warning: don't worry if this starts and takes ages before starting printing anything. It is because it is
               doing the SYS_CONNECT_BY_PATH query, which takes a long time to return

      :param fileQueue : queue in which to put the values to be inserted for the files
      :param dirQueue : queue in which to put the values to be inserted for the directories
      :param dirClosureQueryQueue : queue in which to put the insert statement for the directory closure table

  """

  print("getDirAndFileData start")
  startTime = time.time()
  stepCounter = 0
  fileCounter = 0
  dirCounter = 0

  connection = cx_Oracle.Connection(prodDbAccount, prodDbAccountPassword, prodDbTNS, threaded=True)
  fromdbR_ = connection.cursor()
  fromdbR_.arraysize = 1000

  # Parameters we query.
  # It is to be noted that Files and directories are stored in the same table in the LFC,
  # hence we query, and then we have to make the distinction
  directoryParameters = [
      'fileid',
      'parent_fileid',
      'guid',
      "SYS_CONNECT_BY_PATH(name, '/') path",
      'LEVEL lvl',
      'filemode',
      'nlink',
      'owner_uid',
      'gid',
      'filesize',
      'atime',
      'mtime',
      'ctime',
      'status',
      'csumtype',
      'csumvalue']
  directoryCommand = "SELECT %s from CNS_FILE_METADATA START WITH fileid=3 \
                    CONNECT BY NOCYCLE PRIOR  fileid=parent_fileid order by lvl asc,\
                     parent_fileid" % (','.join(directoryParameters))

  # These variables are the index of the various information in the tuples returned by Oracle
  CNS_FILE_MODE = directoryParameters.index('filemode')
  CNS_FILE_FILEID = directoryParameters.index('fileid')
  CNS_FILE_PARENTID = directoryParameters.index('parent_fileid')
  CNS_FILE_STATUS = directoryParameters.index('status')
  CNS_FILE_SIZE = directoryParameters.index('filesize')
  CNS_FILE_UID = directoryParameters.index('owner_uid')
  CNS_FILE_GID = directoryParameters.index('gid')
  CNS_FILE_NAME = directoryParameters.index("SYS_CONNECT_BY_PATH(name, '/') path")
  CNS_FILE_GUID = directoryParameters.index('guid')
  CNS_FILE_CHECKSUM = directoryParameters.index('csumvalue')
  CNS_FILE_CTIME = directoryParameters.index('ctime')
  CNS_FILE_MTIME = directoryParameters.index('mtime')

  if fromdbR_.execute(directoryCommand):
    done = False
    batchSize = 1000  # Size of the batch that are retrieved at once
    # The insertion is done in 2 times (part for the files, ther other for directories)
    while not done:
      print("getDirAndFileData step %s elapsed time %s dir %s files %s" %
            (stepCounter, time.time() - startTime, dirCounter, fileCounter))
      stepCounter += 1

      # If doing an early stop
      if earlyStopEnabled and (fileCounter + dirCounter) >= earlyStop:
        done = True

      rows = fromdbR_.fetchmany(batchSize)
      # We prepare list of tuples of values to be inserted at once
      fileTuple = []
      dirTuple = []

      # If nothing is returned anymore, we are done.
      if rows == []:
        done = True

      else:
        for row in rows:
          cns_file_mode = row[CNS_FILE_MODE]
          cns_file_id = row[CNS_FILE_FILEID]
          cns_parent_id = row[CNS_FILE_PARENTID]
          cns_uid = row[CNS_FILE_UID]
          cns_gid = row[CNS_FILE_GID]

          # we ignore the real / so that /grid becomes the new root
          if cns_parent_id == 0:
            continue

          # The LFC only stores the current path level, but the query above
          # returned a full path.
          # We want to get ride of the '/grid' that was mandatory in the LFC
          cns_name = cns_name = os.path.realpath(row[CNS_FILE_NAME])
          if cns_name.startswith('/grid/'):
            cns_name = cns_name[5:]
          elif cns_name == '/grid':
            cns_name = '/'
            cns_parent_id = 0

          # Convert the status from the LFC to the DFC.
          # Basically, 'P' becomes Trash, and the rest becomes 'APrioriGood'
          dfc_status = lfc_dfc_status.get(row[CNS_FILE_STATUS], 2)
          # Convert the LFC mode to posix mode for the DFC
          dfc_mode = S_IMODE(int(row[CNS_FILE_MODE]))

          # Creation and modification time.
          # (nb: I have doubts about how trustable are these dates...)
          dfc_cdate = datetime.utcfromtimestamp(row[CNS_FILE_CTIME]).strftime('%Y-%m-%d %H:%M:%S')
          dfc_mdate = datetime.utcfromtimestamp(row[CNS_FILE_MTIME]).strftime('%Y-%m-%d %H:%M:%S')

          # It's a file
          if not isDir(cns_file_mode):
            fileCounter += 1

            # Get the size
            cns_file_size = row[CNS_FILE_SIZE]

            # In the file table, we only store the actual file name, not the path
            dfc_name = os.path.basename(cns_name)

            # Insert the tuple of values to be inserted in the File table
            fileTuple.append(
                "(%s, %s, %s, %s, %s, %s, '%s','%s','%s','%s','%s','%s','%s',%s)" %
                (cns_file_id,
                 cns_parent_id,
                 cns_file_size,
                 cns_uid,
                 cns_gid,
                 dfc_status,
                 dfc_name,
                 row[CNS_FILE_GUID],
                    row[CNS_FILE_CHECKSUM],
                    'Adler32',
                    'File',
                    dfc_cdate,
                    dfc_mdate,
                    dfc_mode))

          else:
            dirCounter += 1

            # Insert the tuple of values to be inserted in the directory table
            dirTuple.append(
                "(%s,'%s',%s,%s,'%s','%s',%s,%s)" %
                (cns_file_id,
                 cns_name,
                 cns_uid,
                 cns_gid,
                 dfc_cdate,
                 dfc_mdate,
                 dfc_mode,
                 dfc_status))

            # Insert the statement in the directory closure table for the directory to itself
            sqlDirClosure = "INSERT INTO FC_DirectoryClosure \
                            (ParentID, ChildID, Depth )\
                            VALUES (%s,%s, 0);" % (cns_file_id, cns_file_id)
            dirClosureQueryQueue.put("%s\n" % sqlDirClosure)

            # If we have a parent, we must also insert all the hierarchy in teh closure table
            if cns_parent_id:
              sqlDirClosureSub = "INSERT INTO FC_DirectoryClosure \
                                  (ParentID, ChildID, depth) \
                                  SELECT p.ParentID, %s, p.depth  + 1 \
                                  FROM FC_DirectoryClosure p \
                                  WHERE p.ChildID = %s;" % (cns_file_id, cns_parent_id)
#               fileAndDirInsert.write( "%s\n" % sqlDirClosureSub )
              dirClosureQueryQueue.put("%s\n" % sqlDirClosureSub)

        print("getDirAndFileData put in queues file %s" % (len(fileTuple)))

        # Add the list of tuples to be inserted to the queues
        if fileTuple:
          fileQueue.put(fileTuple)
        if dirTuple:
          dirQueue.put(dirTuple)

  print("getDirAndFileData done. elapsed time %s dir %s files %s" % (time.time() - startTime, dirCounter, fileCounter))
  fromdbR_.close()
  connection.close()

  # Poison pills
  fileQueue.put(None)
  dirQueue.put(None)
  dirClosureQueryQueue.put(None)
  return


def getReplicaData(replicaQueue, name_id_se):
  """ This fetches the replicas from the LFC, and prepare the values to be inserted in the DFC
      The Replica ID is kept consistent between the LFC and the DFC.

      :param replicaQueue: queue in which we insert the tuples of values to be inserted
      :param name_id_se: mapping between {SEName:id in the DFC} that was built earlier

  """

  print("getReplicaData start")
  startTime = time.time()
  stepCounter = 0
  replicaCounter = 0
  # This is the ID we assign to the Replica
  # Not strictly necessary because of auto increment
  cns_rep_id = 1

  connection = cx_Oracle.Connection(prodDbAccount, prodDbAccountPassword, prodDbTNS, threaded=True)
  fromdbR_ = connection.cursor()
  fromdbR_.arraysize = 1000

  # Replica parameters we are interested in
  replicaParameters = ['FileId', 'NBACCESSES', 'ATIME', 'PTIME',
                       'Status', 'F_TYPE', 'POOLNAME', 'Host', 'FS', 'SFN',
                       'CTIME', 'LTIME', 'R_TYPE', 'SETNAME', 'XATTR']

  getReplicaCommand = "SELECT %s from CNS_FILE_REPLICA" % (','.join(replicaParameters))

  # These variables are the index of the parameters in the tuples
  # returned by the LFC
  CNS_REP_FILEID = replicaParameters.index('FileId')
  CNS_REP_STATUS = replicaParameters.index('Status')
  CNS_REP_SFN = replicaParameters.index('SFN')
  CNS_REP_CTIME = replicaParameters.index('CTIME')
  CNS_REP_MTIME = replicaParameters.index('ATIME')
  CNS_REP_HOST = replicaParameters.index('Host')

  if fromdbR_.execute(getReplicaCommand):
    done = False
    # Size of the batch that are retrieved and inserted at once
    batchSize = 1000

    while not done:
      print("getReplicaData step %s elapsed time %s replica %s" %
            (stepCounter, time.time() - startTime, replicaCounter))
      stepCounter += 1

      # If early stop enabled
      if earlyStopEnabled and (replicaCounter) >= earlyStop:
        done = True

      # list of tuples that will be inserted at once
      repTuple = []

      rows = fromdbR_.fetchmany(batchSize)

      # If there is nothing else returned, we are done
      if rows == []:
        done = True
      else:
        for row in rows:
          replicaCounter += 1

          # Get the SSE ID
          dfc_rep_seId = name_id_se.get(row[CNS_REP_HOST])
          # Since the file id were kept consistent, we keep the same
          cns_file_id = row[CNS_REP_FILEID]

          # Conversion of the status, just for the file
          dfc_status = lfc_dfc_status.get(row[CNS_REP_STATUS], 2)

          # Modification time
          dfc_mdate = datetime.utcfromtimestamp(row[CNS_REP_MTIME]).strftime('%Y-%m-%d %H:%M:%S')

          # Creation time
          # sometimes, row[CNS_REP_CTIME]  is None
          try:
            dfc_cdate = datetime.utcfromtimestamp(row[CNS_REP_CTIME]).strftime('%Y-%m-%d %H:%M:%S')
          except BaseException:
            dfc_cdate = dfc_mdate

          # The PFN (even if arguably useful now...)
          cns_rep_sfn = row[CNS_REP_SFN]

          # Add the values to the list of tuple
          repTuple.append("(%s,%s,%s,%s,'%s','%s','%s')" % (cns_rep_id, cns_file_id, dfc_rep_seId, dfc_status,
                                                            dfc_cdate, dfc_mdate, cns_rep_sfn))
          cns_rep_id += 1

        print("getReplicaData add replicaQueue %s" % (len(repTuple)))

        # Add the values to be inserted
        if repTuple:
          replicaQueue.put(repTuple)

  print("getReplicaData done. elapsed time %s replicas %s" % (time.time() - startTime, replicaCounter))

  fromdbR_.close()
  connection.close()

  # Poison pill
  replicaQueue.put(None)
  return


def loadDataInMySQL(queryQueue, workerId):
  """ This generic method just executes whatever SQL query is put in the queue.
      It stops when getting the poison pill (None).
      The execution is done in transactions, and we disable to foreign key checking

      :param queryQueue: queue that contains the query
      :param workerID : whatever name/id for prints
  """

  con = mdb.connect(host=dfc_db_host, port=dfc_db_port, user=dfc_db_user,
                    passwd=dfc_db_passwd, db=dfc_db_name, unix_socket=dfc_unix_socket)
  cur = con.cursor()
  con.autocommit(False)
  cur.execute("START TRANSACTION;")
  cur.execute("SET FOREIGN_KEY_CHECKS = 0;")

  queryExecuted = 0

  logfile = open(os.path.join(logDir, "worker%s.txt" % workerId), 'w')

  while True:
    next_query = queryQueue.get()

    # If we go the poison pill,
    # commit all, set back the foreign key check
    # close the connections, and bye bye
    if next_query is None:
      cur.execute("COMMIT;")
      cur.execute("SET FOREIGN_KEY_CHECKS = 1;")
      con.commit()
      con.autocommit(True)
      cur.close()
      con.close()
      print("loadDataInMySQL EXITING ")
      logfile.write("loadDataInMySQL EXITING ")
      logfile.close()
      return

    # If we reach the threshold, we commit
    if queryExecuted % commitThreshold == 0:
      cur.execute("COMMIT;")
      con.commit()
      cur.execute("START TRANSACTION;")

    # We try to execute the query
    # If ever it fail for a reason or another, we try again
    # If it fails gain, we ignore this query
    try:
      cur.execute(next_query)
    except Exception as e:
      print("worker %s : EXCEPTION %s\nworker %s :QUERY %s" % (workerId, e, workerId, next_query))
      print("worker %s trying again " % workerId)

      logfile.write("worker %s : EXCEPTION %s\nworker %s :QUERY %s" % (workerId, e, workerId, next_query))
      logfile.write("worker %s trying again " % workerId)

      try:
        cur.execute(next_query)
      except Exception as ee:
        print("worker %s COMPLETELY FAILED " % workerId)
        print("worker %s : EXCEPTION %s\nworker %s :QUERY %s" % (workerId, e, workerId, next_query))
        logfile.write("worker %s COMPLETELY FAILED " % workerId)
        logfile.write("worker %s : EXCEPTION %s\nworker %s :QUERY %s" % (workerId, e, workerId, next_query))

    queryExecuted += 1
    # Tell we are done with this task
    queryQueue.task_done()
    if queryExecuted % 10000 == 0:
      print("worker %s (%s) : %s" % (workerId, queryExecuted, next_query))
      logfile.write("worker %s (%s) : %s" % (workerId, queryExecuted, next_query))

  logfile.close()


def loadTupleDataInMySQL(queryQueue, workerId, querybase):
  """ This generic method executes an insert query given in querybase argument
      using the values given in the queryQueue.
      The execution is done in transactions, and we disable to foreign key checking

      :param queryQueue: queue that contains the values as a list of string tuples (e.g. [ '(1,2)', '(3,4)'])
      :param workerID : whatever name/id for prints
      :param querybase: base of the insert sql statement (e.g. "INSERT INTO myTable (x,y) values ")
  """

  con = mdb.connect(host=dfc_db_host, port=dfc_db_port, user=dfc_db_user,
                    passwd=dfc_db_passwd, db=dfc_db_name, unix_socket=dfc_unix_socket)
  cur = con.cursor()
  cur.execute("SET FOREIGN_KEY_CHECKS = 0;")

  queryExecuted = 0

  logfile = open(os.path.join(logDir, "worker%s.txt" % workerId), 'w')

  workerStart = time.time()

  while True:
    next_tuple = queryQueue.get()

    # If we get the poison pill, clean and exit
    if next_tuple is None:
      print("worker %s : got poison pill. elapsed time %s" % (workerId, time.time() - workerStart))
      logfile.write("worker %s : got poison pill. elapsed time %s\n" % (workerId, time.time() - workerStart))

      cur.execute("SET FOREIGN_KEY_CHECKS = 1;")
      cur.close()
      con.close()
      print("loadDataInMySQL %s EXITING " % workerId)
      logfile.write("loadDataInMySQL %s EXITING " % workerId)
      logfile.close()
      return

    print("worker %s : got %s" % (workerId, len(next_tuple)))
    logfile.write("worker %s : got %s\n" % (workerId, len(next_tuple)))

    # Build the query and execute
    # If it fails, try again
    # If it fails again, forget about that one
    try:
      next_query = querybase + ','.join(next_tuple) + ';'
      cur.execute(next_query)
      con.commit()
    except Exception as e:
      print("worker %s : EXCEPTION %s\nworker %s :QUERY %s" % (workerId, e, workerId, next_query))
      print("worker %s trying again " % workerId)

      logfile.write("worker %s : EXCEPTION %s\nworker %s :QUERY %s" % (workerId, e, workerId, next_query))
      logfile.write("worker %s trying again " % workerId)

      try:
        cur.execute(next_query)
        con.commit()
      except Exception:
        print("worker %s COMPLETELY FAILED " % workerId)
        print("worker %s : EXCEPTION %s " % (workerId, e))
        logfile.write("worker %s COMPLETELY FAILED " % workerId)
        logfile.write("worker %s : EXCEPTION %s\nworker %s :QUERY %s" % (workerId, e, workerId, next_query))

    queryExecuted += len(next_tuple)
    queryQueue.task_done()
    if queryExecuted % 10000 == 0:
      print("worker %s (%s) elapsed time %s" % (workerId, queryExecuted, time.time() - workerStart))
      logfile.write("worker %s (%s) elapsed time %s " % (workerId, queryExecuted, time.time() - workerStart))

  logfile.close()


# Admin has ID 0 in LFC, 1 in DFC
def updateAdminID():
  """ The 'root'/admin has an ID 0 in the LFC but 1 in the DFC. So we make the update.
  """
  con = mdb.connect(host=dfc_db_host, port=dfc_db_port, user=dfc_db_user,
                    passwd=dfc_db_passwd, db=dfc_db_name, unix_socket=dfc_unix_socket)
  cur = con.cursor()

  workerId = "updateAdminID"

  logfile = open(os.path.join(logDir, "worker%s.txt" % workerId), 'w')

  updateQueries = {"Update FC_Files": "Update FC_Files set UID = 1, GID = 1 where UID = 0 and GID = 0",
                   "Update FC_DirectoryList":
                   "Update FC_DirectoryList set UID = 1, GID = 1 where UID = 0 and GID = 0"}

  for desc, query in updateQueries.items():
    print("worker %s : %s (%s)" % (workerId, desc, query))
    logfile.write("worker %s : %s (%s)\n" % (workerId, desc, query))

    cur.execute(query)
    con.commit()

  cur.close()
  con.close()
  print("updateAdminID EXITING ")
  logfile.write("updateAdminID EXITING ")
  logfile.close()
  return


def databaseIntegrityCheck():
  """ This does some integrity check:
        * If some users have no files/directories (expect some...)
        * If some files belong to non existing users (should not be !)
        * If some directories belong to non existing users (should not be !)
        * If some groups have no files/directories (expect some...)
        * If some files belong to non existing groups (should not be !)
        * If some directories belong to non existing groups (should not be !)
      It just prints the output, does not take action.
  """
  con = mdb.connect(host=dfc_db_host, port=dfc_db_port, user=dfc_db_user,
                    passwd=dfc_db_passwd, db=dfc_db_name, unix_socket=dfc_unix_socket)
  cur = con.cursor()

  workerId = "dbCheck"

  logfile = open(os.path.join(logDir, "worker%s.txt" % workerId), 'w')

  integrityQueries = {"Useless Users": " SELECT u.* from FC_Users u\
                                           LEFT OUTER JOIN\
                                               (select distinct(UID)\
                                                From FC_DirectoryList\
                                                UNION\
                                                select distinct(UID)\
                                                from FC_Files) i\
                                           ON u.UID = i.UID\
                                           WHERE i.UID IS NULL",
                      "Files with Non existing users": "SELECT f.*\
                                                          from FC_Files f\
                                                          LEFT OUTER JOIN\
                                                          FC_Users u\
                                                          ON f.UID = u.UID\
                                                          WHERE u.UID IS NULL",
                      "Directories with non existing users": "SELECT d.*\
                                                               FROM FC_DirectoryList d\
                                                               LEFT OUTER JOIN\
                                                               FC_Users u\
                                                               ON d.UID = u.UID\
                                                               WHERE u.UID IS NULL",
                      "Useless groups": "SELECT g.*\
                                          FROM FC_Groups g\
                                          LEFT OUTER JOIN\
                                             (SELECT distinct(GID)\
                                              FROM FC_DirectoryList\
                                              UNION\
                                              SELECT distinct(GID)\
                                              FROM FC_Files) i\
                                          ON g.GID = i.GID\
                                          WHERE i.GID IS NULL",

                      "Files with non existing groups": "SELECT f.*\
                                                           FROM FC_Files f\
                                                           LEFT OUTER JOIN\
                                                           FC_Groups g\
                                                           ON f.GID = g.GID\
                                                           WHERE g.GID IS NULL",

                      "Directories with non existing groups": "SELECT d.*\
                                                                 FROM FC_DirectoryList d\
                                                                 LEFT OUTER JOIN\
                                                                 FC_Groups g\
                                                                 ON d.GID = g.GID\
                                                                 WHERE g.GID IS NULL"

                      }

  for desc, query in integrityQueries.items():
    print("worker %s : %s (%s)" % (workerId, desc, query))
    logfile.write("worker %s : %s (%s)\n" % (workerId, desc, query))

    cur.execute(query)

    rows = cur.fetchall()

    for row in rows:
      print("\t%s" % (row, ))
      logfile.write("\t%s\n" % (row, ))

  cur.close()
  con.close()
  print("databaseIntegrityCheck EXITING ")
  logfile.write("databaseIntegrityCheck EXITING ")
  logfile.close()
  return


def compareNumbers():
  """ This compares how many entries you have in the LFC and in the DFC.
      In principle, there should be the same, but there might be reasons why they are
      not, and with no reasons to worry... trust the other consistency check more!
      (n.b. : it typically happens when you have 2 replicas for in the same SE for the
      same file, but with a different 'SFN'. The LFC allowed that... They will now be only
      one left, the first, but it doesn't matter, since DIRAC recompute the URL anyway )
  """

  con = mdb.connect(host=dfc_db_host, port=dfc_db_port, user=dfc_db_user,
                    passwd=dfc_db_passwd, db=dfc_db_name, unix_socket=dfc_unix_socket)
  cur = con.cursor()

  connection = cx_Oracle.Connection(prodDbAccount, prodDbAccountPassword, prodDbTNS, threaded=True)
  fromdbR_ = connection.cursor()
  fromdbR_.arraysize = 1000

  lfc_filesDir = 0
  lfc_replicas = 0
  dfc_files = 0
  dfc_dir = 0
  dfc_replicas = 0

  if fromdbR_.execute("SELECT count(*) from CNS_FILE_METADATA"):
    rows = fromdbR_.fetchall()
    print("rows lfc files %s" % (rows, ))
    lfc_filesDir = rows[0][0]

  if fromdbR_.execute("SELECT count(*) from CNS_FILE_REPLICA"):
    rows = fromdbR_.fetchall()
    print("rows lfc replicas %s" % (rows, ))
    lfc_replicas = rows[0][0]

  cur.execute("SELECT count(*) from FC_Files")
  rows = cur.fetchall()
  print("rows dfc files %s" % (rows, ))
  dfc_files = rows[0][0]

  cur.execute("SELECT count(*) from FC_DirectoryList")
  rows = cur.fetchall()
  print("rows dfc dir %s" % (rows, ))
  dfc_dir = rows[0][0]

  cur.execute("SELECT count(*) from FC_Replicas")
  rows = cur.fetchall()
  print("rows dfc replicas %s" % (rows, ))
  dfc_replicas = rows[0][0]

  allCounters = [lfc_filesDir, lfc_replicas, dfc_files, dfc_dir, dfc_replicas]

  for counter in allCounters:
    if counter:
      print("OK")
    else:
      print("EMPTY COUNTER")

  if lfc_filesDir != (dfc_files + dfc_dir + 1):  # /grid folder
    print("ERROR !  lfc_filesDir != (dfc_files + dfc_dir + 1) %s != %s" % (lfc_filesDir, dfc_files + dfc_dir + 1))

  if lfc_replicas != dfc_replicas:
    print("ERROR ! lfc_replicas != dfc_replicas %s != %s" % (lfc_replicas, dfc_replicas))

  cur.close()
  con.close()
  fromdbR_.close()
  connection.close()

  return


if __name__ == '__main__':

  startTime = time.time()
  manager = multiprocessing.Manager()

  # Mapping between SEnames and its id in the DFC.
  name_id_se = manager.dict()

  # List of queues we are going to use
  queueTab = []

  # Queue for Users, Groups and SE
  # We don't add it in the queue tab because
  # we need this one to be finished before starting the others
  ugsQueryQueue = multiprocessing.JoinableQueue()

  # Queue for the file
  fileQueryQueue = multiprocessing.JoinableQueue()
  queueTab.append(fileQueryQueue)

  # Queue for the directory
  dirQueryQueue = multiprocessing.JoinableQueue()
  queueTab.append(dirQueryQueue)

  # Queue for the directory closure
  dirClosureQueryQueue = multiprocessing.JoinableQueue()
  queueTab.append(dirClosureQueryQueue)

  # Queue for the replicas
  replicaQueryQueue = multiprocessing.JoinableQueue()
  queueTab.append(replicaQueryQueue)

  print("Starting the Worker processes")

  # process to execute the query for Users, Groups and SEs
  workerUgsProcess = multiprocessing.Process(target=loadDataInMySQL, args=(ugsQueryQueue, 'ugs'))
  workerUgsProcess.start()

  # process to insert the values for Files
  fileBaseQuery = "INSERT IGNORE INTO FC_Files (FileID, DirID, Size, UID, GID, Status,\
   Filename, GUID, Checksum, ChecksumType, Type, CreationDate, ModificationDate, Mode) Values "
  workerFileProcess = multiprocessing.Process(target=loadTupleDataInMySQL, args=(fileQueryQueue, 'file', fileBaseQuery))
  workerFileProcess.start()

  # process to insert the values for Directories
  dirBaseQuery = "INSERT INTO FC_DirectoryList (DirID, Name, UID,GID, CreationDate,\
   ModificationDate, Mode, Status)  values "
  workerDirProcess = multiprocessing.Process(target=loadTupleDataInMySQL, args=(dirQueryQueue, 'dir', dirBaseQuery))
  workerDirProcess.start()

  # process to execute the query for directory closure
  workerDirClosureProcess = multiprocessing.Process(target=loadDataInMySQL, args=(dirClosureQueryQueue, 'dirClosure'))
  workerDirClosureProcess.start()

  # process to insert the values for Replicas
  # CAUTION: note the IGNORE statement...
  # This saves us from the double replica on a singel SE
  replicaBaseQuery = "INSERT IGNORE INTO FC_Replicas (RepID, FileID, SEID, Status,\
   CreationDate, ModificationDate, PFN) values "
  workerReplicaProcess = multiprocessing.Process(
      target=loadTupleDataInMySQL, args=(
          replicaQueryQueue, 'replica', replicaBaseQuery))
  workerReplicaProcess.start()

  # First we get dump the Users, Groups and SEs

  print("Worker processes started")
  print("Starting the ugsDumpProcess")
  ugsDumpProcess = multiprocessing.Process(target=getUsersGroupsAndSEs, args=(ugsQueryQueue, name_id_se))
  ugsDumpProcess.start()

  print("ugsDumpProcess started")
  print("Waiting for ugsDumpProcess to join")
  ugsDumpProcess.join()
  print("ugsDumpProcess joined")

  print("Waiting for empty ugs query queue")
  # We have to wait to fill in all the caches
  while not ugsQueryQueue.empty():
    print("ugs queue not empty %s" % (ugsQueryQueue.qsize()))
    time.sleep(5)

  # Now we go :-)
  # We start getting and inserting in parallel files, directories and replicas

  print("Queue is empty")
  print("Start fileDumpProcess")

  fileDumpProcess = multiprocessing.Process(target=getDirAndFileData,
                                            args=(fileQueryQueue,
                                                  dirQueryQueue, dirClosureQueryQueue))
  fileDumpProcess.start()

  print("fileDumpProcess started")
  print("Start replicaDumpProcess")

  replicaDumpProcess = multiprocessing.Process(target=getReplicaData, args=(replicaQueryQueue, name_id_se))
  replicaDumpProcess.start()

  print("replicaDumpProcess started")

  print("joining fileDumpProcess")
  fileDumpProcess.join()
  print("fileDumpProcess joined")
  print("joining replicaDumpProcess")
  replicaDumpProcess.join()
  print("replicaDumpProcess joined")

  # We wait till everything is empty
  print("Waiting for empty query queue")
  allEmpty = False
  # We have to wait to fill in all the caches
  while not allEmpty:
    time.sleep(5)
    emptyTab = [q.empty() for q in queueTab]
    allEmpty = reduce(lambda x, y: x and y, emptyTab, True)
    print("Queues not empty %s " % ([q.qsize() for q in queueTab]))

  print("queues should be empty  %s" % ([q.empty() for q in queueTab]))

  print("finished (before join...). Total time : %s" % (time.time() - startTime))

  # Here we are done
  # Joining the process

  print("joining worker processes")
  print("file worker")
  workerFileProcess.join()
  print("dir worker")
  workerDirProcess.join()
  print("dirClosure worker")
  workerDirClosureProcess.join()
  print("replica worker")
  workerReplicaProcess.join()

  # Translating admin ID from 0 to 1
  print("Updating Admin ID")
  updateIDStart = time.time()
  updateAdminID()
  updateIDTime = time.time() - updateIDStart

  print("Finished updating AdminID in %s" % updateIDTime)

  # Doing the integrity check

  print("doing integrity check")
  integrityStart = time.time()
  databaseIntegrityCheck()
  integrityTime = time.time() - integrityStart

  print("Finished integrity check in %s" % integrityTime)

  # Comparing the numbers
  print("doing number compare  check")
  numberCompareStart = time.time()
  compareNumbers()
  numberCompareTime = time.time() - numberCompareStart

  print("Finished comparing number in %s" % numberCompareTime)

  # To be clean, we try to join the queues
  # but here kicks the mystery of queues and multiprocess
  # in python, so it never ends

  print("joining queues")
  print("file queue")
  fileQueryQueue.join()

  print("replicas queue")
  replicaQueryQueue.join()
  print("queue joined")
