-- $HeaderURL $
-- ------------------------------------------------------------------------------
DROP DATABASE IF EXISTS FileCatalogDB;
-- ------------------------------------------------------------------------------
CREATE DATABASE FileCatalogDB;

use mysql;
-- options to set in the db
-- transaction-isolation = READ-COMMITTED


GRANT SELECT,INSERT,LOCK TABLES,CREATE TEMPORARY TABLES,UPDATE,DELETE,CREATE,DROP,ALTER,REFERENCES ON FileCatalogDB.* TO Dirac@'%';
GRANT ALTER ROUTINE, CREATE ROUTINE, EXECUTE ON FileCatalogDB.* TO  Dirac@'%';
GRANT TRIGGER ON FileCatalogDB.* TO  Dirac@'%';

GRANT SELECT,INSERT,LOCK TABLES,CREATE TEMPORARY TABLES,UPDATE,DELETE,CREATE,DROP,ALTER,REFERENCES ON FileCatalogDB.* TO Dirac@'localhost';
GRANT ALTER ROUTINE, CREATE ROUTINE, EXECUTE ON FileCatalogDB.* TO  Dirac@'localhost';
GRANT TRIGGER ON FileCatalogDB.* TO  Dirac@'localhost';


FLUSH PRIVILEGES;

USE FileCatalogDB;
SET FOREIGN_KEY_CHECKS = 0;

-- ------------------------------------------------------------------------------

CREATE TABLE FC_Statuses (
    StatusID INT AUTO_INCREMENT,
    Status VARCHAR(32),

    PRIMARY KEY (StatusID),

    UNIQUE(Status)
) ENGINE = INNODB;

INSERT INTO FC_Statuses (StatusID, Status) values (1, 'FakeStatus');

-- -----------------------------------------------------------------------------


CREATE TABLE FC_StorageElements (
    SEID INTEGER AUTO_INCREMENT,
    SEName VARCHAR(127) CHARACTER SET latin1 COLLATE latin1_bin NOT NULL,
    AliasName VARCHAR(127) DEFAULT '',

    PRIMARY KEY (SEID),

    UNIQUE (SEName)
) ENGINE = INNODB;

INSERT INTO FC_StorageElements (SEID, SEName) values (1, 'FakeSE');

-- ------------------------------------------------------------------------------


CREATE TABLE FC_Groups (
    GID INTEGER NOT NULL AUTO_INCREMENT,
    GroupName VARCHAR(127) NOT NULL,

    PRIMARY KEY (GID),

    UNIQUE (GroupName)
) ENGINE = INNODB;

INSERT INTO FC_Groups (GID, GroupName) values (1, 'root');

-- ------------------------------------------------------------------------------

CREATE TABLE FC_Users (
    UID INTEGER NOT NULL AUTO_INCREMENT,
    UserName VARCHAR(127) NOT NULL,

    PRIMARY KEY (UID),

    UNIQUE (UserName)
) ENGINE = INNODB;

INSERT INTO FC_Users (UID, UserName) values (1, 'root');
-- ------------------------------------------------------------------------------



--
-- create table FC_DirectoryList (
--   DirID INT NOT NULL AUTO_INCREMENT,
--   Name varchar(255)CHARACTER SET latin1 COLLATE latin1_bin NOT NULL,
--
--   PRIMARY KEY (DirID),
--
--   UNIQUE (Name)
-- ) ENGINE=INNODB;
--
--
--
--
--
-- CREATE TABLE FC_DirectoryInfo (
--     DirID INTEGER NOT NULL,
--     UID INTEGER NOT NULL DEFAULT 0,
--     GID INTEGER NOT NULL DEFAULT 0,
--     CreationDate DATETIME,
--     ModificationDate DATETIME,
--     Mode SMALLINT UNSIGNED NOT NULL DEFAULT 775,
--     Status INTEGER NOT NULL DEFAULT 0,
--
--     PRIMARY KEY (DirID),
--     FOREIGN KEY (DirID) REFERENCES FC_DirectoryList(DirID) ON DELETE CASCADE,
--     FOREIGN KEY (UID) REFERENCES FC_Users(UID),
--     FOREIGN KEY (GID) REFERENCES FC_Groups(GID),
--     FOREIGN KEY (Status) REFERENCES FC_Statuses(StatusID)
-- ) ENGINE = INNODB;



create table FC_DirectoryList (
  DirID INT NOT NULL AUTO_INCREMENT,
  UID INTEGER NOT NULL DEFAULT 0,
  GID INTEGER NOT NULL DEFAULT 0,
  CreationDate DATETIME,
  ModificationDate DATETIME,
  Mode SMALLINT UNSIGNED NOT NULL DEFAULT 775,
  Status INTEGER NOT NULL DEFAULT 0,
  Name varchar(255)CHARACTER SET latin1 COLLATE latin1_bin NOT NULL,

  PRIMARY KEY (DirID),
  FOREIGN KEY (UID) REFERENCES FC_Users(UID),
  FOREIGN KEY (GID) REFERENCES FC_Groups(GID),
  FOREIGN KEY (Status) REFERENCES FC_Statuses(StatusID),
  UNIQUE (Name)
) ENGINE=INNODB;



-- ------------------------------------------------------------------------------

CREATE TABLE FC_DirectoryClosure (
 ClosureID INT NOT NULL AUTO_INCREMENT,
 ParentID INT NOT NULL,
 ChildID INT NOT NULL,
 Depth INT NOT NULL DEFAULT 0,

 PRIMARY KEY (ClosureID),
 FOREIGN KEY (ParentID) REFERENCES FC_DirectoryList(DirID) ON DELETE CASCADE,
 FOREIGN KEY (ChildID) REFERENCES FC_DirectoryList(DirID) ON DELETE CASCADE,

 INDEX (ParentID, Depth),
 INDEX (ChildID)
) ENGINE = INNODB;
-- ------------------------------------------------------------------------------

CREATE TABLE FC_Files(
    FileID INT AUTO_INCREMENT,
    DirID INT NOT NULL,
    Size BIGINT UNSIGNED NOT NULL,
    UID INT NOT NULL,
    GID INT NOT NULL,
    Status INT NOT NULL,
    GUID char(36) NOT NULL,
    Type ENUM('File','Link') NOT NULL DEFAULT 'File',
    CreationDate DATETIME,
    ModificationDate DATETIME,
    Mode SMALLINT UNSIGNED NOT NULL DEFAULT 775,
    ChecksumType ENUM('Adler32','MD5'),
    Checksum VARCHAR(32),
    FileName VARCHAR(128) CHARACTER SET latin1 COLLATE latin1_bin NOT NULL,

    PRIMARY KEY (FileID),
    FOREIGN KEY (DirID) REFERENCES FC_DirectoryList(DirID) ON DELETE CASCADE,
    FOREIGN KEY (Status) REFERENCES FC_Statuses(StatusID),
    FOREIGN KEY (UID) REFERENCES FC_Users(UID),
    FOREIGN KEY (GID) REFERENCES FC_Groups(GID),

    UNIQUE (DirID, FileName),
    UNIQUE(GUID),

    INDEX (UID,GID),
    INDEX (Status),
    INDEX (FileName)

) ENGINE = INNODB;


-- -----------------------------------------------------------------------------
-- do we want the delete on cascade on the SE?

CREATE TABLE FC_Replicas (
    RepID INT AUTO_INCREMENT,
    FileID INT NOT NULL,
    SEID INTEGER NOT NULL,
    Status INTEGER NOT NULL,
    RepType ENUM ('Master','Replica') NOT NULL DEFAULT 'Master',
    CreationDate DATETIME,
    ModificationDate DATETIME,
    PFN VARCHAR(1024),

    PRIMARY KEY (RepID),
    FOREIGN KEY (FileID) REFERENCES FC_Files(FileID),
    FOREIGN KEY (SEID) REFERENCES FC_StorageElements(SEID),
    FOREIGN KEY (Status) REFERENCES FC_Statuses(StatusID),

    UNIQUE (FileID,SEID)


) ENGINE = INNODB;


-- ------------------------------------------------------------------------------


CREATE TABLE FC_DirectoryUsage(
   DirID INTEGER NOT NULL,
   SEID INTEGER NOT NULL,
   SESize BIGINT NOT NULL,
   SEFiles BIGINT NOT NULL,
   LastUpdate TIMESTAMP,

   PRIMARY KEY (DirID,SEID),
   FOREIGN KEY (SEID) REFERENCES FC_StorageElements(SEID) ON DELETE CASCADE,
   FOREIGN KEY (DirID) REFERENCES FC_DirectoryList(DirID) ON DELETE CASCADE

   -- INDEX(SEID)

) ENGINE = INNODB;

-- ------------------------------------------------------------------------------


CREATE TABLE FC_DirMeta (
    DirID INTEGER NOT NULL,
    MetaKey VARCHAR(31) CHARACTER SET latin1 COLLATE latin1_bin NOT NULL DEFAULT 'Noname',
    MetaValue VARCHAR(31) NOT NULL DEFAULT 'Noname',
    PRIMARY KEY (DirID,MetaKey)
) ENGINE = INNODB;

-- ------------------------------------------------------------------------------

CREATE TABLE FC_FileMeta (
    FileID INTEGER NOT NULL,
    MetaKey VARCHAR(31) CHARACTER SET latin1 COLLATE latin1_bin NOT NULL DEFAULT 'Noname',
    MetaValue VARCHAR(31) NOT NULL DEFAULT 'Noname',
    PRIMARY KEY (FileID,MetaKey)
) ENGINE = INNODB;


-- ------------------------------------------------------------------------------

CREATE TABLE FC_MetaFields (
  MetaID INT AUTO_INCREMENT PRIMARY KEY,
  MetaName VARCHAR(64) CHARACTER SET latin1 COLLATE latin1_bin NOT NULL,
  MetaType VARCHAR(128) NOT NULL
) ENGINE = INNODB;

-- ------------------------------------------------------------------------------

CREATE TABLE FC_FileMetaFields (
  MetaID INT AUTO_INCREMENT PRIMARY KEY,
  MetaName VARCHAR(64) CHARACTER SET latin1 COLLATE latin1_bin NOT NULL,
  MetaType VARCHAR(128) NOT NULL
) ENGINE = INNODB;

-- ------------------------------------------------------------------------------

CREATE TABLE FC_MetaSetNames (
  MetaSetID INT AUTO_INCREMENT PRIMARY KEY,
  MetaSetName VARCHAR(64)  NOT NULL,
  UNIQUE INDEX (MetaSetName)
) ENGINE = INNODB;

-- ------------------------------------------------------------------------------

CREATE TABLE FC_FileAncestors (
  FileID INT NOT NULL DEFAULT 0,
  AncestorID INT NOT NULL DEFAULT 0,
  AncestorDepth INT NOT NULL DEFAULT 0,
  INDEX (FileID),
  INDEX (AncestorID),
  INDEX (AncestorDepth),
  UNIQUE INDEX (FileID,AncestorID)
) ENGINE = INNODB;



-- ------------------------------------------------------------------------------

CREATE TABLE FC_MetaDatasets (
  DatasetID INT AUTO_INCREMENT,
  DatasetName VARCHAR(128) CHARACTER SET latin1 COLLATE latin1_bin NOT NULL,
  MetaQuery VARCHAR(512),
  DirID INT NOT NULL DEFAULT 0,
  TotalSize BIGINT UNSIGNED NOT NULL,
  NumberOfFiles INT NOT NULL,
  UID SMALLINT UNSIGNED NOT NULL,
  GID TINYINT UNSIGNED NOT NULL,
  Status SMALLINT UNSIGNED NOT NULL,
  CreationDate DATETIME,
  ModificationDate DATETIME,
  DatasetHash CHAR(36) NOT NULL,
  Mode SMALLINT UNSIGNED NOT NULL DEFAULT 509,

  PRIMARY KEY (DatasetID),
  UNIQUE INDEX (DatasetName,DirID)
) ENGINE = INNODB;

-- ------------------------------------------------------------------------------

CREATE TABLE FC_MetaDatasetFiles (
 MetaDatasetFileID INT AUTO_INCREMENT,
 DatasetID INT NOT NULL,
 FileID INT NOT NULL,

 PRIMARY KEY (MetaDatasetFileID),
 FOREIGN KEY (DatasetID) REFERENCES FC_MetaDatasets(DatasetID) ON DELETE CASCADE,
 FOREIGN KEY (FileID) REFERENCES FC_Files(FileID) ON DELETE CASCADE,
 UNIQUE INDEX (DatasetID,FileID)

) ENGINE = INNODB;

-- ------------------------------------------------------------------------------

CREATE TABLE FC_DatasetAnnotations (
 DatasetID INT NOT NULL,
 Annotation VARCHAR(512),

 PRIMARY KEY (DatasetID),
 FOREIGN KEY (DatasetID) REFERENCES FC_MetaDatasets(DatasetID) ON DELETE CASCADE

) ENGINE = INNODB;

-- ------------------------------------------------------------------------------



-- ps_find_dir : returns the dir id and the depth of a directory from its name
-- dirName : directory name
-- dir_id : directory id
-- dir_lvl : directory depth

DROP PROCEDURE IF EXISTS ps_find_dir;
DELIMITER //
CREATE PROCEDURE ps_find_dir
(IN dirName varchar(255), OUT dir_id INT, OUT dir_lvl INT)
BEGIN
  SELECT SQL_NO_CACHE DirID INTO dir_id from FC_DirectoryList where Name = dirName;
  IF dir_id IS NULL THEN
    SET dir_id = 0;
  END IF;
  SELECT  SQL_NO_CACHE max(Depth) INTO dir_lvl FROM FC_DirectoryClosure WHERE ChildID = dir_id;
END //
DELIMITER ;

-- ps_find_dirs : returns the directory ids from multiple directory names
-- dirNames : list of directory name, coma separated


DROP PROCEDURE IF EXISTS ps_find_dirs;
DELIMITER //
CREATE PROCEDURE ps_find_dirs
(IN dirNames MEDIUMTEXT)
BEGIN
--   SELECT DirID from FC_DirectoryList where Name in (dirNames);
  SET @sql = CONCAT('SELECT SQL_NO_CACHE Name, DirID from FC_DirectoryList where Name in (', dirNames, ')');
  PREPARE stmt FROM @sql;
  EXECUTE stmt;
  DEALLOCATE PREPARE stmt;

END //
DELIMITER ;


-- ps_remove_dir : removes a directory from its id.
--                Because of the cascade, it will also
--                delete the DirectoryInfo and DirectoryUsage entries
-- dir_id : directory id


DROP PROCEDURE IF EXISTS ps_remove_dir;
DELIMITER //
CREATE PROCEDURE ps_remove_dir
(IN dir_id INT)
BEGIN
  DELETE FROM FC_DirectoryList where DirID = dir_id;
END //
DELIMITER ;




-- ps_insert_dir : insert a directory and its info
--
-- parent_id : directory id of the parent
-- child_name : the directory name
-- UID : id to the user
-- gid : id to the group
-- mode : directory mode (posix)
-- status : ??
--
-- returns (errno, message)

DROP PROCEDURE IF EXISTS ps_insert_dir;
DELIMITER //
CREATE PROCEDURE ps_insert_dir
(IN parent_id INT, IN child_name varchar(255), IN UID INT,
 IN GID INT, IN Mode SMALLINT UNSIGNED, IN Status INT)
BEGIN
  DECLARE dir_id INT DEFAULT 0;

  DECLARE exit handler for sqlexception
    BEGIN
    ROLLBACK;
    RESIGNAL;
  END;

  START TRANSACTION;

    INSERT INTO FC_DirectoryList (UID, GID, CreationDate, ModificationDate, Mode, Status, Name) values ( UID, GID, UTC_TIMESTAMP(), UTC_TIMESTAMP(), Mode, Status, child_name);
    SELECT LAST_INSERT_ID() INTO dir_id;

    INSERT INTO FC_DirectoryClosure (ParentID, ChildID, Depth ) VALUES (dir_id, dir_id, 0);

    IF parent_id != 0 THEN
      INSERT INTO FC_DirectoryClosure(ParentID, ChildID, depth)
        SELECT SQL_NO_CACHE p.ParentID, dir_id, p.depth + 1
        FROM FC_DirectoryClosure p
        WHERE p.ChildID = parent_id;
    END IF;


    SELECT dir_id;

   COMMIT;
END //
DELIMITER ;


-- ps_get_dirName_from_id : returns the directory name from its id
-- dir_id : directory id
-- dirName (out param): the name of the directory

DROP PROCEDURE IF EXISTS ps_get_dirName_from_id;
DELIMITER //
CREATE PROCEDURE ps_get_dirName_from_id
(IN dir_id INT, OUT dirName varchar(255) )
BEGIN
   SELECT SQL_NO_CACHE Name INTO dirName from FC_DirectoryList where DirID = dir_id;
END //
DELIMITER ;

-- ps_get_dirNames_from_ids : returns the directory names from their ids
-- dirIds : comma separated list of ids
-- returns (directory id, directory name)

DROP PROCEDURE IF EXISTS ps_get_dirNames_from_ids;
DELIMITER //
CREATE PROCEDURE ps_get_dirNames_from_ids
(IN dirIds TEXT)
BEGIN
--   SELECT DirID from FC_DirectoryList where Name in (dirNames);
  SET @sql = CONCAT('SELECT SQL_NO_CACHE DirID, Name from FC_DirectoryList where DirID in (', dirIds, ')');
  PREPARE stmt FROM @sql;
  EXECUTE stmt;
  DEALLOCATE PREPARE stmt;

END //
DELIMITER ;


-- ps_get_parentIds_from_id : returns all the parent directory id, including self, ordered by depth
-- dir_id : directory id
-- returns : (directory id)

DROP PROCEDURE IF EXISTS ps_get_parentIds_from_id;
DELIMITER //
CREATE PROCEDURE ps_get_parentIds_from_id
(IN dir_id INT )
BEGIN
   SELECT SQL_NO_CACHE ParentID FROM FC_DirectoryClosure WHERE ChildID = dir_id order by Depth desc;
END //
DELIMITER ;


-- ps_get_direct_children : returns the direct children of a directory
-- dir_id : directory id
-- returns : (directory id)

DROP PROCEDURE IF EXISTS ps_get_direct_children;
DELIMITER //
CREATE PROCEDURE ps_get_direct_children
(IN dir_id INT )
BEGIN
   SELECT SQL_NO_CACHE d.DirID from FC_DirectoryList d JOIN FC_DirectoryClosure c on (d.DirID = c.ChildID) where c.ParentID = dir_id and c.Depth = 1;
END //
DELIMITER ;


-- ps_get_sub_directories : returns all subdirectories, and their absolute level.
-- dir_id : directory id
-- includeParent: if true, include oneself
-- returns (directory id, absolute level)

DROP PROCEDURE IF EXISTS ps_get_sub_directories;
DELIMITER //
CREATE PROCEDURE ps_get_sub_directories
(IN dir_id INT, IN includeParent BOOLEAN )
BEGIN

    IF includeParent THEN
      SELECT SQL_NO_CACHE c1.ChildID, max(c1.Depth) AS lvl
      FROM FC_DirectoryClosure c1
      JOIN FC_DirectoryClosure c2 ON c1.ChildID = c2.ChildID
      WHERE c2.ParentID = dir_id
      GROUP BY c1.ChildID
      ORDER BY NULL;
    ELSE
      SELECT SQL_NO_CACHE c1.ChildID, max(c1.Depth) AS lvl
      FROM FC_DirectoryClosure c1
      JOIN FC_DirectoryClosure c2 ON c1.ChildID = c2.ChildID
      WHERE c2.ParentID = dir_id AND c2.Depth != 0
      GROUP BY c1.ChildID
      ORDER BY NULL;
    END IF;

END //
DELIMITER ;


-- ps_get_multiple_sub_directories : returns a disordered list of children directory ID, including self
-- dirIds: comma separated list of directory ids
-- returns (directory ID)

DROP PROCEDURE IF EXISTS ps_get_multiple_sub_directories;
DELIMITER //
CREATE PROCEDURE ps_get_multiple_sub_directories
(IN dirIds TEXT)
BEGIN
  SET @sql = CONCAT('select SQL_NO_CACHE distinct(ChildID) from FC_DirectoryClosure where ParentID in (',dirIds ,')');
  PREPARE stmt FROM @sql;
  EXECUTE stmt;
  DEALLOCATE PREPARE stmt;

END //
DELIMITER ;


-- ps_count_sub_directories : counts how many subdirectories are in a given directory
-- dir_id : directory id
-- includeParent : if true, counts oneself
-- countDir (out value): amount of subdir

DROP PROCEDURE IF EXISTS ps_count_sub_directories;
DELIMITER //
CREATE PROCEDURE ps_count_sub_directories
(IN dir_id INT, IN includeParent BOOLEAN, OUT countDir INT )
BEGIN

  SELECT SQL_NO_CACHE count(ChildID) INTO countDir FROM FC_DirectoryClosure WHERE ParentID = dir_id;

    IF NOT includeParent THEN
      IF countDir != 0 THEN
        set countDir = countDir - 1;
      END IF;
    END IF;
END //
DELIMITER ;


-- ps_count_files_in_dir : counts how many files are in a given directory
-- dir_id : directory id
-- countFile (out value): amount of files

DROP PROCEDURE IF EXISTS ps_count_files_in_dir;
DELIMITER //
CREATE PROCEDURE ps_count_files_in_dir
(IN dir_id INT, OUT countFile INT )
BEGIN

  SELECT SQL_NO_CACHE count(FileID) INTO countFile FROM FC_Files WHERE DirID = dir_id;

END //
DELIMITER ;


-- dir_id : the id of the dir in which we insert/remove
-- se_id : the id of the SE in which the replica was inserted
-- size_diff : the modification to bring to the size (positif if adding a replica, negatif otherwise)
-- file_diff : + or - 1 depending whether we add or remove a replica

DROP PROCEDURE IF EXISTS update_directory_usage;
DELIMITER //
CREATE PROCEDURE update_directory_usage
(IN dir_id INT, IN se_id INT, IN size_diff BIGINT, IN file_diff INT)
BEGIN

    -- alternative
    -- If it is the first replica inserted for the given SE, then we insert the new row, otherwise we do an update
    INSERT INTO FC_DirectoryUsage (DirID, SEID, SESize, SEFiles) VALUES (dir_id, se_id, size_diff, file_diff) ON DUPLICATE KEY UPDATE  SESize = SESize + size_diff, SEFiles = SEFiles + file_diff;

END //
DELIMITER ;


DROP TRIGGER IF EXISTS trg_after_update_replica_move_size;
DELIMITER //
CREATE TRIGGER trg_after_update_replica_move_size AFTER UPDATE ON FC_Replicas
FOR EACH ROW
BEGIN

  DECLARE file_size BIGINT;
  DECLARE dir_id INT;

  -- We only update if the replica was moved
  IF new.SEID <> old.SEID THEN

    SELECT SQL_NO_CACHE Size, DirID INTO file_size, dir_id FROM FC_Files where FileID = old.FileID;


--     -- Decrease the usage of old storage ...
--     call update_directory_usage (old.FileID, old.SEID, -file_size, -1);
--
--     -- ... and increase it on the new one
--     call update_directory_usage (old.FileID, new.SEID, file_size, 1);
    -- Decrease the usage of old storage ...
    call update_directory_usage (dir_id, old.SEID, -file_size, -1);

    -- ... and increase it on the new one
    call update_directory_usage (dir_id, new.SEID, file_size, 1);
  END IF;
END //
DELIMITER ;



-- ps_get_replicas_for_files_in_dir : get replica information for all the files in a given dir
-- dir_id : directory id
-- allStatus : if False, consider only the status defined in visibleFileStatus and visibleReplicaStatus
-- visibleFileStatus : status of files to be considered
-- visibleReplicaStatus : status of replicas to be considered
-- outputs : FileName, FileID, SEName, PFN

DROP PROCEDURE IF EXISTS ps_get_replicas_for_files_in_dir;
DELIMITER //
CREATE PROCEDURE ps_get_replicas_for_files_in_dir
(IN dir_id INT, IN allStatus BOOLEAN, IN visibleFileStatus VARCHAR(255), IN visibleReplicaStatus VARCHAR(255) )
BEGIN

  set @sql = 'select SQL_NO_CACHE f.FileName, f.FileID, s.SEName, r.PFN from FC_Replicas r join FC_Files f on f.FileID = r.FileID join FC_StorageElements s on s.SEID = r.SEID ';

  IF not allStatus THEN
    SET @sql = CONCAT(@sql, ' join FC_Statuses fst on f.Status = fst.StatusID join FC_Statuses rst on r.Status = rst.StatusID where DirID = ',dir_id ,' and fst.Status  in (',visibleFileStatus,') and rst.Status in (', visibleReplicaStatus, ')');
  ELSE
    SET @sql = CONCAT(@sql, 'where DirID = ',dir_id );
  END IF;

  PREPARE stmt FROM @sql;
  EXECUTE stmt;
  DEALLOCATE PREPARE stmt;

END //
DELIMITER ;


-- ps_get_file_id_from_lfn : get the file id from a given LFN ("dirName/fileName")
-- dirName : name of the directory
-- fileName : name of the file
-- file_id (out) : id of the file

DROP PROCEDURE IF EXISTS ps_get_file_id_from_lfn;
DELIMITER //
CREATE PROCEDURE ps_get_file_id_from_lfn
(IN dirName VARCHAR(255), IN fileName VARCHAR(255), OUT file_id INT )
BEGIN
  DECLARE done INT;
  DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;

  SELECT SQL_NO_CACHE FileID INTO file_id FROM FC_Files f JOIN FC_DirectoryList d ON d.DirID = f.DirID WHERE d.Name = dirName and f.FileName = fileName;
  IF file_id IS NULL THEN
    SET file_id = 0;
  END IF;

END //
DELIMITER ;


-- ps_get_file_ids_from_dir_id : get the file ids of files in a given directory and with matching names
-- dir_id : directory id
-- file_names : names of the files we are interested in
-- output : FileID, FileName

DROP PROCEDURE IF EXISTS ps_get_file_ids_from_dir_id;
DELIMITER //
CREATE PROCEDURE ps_get_file_ids_from_dir_id
(IN dir_id INT, IN file_names MEDIUMTEXT)
BEGIN

  SET @sql = CONCAT('SELECT SQL_NO_CACHE FileID, FileName FROM FC_Files f WHERE DirID = ', dir_id, ' AND FileName IN (', file_names, ')');
  PREPARE stmt FROM @sql;
  EXECUTE stmt;
  DEALLOCATE PREPARE stmt;


END //
DELIMITER ;




-- ps_get_all_info_for_files_in_dir : get all the info about files in a given directory
-- dir_id : directory id
-- specificFiles : if True, consider the file_names list
-- file_names : list of files to consider
-- allStatus : if False, consider the visibleFileStatus
-- visibleFileStatus : list of status we are interested in
-- output : FileName, DirID, f.FileID, Size, f.uid, UserName, f.gid, GroupName, s.Status,
--                     GUID, Checksum, ChecksumType, Type, CreationDate,ModificationDate, Mode

drop procedure if exists ps_get_all_info_for_files_in_dir;
DELIMITER //
CREATE PROCEDURE ps_get_all_info_for_files_in_dir
(IN dir_id INT, IN specificFiles BOOLEAN, IN file_names MEDIUMTEXT, IN allStatus BOOLEAN, IN visibleFileStatus VARCHAR(255))
BEGIN

  set @sql = CONCAT('SELECT SQL_NO_CACHE FileName, DirID, f.FileID, Size, f.uid, UserName, f.gid, GroupName, s.Status,
                     GUID, Checksum, ChecksumType, Type, CreationDate,ModificationDate, Mode
                    FROM FC_Files f
                    JOIN FC_Users u ON f.UID = u.UID
                    JOIN FC_Groups g ON f.GID = g.GID
                    JOIN FC_Statuses s ON f.Status = s.StatusID
                    WHERE DirID = ', dir_id, ' ' );

  IF not allStatus THEN
    SET @sql = CONCAT(@sql,' and s.Status  in (',visibleFileStatus,') ');
  END IF;

  IF specificFiles THEN
    SET @sql = CONCAT(@sql,' and f.FileName  in (',file_names,') ');
  END IF;

  PREPARE stmt FROM @sql;
  EXECUTE stmt;
  DEALLOCATE PREPARE stmt;

END //
DELIMITER ;



-- ps_get_all_info_for_file_ids : get all the info for given file ids
-- file_ids : list of file ids
-- output : FileID, Size, UID, GID, s.Status, GUID, CreationDate

DROP PROCEDURE IF EXISTS ps_get_all_info_for_file_ids;
DELIMITER //
CREATE PROCEDURE ps_get_all_info_for_file_ids
(IN file_ids TEXT)
BEGIN

  SET @sql = CONCAT('SELECT SQL_NO_CACHE f.FileID, Size, UID, GID, s.Status, GUID, CreationDate
                     FROM FC_Files f
                     JOIN FC_Statuses s ON f.Status = s.StatusID
                     WHERE f.FileID IN (', file_ids, ')');

  PREPARE stmt FROM @sql;
  EXECUTE stmt;
  DEALLOCATE PREPARE stmt;


END //
DELIMITER ;



-- ps_insert_file : insert a new file and update the DirectoryUsage table
-- parameter list is self explaining
-- output : new fileId, message. If fileId == 0, an error happened
DROP PROCEDURE IF EXISTS ps_insert_file;
DELIMITER //
CREATE PROCEDURE ps_insert_file
(IN dir_id INT, IN size BIGINT, IN UID INT, IN GID INT,
 IN status_id INT, IN filename VARCHAR(255), IN GUID VARCHAR(36),
 IN checksum VARCHAR(32), IN checksumtype ENUM('Adler32','MD5'), IN mode SMALLINT )
BEGIN
  DECLARE file_id INT DEFAULT 0;

  DECLARE exit handler for sqlexception
    BEGIN
    ROLLBACK;
    RESIGNAL;
  END;


  START TRANSACTION;
  INSERT INTO FC_Files (DirID, Size, UID, GID, Status, FileName,GUID, Checksum, ChecksumType, CreationDate, ModificationDate, Mode )
  VALUES (dir_id, size, UID, GID, status_id, filename, GUID, checksum, checksumtype, UTC_TIMESTAMP(), UTC_TIMESTAMP(), mode);
  SELECT LAST_INSERT_ID() INTO file_id;

  INSERT INTO FC_DirectoryUsage (DirID, SEID, SESize, SEFiles) VALUES (dir_id, 1, size, 1) ON DUPLICATE KEY UPDATE  SESize = SESize + size, SEFiles = SEFiles + 1;

  COMMIT;

  SELECT file_id;

END //
DELIMITER ;



-- ps_insert_multiple_file : insert many files together and update the DirectoryUsage table
-- fileValues DirID, Size, UID, GID, Status, FileName, GUID, Checksum, ChecksumType, CreationDate, ModificationDate, Mode for each file, formated like "(a,y,z), (u,v,w)"
-- fileDesc : unique identifier of files formated like " (DirID = x and FileName = y) OR (DirID = u and FileName = v)"
-- output : DirID, FileName, FileID

DROP PROCEDURE IF EXISTS ps_insert_multiple_file;
DELIMITER //
CREATE PROCEDURE ps_insert_multiple_file
(IN fileValues LONGTEXT, IN fileDesc LONGTEXT )
BEGIN

  DECLARE exit handler for sqlexception
    BEGIN
    ROLLBACK;
    RESIGNAL;
  END;

  START TRANSACTION;
  SET @sql = CONCAT('INSERT INTO FC_Files (DirID, Size, UID, GID, Status, FileName, GUID, Checksum, ChecksumType, CreationDate, ModificationDate, Mode) VALUES ', fileValues);


  PREPARE stmt FROM @sql;
  EXECUTE stmt;
  DEALLOCATE PREPARE stmt;


  SET @sql = CONCAT('INSERT INTO FC_DirectoryUsage (DirID, SEID, SESize, SEFiles) SELECT SQL_NO_CACHE f.DirID, 1, f.Size, 1 FROM FC_Files f WHERE ', fileDesc);
  SET @sql = CONCAT(@sql, ' ON DUPLICATE KEY UPDATE SESize = SESize + f.Size, SEFiles = SEFiles + 1');

  PREPARE stmt FROM @sql;
  EXECUTE stmt;
  DEALLOCATE PREPARE stmt;

  COMMIT;

  SET @sql = CONCAT('SELECT SQL_NO_CACHE DirID, FileName, FileID FROM FC_Files WHERE ', fileDesc );

  PREPARE stmt FROM @sql;
  EXECUTE stmt;
  DEALLOCATE PREPARE stmt;

END //
DELIMITER ;



-- ps_get_file_ids_from_guids : return list of file ids for given guids
-- guids : list of guids
-- output : GUID, FileID

DROP PROCEDURE IF EXISTS ps_get_file_ids_from_guids;
DELIMITER //
CREATE PROCEDURE ps_get_file_ids_from_guids
(IN  guids TEXT)
BEGIN
  SET @sql = CONCAT('SELECT SQL_NO_CACHE GUID, FileID FROM FC_Files f WHERE GUID IN (', guids, ')');

  PREPARE stmt FROM @sql;
  EXECUTE stmt;
  DEALLOCATE PREPARE stmt;


END //
DELIMITER ;

-- ps_get_lfns_from_guids : return list of file lfns for given guids
-- guids : list of guids
-- output : GUID, LFN

DROP PROCEDURE IF EXISTS ps_get_lfns_from_guids;
DELIMITER //
CREATE PROCEDURE ps_get_lfns_from_guids
(IN  guids TEXT)
BEGIN
  SET @sql = CONCAT('SELECT SQL_NO_CACHE GUID, CONCAT(d.Name, "/", f.FileName) FROM FC_Files f JOIN FC_DirectoryList d on f.DirID = d.DirID WHERE GUID IN (', guids, ')');

  PREPARE stmt FROM @sql;
  EXECUTE stmt;
  DEALLOCATE PREPARE stmt;


END //
DELIMITER ;


-- ps_delete_replicas_from_file_ids : delete all the replicas for given file ids and update the DirectoryUsage table
-- file_ids : list of file ids
-- output : 0, 'OK'

DROP PROCEDURE IF EXISTS ps_delete_replicas_from_file_ids;
DELIMITER //
CREATE PROCEDURE ps_delete_replicas_from_file_ids
(IN  file_ids TEXT)
BEGIN

  DECLARE exit handler for sqlexception
    BEGIN
    ROLLBACK;
    RESIGNAL;
  END;

  START TRANSACTION;

 -- Store the name of the tmp table once for all

  SET @tmpTableName = CONCAT('tmpDirUsageDelRep_',CONNECTION_ID());

  -- We create the table if it does not exist
  SET @sql = CONCAT('CREATE TEMPORARY TABLE IF NOT EXISTS ',@tmpTableName ,' (DirID INT, SEID INT, t_size BIGINT UNSIGNED, t_file INT, INDEX(DirID))');

  PREPARE stmt FROM @sql;
  EXECUTE stmt;
  DEALLOCATE PREPARE stmt;

  -- Insert into it the values we will have to substract later on
  SET @sql = CONCAT('INSERT INTO ', @tmpTableName, '(DirID, SEID, t_size, t_file) SELECT d1.DirID, d1.SEID, SUM(f.Size) as t_size, count(*) as t_file
  FROM FC_DirectoryUsage d1, FC_Files f, FC_Replicas r
  WHERE r.FileID = f.FileID AND f.DirID = d1.DirID AND r.SEID = d1.SEID AND f.FileID IN (', file_ids, ') GROUP BY d1.DirID, d1.SEID');

  PREPARE stmt FROM @sql;
  EXECUTE stmt;
  DEALLOCATE PREPARE stmt;

  -- perform the update
  SET @sql = CONCAT('UPDATE FC_DirectoryUsage d, ',@tmpTableName,' t set d.`SESize` = d.`SESize` - t.t_size, d.`SEFiles` = d.`SEFiles` - t.t_file where d.DirID = t.DirID and d.`SEID`= t.SEID');

  PREPARE stmt FROM @sql;
  EXECUTE stmt;
  DEALLOCATE PREPARE stmt;



  -- delete the entries from the temporary table
  SET @sql = CONCAT('DELETE t FROM ',@tmpTableName, ' t JOIN FC_Files f ON t.DirID = f.DirID where f.FileID IN (', file_ids, ')');

  PREPARE stmt FROM @sql;
  EXECUTE stmt;
  DEALLOCATE PREPARE stmt;


  -- delete the entry from the FC_Replicas table
  SET @sql = CONCAT('DELETE FROM FC_Replicas WHERE FileID IN (', file_ids, ')');
  PREPARE stmt FROM @sql;
  EXECUTE stmt;
  DEALLOCATE PREPARE stmt;

  COMMIT;
  SELECT 0, 'OK';

END //
DELIMITER ;


-- ps_delete_files : delete files from file ids and update the DirectoryUsage table.
--                   CAREFUL : the cascade delete also removes the replicas but will not update DirectoryUsage
-- file_ids list of file ids
-- output 0, 'OK'

DROP PROCEDURE IF EXISTS ps_delete_files;
DELIMITER //
CREATE PROCEDURE ps_delete_files
(IN  file_ids MEDIUMTEXT)
BEGIN

  DECLARE exit handler for sqlexception
    BEGIN
    ROLLBACK;
    RESIGNAL;
  END;

   START TRANSACTION;

   -- Store the name of the tmp table once for all

   SET @tmpTableName = CONCAT('tmpDirUsageDelFile_',CONNECTION_ID());

   -- We create the table if it does not exist
   SET @sql = CONCAT('CREATE TEMPORARY TABLE IF NOT EXISTS ',@tmpTableName ,' (DirID INT, t_size BIGINT UNSIGNED, t_file INT, INDEX(DirID))');

   PREPARE stmt FROM @sql;
   EXECUTE stmt;
   DEALLOCATE PREPARE stmt;


   -- Insert into it the values we will have to substract later on
   SET @sql = CONCAT('INSERT INTO ', @tmpTableName, '(DirID,t_size, t_file) SELECT d1.DirID, SUM(f.Size) as t_size, count(*) as t_file
   FROM FC_DirectoryList d1, FC_Files f
   WHERE f.DirID = d1.DirID AND f.FileID IN (', file_ids, ') GROUP BY d1.DirID');

   PREPARE stmt FROM @sql;
   EXECUTE stmt;
   DEALLOCATE PREPARE stmt;

   -- perform the update
   SET @sql = CONCAT('UPDATE FC_DirectoryUsage d, ',@tmpTableName,' t set d.`SESize` = d.`SESize` - t.t_size, d.`SEFiles` = d.`SEFiles` - t.t_file where d.DirID = t.DirID and d.`SEID`= 1');

   PREPARE stmt FROM @sql;
   EXECUTE stmt;
   DEALLOCATE PREPARE stmt;



   -- delete the entries from the temporary table
   SET @sql = CONCAT('DELETE t FROM ',@tmpTableName, ' t JOIN FC_Files f ON t.DirID = f.DirID where f.FileID IN (', file_ids, ')');

   PREPARE stmt FROM @sql;
   EXECUTE stmt;
   DEALLOCATE PREPARE stmt;


   -- delete the entry from the File table
   SET @sql = CONCAT('DELETE FROM FC_Files WHERE FileID IN (', file_ids, ')');
   PREPARE stmt FROM @sql;
   EXECUTE stmt;
   DEALLOCATE PREPARE stmt;

   COMMIT;
   SELECT 0, 'OK';
END //
DELIMITER ;

-- ps_insert_replica : insert a replica and update the DirectoryUsage table
-- file_id : id of the file it belongs to
-- se_id : storage element id where it is stored
-- status_id : id of the replica status
-- rep_type : Master or Replica
-- pfn : url of the file on the SE
-- output : replica ID, msg. if ReplicaID == 0, msg is an error message

DROP PROCEDURE IF EXISTS ps_insert_replica;
DELIMITER //
CREATE PROCEDURE ps_insert_replica
(IN file_id INT, IN se_id INT, IN status_id INT,
 IN rep_type ENUM ('Master','Replica') , IN pfn VARCHAR(1024)
)
BEGIN
  DECLARE replica_id INT DEFAULT 0;


  DECLARE EXIT HANDLER FOR sqlexception BEGIN
    -- retrieve the error message
    GET DIAGNOSTICS CONDITION 1
         @errno = MYSQL_ERRNO;
    ROLLBACK;

    -- The replica already exists
    IF @errno = 1062
    THEN
      SELECT RepID as replica_id from FC_Replicas where FileID = file_id and SEID = se_id;
    ELSE
      RESIGNAL;
    END IF;
  END;


  START TRANSACTION;
  INSERT INTO FC_Replicas (FileID, SEID, Status, RepType, CreationDate, ModificationDate, PFN)
  VALUES (file_id, se_id, status_id, rep_type, UTC_TIMESTAMP(), UTC_TIMESTAMP(), pfn);
  SELECT LAST_INSERT_ID() INTO replica_id;
  INSERT INTO FC_DirectoryUsage (DirID, SEID, SESize, SEFiles)  SELECT DirID, se_id, Size, 1 from FC_Files f where f.FileID = file_id ON DUPLICATE KEY UPDATE  SESize = SESize + Size, SEFiles = SEFiles + 1;

  COMMIT;

  SELECT replica_id;

END //
DELIMITER ;



-- ps_insert_multiple_replica : insert multiple replica at the time and update DirectoryUsage table
-- replicaValues : FileID, SEID, Status, RepType, CreationDate, ModificationDate, PFN formated like "(a,y,z), (u,v,w)"
-- replicaDesc formated like " (FileID = x and SEID = y) OR (FileID = u and SEID = v)"
-- output : FileID, SEID, RepID
DROP PROCEDURE IF EXISTS ps_insert_multiple_replica;
DELIMITER //
CREATE PROCEDURE ps_insert_multiple_replica
(IN replicaValues LONGTEXT, IN replicaDesc LONGTEXT )
BEGIN

  DECLARE exit handler for sqlexception
    BEGIN
    ROLLBACK;
    RESIGNAL;
  END;

  START TRANSACTION;

  SET @sql = CONCAT('INSERT INTO  FC_Replicas (FileID, SEID, Status, RepType, CreationDate, ModificationDate, PFN) VALUES ', replicaValues);


  PREPARE stmt FROM @sql;
  EXECUTE stmt;
  DEALLOCATE PREPARE stmt;


  SET @sql = CONCAT('INSERT INTO FC_DirectoryUsage (DirID, SEID, SESize, SEFiles) SELECT SQL_NO_CACHE f.DirID, SEID, f.Size, 1 FROM FC_Files f, FC_Replicas r WHERE f.FileID = r.FileID AND (', replicaDesc);
  SET @sql = CONCAT(@sql, ') ON DUPLICATE KEY UPDATE SESize = SESize + f.Size, SEFiles = SEFiles + 1');
  -- insert into FC_DirectoryUsage (DirID, SEID, SESize, SEFiles) select f.DirID, 1, f.Size as size_diff, 1 as file_diff from FC_Files f where (DirID = 1 and FileName = 'a.txt') OR (DirID = 1 and FileName = '1.txt') on duplicate key update SESize = SESize + f.Size, SEFiles = SEFiles + 1;
  PREPARE stmt FROM @sql;
  EXECUTE stmt;
  DEALLOCATE PREPARE stmt;

  COMMIT;

  SET @sql = CONCAT('SELECT SQL_NO_CACHE FileID, SEID, RepID FROM FC_Replicas r WHERE ', replicaDesc );

  PREPARE stmt FROM @sql;
  EXECUTE stmt;
  DEALLOCATE PREPARE stmt;

END //
DELIMITER ;


-- ps_get_replica_id : get the replica ID for a given file and se
-- file_id : file id
-- se id : storage element id
-- rep_id (out) : replica id

DROP PROCEDURE IF EXISTS ps_get_replica_id;
DELIMITER //
CREATE PROCEDURE ps_get_replica_id
(IN  file_id INT, IN se_id INT, OUT rep_id INT)
BEGIN

  SELECT SQL_NO_CACHE RepID INTO rep_id FROM FC_Replicas WHERE FileID = file_id AND SEID = se_id;

  IF rep_id IS NULL THEN
    SET rep_id = 0;
  END IF;

END //
DELIMITER ;



-- ps_delete_replica_from_file_and_se_ids : delete a given replica and update the DirectoryUsage
--
-- file_id : id of the file the replica refers to
-- se_id : id of the SE the replica is on
--
-- output : 0, 'OK'

DROP PROCEDURE IF EXISTS ps_delete_replica_from_file_and_se_ids;
DELIMITER //
CREATE PROCEDURE ps_delete_replica_from_file_and_se_ids
(IN  file_id INT, IN se_id INT)
BEGIN
  DECLARE file_size BIGINT DEFAULT 0;
  DECLARE dir_id INT DEFAULT 0;

  DECLARE exit handler for sqlexception
    BEGIN
    ROLLBACK;
    RESIGNAL;
  END;

  -- We need to join on the replicas to make sure that there is a replica at the given se
  -- otherwise the DirectoryUsage will be updated for no good reason
  SELECT Size, DirID INTO file_size, dir_id from FC_Files f JOIN FC_Replicas r on f.FileID = r.FileID where f.FileID = file_id and r.SEID = se_id;

  START TRANSACTION;
    UPDATE FC_DirectoryUsage SET SESize = SESize - file_size, SEFiles = SEFiles - 1 WHERE DirID = dir_id and SEID = se_id;
    DELETE FROM FC_Replicas WHERE FileID = file_id AND SEID = se_id;
  COMMIT;

  SELECT 0, 'OK';

END //
DELIMITER ;

-- ps_set_replica_status : set the replica status
--
-- file_id : id of the file
-- se_id : SE ID
-- status_id : new status id
--
-- output : 0, number of column affected (should be 1 or 0), 'OK'

DROP PROCEDURE IF EXISTS ps_set_replica_status;
DELIMITER //
CREATE PROCEDURE ps_set_replica_status
(IN  file_id INT, IN se_id INT, IN status_id INT)
BEGIN

  UPDATE FC_Replicas SET Status = status_id WHERE FileID = file_id AND SEID = se_id;

  SELECT ROW_COUNT() as affected;

END //
DELIMITER ;



-- ps_set_replica_host : 'move' a replica to another SE and updates the DirectoryUsage
--
-- file_id : id of the file
-- old_se_id : old SE ID
-- new_se_id : new se id
--
-- output : errno, number of column affected (should be 1 or 0), msg. errno == 0 -> all good

DROP PROCEDURE IF EXISTS ps_set_replica_host;
DELIMITER //
CREATE PROCEDURE ps_set_replica_host
(IN  file_id INT, IN old_se_id INT, IN new_se_id INT)
BEGIN
  DECLARE file_size BIGINT DEFAULT 0;
  DECLARE dir_id INT DEFAULT 0;

  DECLARE exit handler for sqlexception
    BEGIN
    ROLLBACK;
    RESIGNAL;
  END;

  SELECT Size, DirID INTO file_size, dir_id from FC_Files WHERE FileID = file_id;

  START TRANSACTION;
    UPDATE FC_DirectoryUsage SET SESize = SESize - file_size, SEFiles = SEFiles - 1 WHERE DirID = dir_id and SEID = old_se_id;
    INSERT INTO FC_DirectoryUsage (DirID, SEID, SESize, SEFiles)  VALUES (dir_id, new_se_id, file_size, 1) ON DUPLICATE KEY UPDATE  SESize = SESize + file_size, SEFiles = SEFiles + 1;
    UPDATE FC_Replicas SET SEID = new_se_id WHERE FileID = file_id AND SEID = old_se_id;
  COMMIT;

  SELECT ROW_COUNT() as affected;

END //
DELIMITER ;


-- ps_set_file_uid : change owner of a file
--
-- file_id : id of the file
-- in_uid : new uid
--
-- output : errno, msg. If errno ==0, all good

DROP PROCEDURE IF EXISTS ps_set_file_uid;
DELIMITER //
CREATE PROCEDURE ps_set_file_uid
(IN  file_id INT, IN in_uid INT)
BEGIN

  UPDATE FC_Files SET UID = in_uid, ModificationDate = UTC_TIMESTAMP() where FileID = file_id;

  SELECT ROW_COUNT();

END //
DELIMITER ;


-- ps_set_file_gid : change group of a file
--
-- file_id : id of the file
-- in_gid : new gid
--
-- output : errno, msg. If errno ==0, all good

DROP PROCEDURE IF EXISTS ps_set_file_gid;
DELIMITER //
CREATE PROCEDURE ps_set_file_gid
(IN  file_id INT, IN in_gid INT)
BEGIN


  UPDATE FC_Files SET GID = in_gid, ModificationDate = UTC_TIMESTAMP() where FileID = file_id;

  SELECT ROW_COUNT();

END //
DELIMITER ;


-- ps_set_file_status : change the status of a file
--
-- file_id : id of the file
-- status_id : id of the new status
--
-- output errno, msg. If errno ==0, all good

DROP PROCEDURE IF EXISTS ps_set_file_status;
DELIMITER //
CREATE PROCEDURE ps_set_file_status
(IN  file_id INT, IN status_id INT)
BEGIN


  UPDATE FC_Files SET Status = status_id, ModificationDate = UTC_TIMESTAMP() where FileID = file_id;

  SELECT ROW_COUNT();

END //
DELIMITER ;


-- ps_set_file_mode : change the mode of a file
--
-- file_id : id of the file
-- in_mode : new mode
--
-- output errno, msg. If errno ==0, all good

DROP PROCEDURE IF EXISTS ps_set_file_mode;
DELIMITER //
CREATE PROCEDURE ps_set_file_mode
(IN  file_id INT, IN in_mode SMALLINT UNSIGNED)
BEGIN

  UPDATE FC_Files SET Mode = in_mode, ModificationDate = UTC_TIMESTAMP()  WHERE FileID = file_id;

  SELECT ROW_COUNT();

END //
DELIMITER ;



-- TO BE DEPRECATED IN FAVOR OF THE BULK METHOD
-- ps_get_all_info_of_replicas : get the info of all replicas of a given file
--
-- file_id : id of the file
-- allStatus : if False, consider visibleReplicaStatus
-- visibleReplicaStatus : list of status we are interested in
--
-- output :  FileID, se.SEName, st.Status, RepType, CreationDate, ModificationDate, PFN

DROP PROCEDURE IF EXISTS ps_get_all_info_of_replicas;
DELIMITER //
CREATE PROCEDURE ps_get_all_info_of_replicas
(IN file_id INT, IN allStatus BOOLEAN, IN visibleReplicaStatus TEXT)
BEGIN


  IF allStatus THEN

    SELECT SQL_NO_CACHE FileID, se.SEName, st.Status, RepType, CreationDate, ModificationDate, PFN
    FROM FC_Replicas r
    JOIN FC_StorageElements se on r.SEID = se.SEID
    JOIN FC_Statuses st on r.Status = st.StatusID
    WHERE FileID = file_id;

  ELSE

    SET @sql = CONCAT(
                    'SELECT SQL_NO_CACHE FileID, se.SEName, st.Status, RepType, CreationDate, ModificationDate, PFN
                    FROM FC_Replicas r
                    JOIN FC_StorageElements se on r.SEID = se.SEID
                    JOIN FC_Statuses st on r.Status = st.StatusID
                    WHERE FileID =',file_id,
                    ' AND st.Status IN (', visibleReplicaStatus, ')');

    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

  END IF;

END //
DELIMITER ;

-- ps_get_all_info_of_replicas_bulk : get the info of all replicas for a list of file ids
--
-- file_ids : list of file IDs
-- allStatus : if False, consider visibleReplicaStatus
-- visibleReplicaStatus : list of status we are interested in
--
-- output :  FileID, se.SEName, st.Status, RepType, CreationDate, ModificationDate, PFN

DROP PROCEDURE IF EXISTS ps_get_all_info_of_replicas_bulk;
DELIMITER //
CREATE PROCEDURE ps_get_all_info_of_replicas_bulk
(IN file_ids TEXT, IN allStatus BOOLEAN, IN visibleReplicaStatus TEXT)
BEGIN


  IF allStatus THEN

    SET @sql = CONCAT('SELECT SQL_NO_CACHE FileID, se.SEName, st.Status, RepType, CreationDate, ModificationDate, PFN
                      FROM FC_Replicas r
                      JOIN FC_StorageElements se on r.SEID = se.SEID
                      JOIN FC_Statuses st on r.Status = st.StatusID
                      WHERE FileID IN (', file_ids, ')');
    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

  ELSE

    SET @sql = CONCAT(
                    'SELECT SQL_NO_CACHE FileID, se.SEName, st.Status, RepType, CreationDate, ModificationDate, PFN
                    FROM FC_Replicas r
                    JOIN FC_StorageElements se on r.SEID = se.SEID
                    JOIN FC_Statuses st on r.Status = st.StatusID
                    WHERE FileID IN (', file_ids, ') ',
                    'AND st.Status IN (', visibleReplicaStatus, ')');

    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

  END IF;

END //
DELIMITER ;


-- ps_get_all_directory_info : get all the info of a given directory
--
-- dir_name : name of the directory
--
-- output : d.DirID, d.UID, u.UserName, d.GID, g.GroupName, d.Status, d.Mode, d.CreationDate, d.ModificationDate

DROP PROCEDURE IF EXISTS ps_get_all_directory_info;
DELIMITER //
CREATE PROCEDURE ps_get_all_directory_info
(IN dir_name VARCHAR(255))
BEGIN

    SELECT SQL_NO_CACHE d.DirID, d.UID, u.UserName, d.GID, g.GroupName, d.Status, d.Mode, d.CreationDate, d.ModificationDate
    FROM FC_DirectoryList d
    JOIN FC_Users u on d.UID = u.UID
    JOIN FC_Groups g on d.GID = g.GID
    WHERE Name = dir_name;
END //
DELIMITER ;


-- ps_get_all_directory_info_from_id : get all the info of a given directory
--
-- dir_id : id of the directory
--
-- output : d.DirID, d.UID, u.UserName, d.GID, g.GroupName, d.Status, d.Mode, d.CreationDate, d.ModificationDate

DROP PROCEDURE IF EXISTS ps_get_all_directory_info_from_id;
DELIMITER //
CREATE PROCEDURE ps_get_all_directory_info_from_id
(IN dir_id INT)
BEGIN


    SELECT SQL_NO_CACHE d.DirID, d.UID, u.UserName, d.GID, g.GroupName, d.Status, d.Mode, d.CreationDate, d.ModificationDate
    FROM FC_DirectoryList d
    JOIN FC_Users u on d.UID = u.UID
    JOIN FC_Groups g on d.GID = g.GID
    WHERE d.DirID = dir_id;
END //
DELIMITER ;


-- ps_set_dir_gid : change group of a directory
--
-- dir_name : name of the directory
-- gid : new group
--
-- output errno, affected column (should be 0/1), errmsg. if errno == 0, all good

DROP PROCEDURE IF EXISTS ps_set_dir_gid;
DELIMITER //
CREATE PROCEDURE ps_set_dir_gid
(IN  dir_name VARCHAR(255), IN gid INT)
BEGIN

  UPDATE FC_DirectoryList SET GID = gid, ModificationDate = UTC_TIMESTAMP() WHERE Name = dir_name;

  SELECT 0 as errno, ROW_COUNT() as affected, 'OK' as errmsg;

END //
DELIMITER ;

-- ps_set_dir_gid_recursive : change group of a directory recursively
--
-- dir_name : name of the directory
-- gid : new gid
--
-- output errno, affected column, errmsg. if errno == 0, all good

DROP PROCEDURE IF EXISTS ps_set_dir_gid_recursive;
DELIMITER //
CREATE PROCEDURE ps_set_dir_gid_recursive
(IN  dir_name VARCHAR(255), IN gid INT)
BEGIN
  DECLARE dirUpdate INT DEFAULT 0;
  DECLARE fileUpdate INT DEFAULT 0;
  DECLARE start_dirId INT DEFAULT 0;

  SELECT SQL_NO_CACHE DirID INTO start_dirId from FC_DirectoryList where Name = dir_name;
  IF start_dirId IS NULL THEN
    SET start_dirId = 0;
  END IF;

  UPDATE FC_DirectoryList d
  JOIN  FC_DirectoryClosure c
  ON d.DirID = c.ChildID
  SET d.GID = gid, d.ModificationDate = UTC_TIMESTAMP()
  WHERE c.ParentID = start_dirID;


  SELECT ROW_COUNT() INTO dirUpdate;

  UPDATE FC_Files f
  JOIN FC_DirectoryClosure c ON f.DirID = c.ChildID
  SET f.GID = gid, f.ModificationDate = UTC_TIMESTAMP()
  WHERE c.ParentID = start_dirId;

  SELECT ROW_COUNT() INTO fileUpdate;

  SELECT 0 as errno, (fileUpdate + dirUpdate) as affected, 'OK' as errmsg;

END //
DELIMITER ;

-- ps_set_dir_uid : change owner of a directory
--
-- dir_name : name of the directory
-- uid : new uid
--
-- output errno, affected column (should be 0/1), errmsg. if errno == 0, all good

DROP PROCEDURE IF EXISTS ps_set_dir_uid;
DELIMITER //
CREATE PROCEDURE ps_set_dir_uid
(IN  dir_name VARCHAR(255), IN uid INT)
BEGIN

  UPDATE FC_DirectoryList SET UID = uid, ModificationDate = UTC_TIMESTAMP() WHERE Name = dir_name;

  SELECT 0 as errno, ROW_COUNT() as affected, 'OK' as errmsg;

END //
DELIMITER ;


-- ps_set_dir_uid_recursive : change owner of a directory recursively
--
-- dir_name : name of the directory
-- uid : new uid
--
-- output errno, affected column (should be 0/1), errmsg. if errno == 0, all good

DROP PROCEDURE IF EXISTS ps_set_dir_uid_recursive;
DELIMITER //
CREATE PROCEDURE ps_set_dir_uid_recursive
(IN  dir_name VARCHAR(255), IN uid INT)
BEGIN
  DECLARE dirUpdate INT DEFAULT 0;
  DECLARE fileUpdate INT DEFAULT 0;
  DECLARE start_dirId INT DEFAULT 0;

  SELECT SQL_NO_CACHE DirID INTO start_dirId from FC_DirectoryList where Name = dir_name;
  IF start_dirId IS NULL THEN
    SET start_dirId = 0;
  END IF;

  UPDATE FC_DirectoryList d
  JOIN  FC_DirectoryClosure c
  ON d.DirID = c.ChildID
  SET d.UID = uid, d.ModificationDate = UTC_TIMESTAMP()
  WHERE c.ParentID = start_dirID;


  SELECT ROW_COUNT() INTO dirUpdate;

  UPDATE FC_Files f
  JOIN FC_DirectoryClosure c ON f.DirID = c.ChildID
  SET f.UID = uid, f.ModificationDate = UTC_TIMESTAMP()
  WHERE c.ParentID = start_dirId;

  SELECT ROW_COUNT() INTO fileUpdate;

  SELECT 0 as errno, (fileUpdate + dirUpdate) as affected, 'OK' as errmsg;

END //
DELIMITER ;


-- ps_set_dir_status : change status of a directory
--
-- dir_name : name of the directory
-- status_id : id of the new status
--
-- output errno, affected column (should be 0/1), errmsg. if errno == 0, all good

DROP PROCEDURE IF EXISTS ps_set_dir_status;
DELIMITER //
CREATE PROCEDURE ps_set_dir_status
(IN  dir_name VARCHAR(255), IN status_id INT)
BEGIN

  UPDATE FC_DirectoryList SET Status = status_id, ModificationDate = UTC_TIMESTAMP() WHERE Name = dir_name;

  SELECT 0 as errno, ROW_COUNT() as affected, 'OK' as errmsg;

END //
DELIMITER ;


-- ps_set_dir_mode : change mode of a directory
--
-- dir_name : name of the directory
-- mode : new mode
--
-- output errno, affected column (should be 0/1), errmsg. if errno == 0, all good

DROP PROCEDURE IF EXISTS ps_set_dir_mode;
DELIMITER //
CREATE PROCEDURE ps_set_dir_mode
(IN  dir_name VARCHAR(255), IN mode SMALLINT UNSIGNED)
BEGIN

  UPDATE FC_DirectoryList SET Mode = mode, ModificationDate = UTC_TIMESTAMP() WHERE Name = dir_name;

  SELECT 0 as errno, ROW_COUNT() as affected, 'OK' as errmsg;

END //
DELIMITER ;


-- ps_set_dir_mode_recursive : change Mode of a directory recursively
--
-- dir_name : name of the directory
-- mode : new mode
--
-- output errno, affected column (should be 0/1), errmsg. if errno == 0, all good

DROP PROCEDURE IF EXISTS ps_set_dir_mode_recursive;
DELIMITER //
CREATE PROCEDURE ps_set_dir_mode_recursive
(IN  dir_name VARCHAR(255), IN mode INT)
BEGIN
  DECLARE dirUpdate INT DEFAULT 0;
  DECLARE fileUpdate INT DEFAULT 0;
  DECLARE start_dirId INT DEFAULT 0;

  SELECT SQL_NO_CACHE DirID INTO start_dirId from FC_DirectoryList where Name = dir_name;
  IF start_dirId IS NULL THEN
    SET start_dirId = 0;
  END IF;

  UPDATE FC_DirectoryList d
  JOIN  FC_DirectoryClosure c
  ON d.DirID = c.ChildID
  SET d.Mode = mode, d.ModificationDate = UTC_TIMESTAMP()
  WHERE c.ParentID = start_dirID;


  SELECT ROW_COUNT() INTO dirUpdate;

  UPDATE FC_Files f
  JOIN FC_DirectoryClosure c ON f.DirID = c.ChildID
  SET f.Mode = mode, f.ModificationDate = UTC_TIMESTAMP()
  WHERE c.ParentID = start_dirId;

  SELECT ROW_COUNT() INTO fileUpdate;

  SELECT 0 as errno, (fileUpdate + dirUpdate) as affected, 'OK' as errmsg;

END //
DELIMITER ;

-- ps_get_files_in_dir : list of files in a directory
--
-- dir_id : id of the directory
--
-- output : FileID, DirID, FileName
--

DROP PROCEDURE IF EXISTS ps_get_files_in_dir;
DELIMITER //
CREATE PROCEDURE ps_get_files_in_dir
(IN dir_id INT)
BEGIN

  SELECT SQL_NO_CACHE FileID, DirID, FileName FROM FC_Files f WHERE DirID = dir_id;

END //
DELIMITER ;


-- ps_get_dir_logical_size : returns the logical size of a directory (irrelevant of amount of replicas),
--                           as written in DirectoryUsage
--
-- dir_id : id of the directory
-- recursiveSum: take subdirectories into account
--
-- output : File Size, number of files

DROP PROCEDURE IF EXISTS ps_get_dir_logical_size;
DELIMITER //
CREATE PROCEDURE ps_get_dir_logical_size
(IN dir_id INT, IN recursiveSum BOOLEAN)
BEGIN
    DECLARE log_size, log_files BIGINT DEFAULT 0;

    IF recursiveSum THEN

      SELECT SQL_NO_CACHE COALESCE(SUM(SESize), 0), COALESCE(SUM(SEFiles),0) FROM FC_DirectoryUsage u
      JOIN FC_DirectoryClosure c on c.ChildID = u.DirID
      JOIN FC_StorageElements s ON s.SEID = u.SEID
      WHERE s.SEName = 'FakeSE'
      AND c.ParentID = dir_id;  

    ELSE

      SELECT SQL_NO_CACHE SESize, SEFiles INTO log_size, log_files FROM FC_DirectoryUsage u
      JOIN FC_StorageElements s ON s.SEID = u.SEID
      WHERE s.SEName = 'FakeSE'
      AND u.DirID = dir_id;    

      SELECT COALESCE(log_size, 0), COALESCE(log_files,0);
      
    END IF;

END //
DELIMITER ;



-- ps_calculate_dir_logical_size : calculate the logical size of a directory (irrelevant of amount of replicas),
--                           It should be equal to ps_get_dir_logical_size
--
-- dir_id : id of the directory
-- recursiveSum: take subdirectories into account
--
-- output : File Size, number of files

DROP PROCEDURE IF EXISTS ps_calculate_dir_logical_size;
DELIMITER //
CREATE PROCEDURE ps_calculate_dir_logical_size
(IN dir_id INT, IN recursiveSum BOOLEAN)
BEGIN
  DECLARE log_size BIGINT DEFAULT 0;
  DECLARE log_files INT DEFAULT 0;

  IF recursiveSum THEN


    SELECT SQL_NO_CACHE COALESCE(SUM(f.Size),0), COUNT(*) INTO log_size, log_files FROM FC_Files f
    JOIN FC_DirectoryClosure d ON f.DirID = d.ChildID
    WHERE ParentID = dir_id;

    -- IF log_size IS NULL THEN
    --   SET log_size = 0;
    -- END IF;

    
  
  ELSE

    SELECT SQL_NO_CACHE COALESCE(SUM(f.Size),0), COUNT(*) INTO log_size, log_files FROM FC_Files f
    WHERE DirID = dir_id;

  END IF;

  select COALESCE(log_size, 0), COALESCE(log_files, 0);

END //
DELIMITER ;



-- ps_get_dir_physical_size : get the physical size of a directory on each SE from DirectoryUsage,
--                           It should be equal to ps_calculate_dir_physical_size
--
-- dir_id : id of the directory
-- recursiveSum: take subdirectories into account
--
-- output : SEName, File Size, number of files

DROP PROCEDURE IF EXISTS ps_get_dir_physical_size;
DELIMITER //
CREATE PROCEDURE ps_get_dir_physical_size
(IN dir_id INT, IN recursiveSum BOOLEAN)
BEGIN

  IF recursiveSum THEN
    SELECT SQL_NO_CACHE SEName, COALESCE(SUM(SESize), 0), COALESCE(SUM(SEFiles), 0)
    FROM FC_DirectoryUsage u
    JOIN FC_DirectoryClosure c on u.DirID = c.ChildID
    JOIN FC_StorageElements se ON se.SEID = u.SEID
    WHERE c.ParentID = dir_id
    AND SEName != 'FakeSE'
    AND (SESize != 0 OR SEFiles != 0)
    GROUP BY se.SEName
    ORDER BY NULL;
  
  ELSE

    SELECT SQL_NO_CACHE SEName, COALESCE(SUM(SESize), 0), COALESCE(SUM(SEFiles), 0)
    FROM FC_DirectoryUsage u
    JOIN FC_StorageElements se ON se.SEID = u.SEID
    WHERE u.DirID = dir_id
    AND SEName != 'FakeSE'
    AND (SESize != 0 OR SEFiles != 0)
    GROUP BY se.SEName
    ORDER BY NULL;
  
  END IF;

END //
DELIMITER ;



-- ps_calculate_dir_physical_size : calculate the physical size of a directory on each SE,
--                           It should be equal to ps_get_dir_physical_size
--
-- dir_id : id of the directory
-- recursiveSum: take subdirectories into account
--
-- output : SEName, File Size, number of files

DROP PROCEDURE IF EXISTS ps_calculate_dir_physical_size;
DELIMITER //
CREATE PROCEDURE ps_calculate_dir_physical_size
(IN dir_id INT, IN recursiveSum BOOLEAN)
BEGIN

  IF recursiveSum THEN

    SELECT SQL_NO_CACHE se.SEName, COALESCE(SUM(f.Size), 0), count(*)
    FROM FC_Replicas r
    JOIN FC_Files f ON f.FileID = r.FileID
    JOIN FC_StorageElements se ON se.SEID = r.SEID
    JOIN FC_DirectoryClosure dc ON dc.ChildID = f.DirID
    WHERE dc.ParentID = dir_id
    GROUP BY se.SEName
    ORDER BY NULL;
  
  ELSE
    SELECT SQL_NO_CACHE se.SEName, COALESCE(SUM(f.Size), 0), count(*)
    FROM FC_Replicas r
    JOIN FC_Files f ON f.FileID = r.FileID
    JOIN FC_StorageElements se ON se.SEID = r.SEID
    WHERE f.DirID = dir_id
    GROUP BY se.SEName
    ORDER BY NULL;

  END IF;

END //
DELIMITER ;



-- Rebuild the directoryUsage table

DROP PROCEDURE IF EXISTS ps_rebuild_directory_usage;
DELIMITER //
CREATE PROCEDURE ps_rebuild_directory_usage()
BEGIN

  DECLARE exit handler for sqlexception
    BEGIN
    ROLLBACK;
    RESIGNAL;
  END;

  START TRANSACTION;

  DELETE FROM FC_DirectoryUsage;

  INSERT INTO FC_DirectoryUsage (DirID, SEID, SESize, SEFiles)
    SELECT SQL_NO_CACHE DirID, 1 as SEID, sum(Size) as SESize, count(*) as SEFiles
    FROM FC_Files
    GROUP BY DirID
    ORDER BY NULL;

  INSERT INTO FC_DirectoryUsage (DirID, SEID, SESize, SEFiles)
    SELECT SQL_NO_CACHE DirID, SEID, sum(Size) as SESize, count(*) as SEFiles
    FROM FC_Replicas r
    JOIN FC_Files f ON r.FileID = f.FileID
    GROUP BY DirID, SEID
    ORDER BY NULL;

  COMMIT;
END //
DELIMITER ;


-- Rebuild the directoryUsage table for one directory
DROP PROCEDURE IF EXISTS ps_rebuild_directory_usage_for_dir;
DELIMITER //
CREATE PROCEDURE ps_rebuild_directory_usage_for_dir
(IN dir_id INT)
BEGIN

  DECLARE exit handler for sqlexception
    BEGIN
    ROLLBACK;
    RESIGNAL;
  END;

  START TRANSACTION;

  DELETE FROM FC_DirectoryUsage where DirID = dir_id;

  INSERT INTO FC_DirectoryUsage (DirID, SEID, SESize, SEFiles)
    SELECT SQL_NO_CACHE DirID, 1 as SEID, sum(Size) as SESize, count(*) as SEFiles
    FROM FC_Files
    WHERE DirID = dir_id
    GROUP BY DirID
    ORDER BY NULL;

  INSERT INTO FC_DirectoryUsage (DirID, SEID, SESize, SEFiles)
    SELECT SQL_NO_CACHE DirID, SEID, sum(Size) as SESize, count(*) as SEFiles
    FROM FC_Replicas r
    JOIN FC_Files f ON r.FileID = f.FileID
    WHERE f.DirID = dir_id
    GROUP BY DirID, SEID
    ORDER BY NULL;

  COMMIT;
END //
DELIMITER ;



-- ps_get_full_lfn_for_file_ids : get the full lfn for given file ids
-- file_ids : list of file ids
-- output : FileID, lfn

DROP PROCEDURE IF EXISTS ps_get_full_lfn_for_file_ids;
DELIMITER //
CREATE PROCEDURE ps_get_full_lfn_for_file_ids
(IN file_ids TEXT)
BEGIN

  SET @sql = CONCAT('SELECT SQL_NO_CACHE f.FileID, CONCAT(d.Name, "/", f.FileName)
                     FROM FC_Files f
                     JOIN FC_DirectoryList d ON f.DirID = d.DirID
                     WHERE f.FileID IN (', file_ids, ')');

  PREPARE stmt FROM @sql;
  EXECUTE stmt;
  DEALLOCATE PREPARE stmt;

END //
DELIMITER ;



-- ps_get_se_dump : dump all the lfns in an SE, with checksum and size
-- se_id : storageElement's ID
-- output : LFN, Checksum, Size

DROP PROCEDURE IF EXISTS ps_get_se_dump;
DELIMITER //
CREATE PROCEDURE ps_get_se_dump
(IN se_id INT)
BEGIN

  SELECT SQL_NO_CACHE CONCAT(d.Name, '/', f.FileName), f.Checksum, f.Size
         FROM FC_Files f
         JOIN FC_Replicas r on f.FileID = r.FileID
         JOIN FC_DirectoryList d on d.DirID = f.DirID
         WHERE SEID = se_id;

END //
DELIMITER ;



-- Consistency checks


-- useless users
-- SELECT u.* from FC_Users u LEFT OUTER JOIN (select distinct(UID) From FC_DirectoryInfo UNION select distinct(UID) from FC_Files) i ON u.UID = i.UID WHERE i.UID IS NULL;

-- We will find some files and dirs with non exisiting users. Some already come from the LFC
-- the otehrs is the mapping between the root lfc and the root dfc. 0 needs to be transformed into 1

-- Files with non existing users
-- SELECT f.* from FC_Files f LEFT OUTER JOIN FC_Users u on f.UID = u.UID WHERE u.UID IS NULL;

-- Directory with non existing users
-- SELECT d.*, di.* from FC_DirectoryInfo di LEFT OUTER JOIN FC_Users u on di.UID = u.UID INNER JOIN FC_DirectoryList d on d.DirID = di.DirID WHERE u.UID IS NULL;

-- useless groups
-- we have some. What to do?
-- SELECT g.* from FC_Groups g LEFT OUTER JOIN (select distinct(GID) From FC_DirectoryInfo UNION select distinct(GID) from FC_Files) i ON g.GID = i.GID WHERE i.GID IS NULL;

-- here we need to convert 0 to 1 for the GID again (root mapping)

-- Files with non existing groups
-- SELECT f.* from FC_Files f LEFT OUTER JOIN FC_Groups g on f.GID = g.GID WHERE g.GID IS NULL;

-- Directory with non existing groups
-- SELECT d.*, di.* from FC_DirectoryInfo di LEFT OUTER JOIN FC_Groups g on di.GID = g.GID INNER JOIN FC_DirectoryList d on d.DirID = di.DirID WHERE g.GID IS NULL;

-- Replicas without Files
-- SELECT r.* FROM FC_Replicas r LEFT OUTER JOIN FC_Files f on r.FileID = f.FileID WHERE f.FileID IS NULL;

-- Files without Replicas
-- we do have some, what to do?
-- SELECT f.* FROM FC_Replicas r RIGHT OUTER JOIN FC_Files f on r.FileID = f.FileID WHERE r.FileID IS NULL;

-- Files without Directory
-- SELECT f.* FROM FC_Files f LEFT OUTER JOIN FC_DirectoryList d on f.DirID = d.DirID where d.DirID is NULL;



-- drop indexes for migration from LFC
-- ALTER TABLE FC_Files DROP FOREIGN KEY FC_Files_ibfk_1,
--                      DROP FOREIGN KEY FC_Files_ibfk_2,
--                      DROP FOREIGN KEY FC_Files_ibfk_3,
--                      DROP FOREIGN KEY FC_Files_ibfk_4,
--                      DROP INDEX DirID,
--                      DROP INDEX GUID,
--                      DROP INDEX GID,
--                      DROP INDEX UID,
--                      DROP INDEX Status,
--                      DROP INDEX FileName;
--
-- ALTER TABLE FC_DirectoryList DROP FOREIGN KEY FC_DirectoryList_ibfk_1,
--                              DROP FOREIGN KEY FC_DirectoryList_ibfk_2,
--                              DROP FOREIGN KEY FC_DirectoryList_ibfk_3,
--                              DROP INDEX Name,
--                              DROP INDEX UID,
--                              DROP INDEX GID,
--                              DROP INDEX Status;
--
-- ALTER TABLE FC_Replicas DROP FOREIGN KEY FC_Replicas_ibfk_1,
--                         DROP FOREIGN KEY FC_Replicas_ibfk_2,
--                         DROP FOREIGN KEY FC_Replicas_ibfk_3,
--                         DROP INDEX FileID,
--                         DROP INDEX SEID,
--                         DROP INDEX Status;


-- recreate indexes  AFTER MIGRATION

-- alter table FC_Files ADD FOREIGN KEY (DirID) REFERENCES FC_DirectoryList(DirID) ON DELETE CASCADE,
--             ADD FOREIGN KEY (Status) REFERENCES FC_Statuses(StatusID),
--             ADD FOREIGN KEY (UID) REFERENCES FC_Users(UID),
--             ADD FOREIGN KEY (GID) REFERENCES FC_Groups(GID),
--             ADD UNIQUE (DirID, FileName),
--             ADD UNIQUE(GUID),
--             ADD INDEX (UID,GID),
--             ADD INDEX (Status),
--             ADD INDEX (FileName);
--
--
-- ALTER TABLE FC_DirectoryList ADD FOREIGN KEY (UID) REFERENCES FC_Users(UID),
--                              ADD FOREIGN KEY (GID) REFERENCES FC_Groups(GID),
--                              ADD FOREIGN KEY (Status) REFERENCES FC_Statuses(StatusID),
--                              ADD UNIQUE (Name);
--
-- ALTER TABLE FC_Replicas ADD FOREIGN KEY (FileID) REFERENCES FC_Files(FileID),
--                         ADD FOREIGN KEY (SEID) REFERENCES FC_StorageElements(SEID),
--                         ADD FOREIGN KEY (Status) REFERENCES FC_Statuses(StatusID),
--                         ADD UNIQUE (FileID,SEID);

SET FOREIGN_KEY_CHECKS = 1;
