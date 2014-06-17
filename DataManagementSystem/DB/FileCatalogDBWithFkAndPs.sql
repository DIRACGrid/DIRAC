-- $HeaderURL $
-- ------------------------------------------------------------------------------
DROP DATABASE IF EXISTS FileCatalogDB;
-- ------------------------------------------------------------------------------
CREATE DATABASE FileCatalogDB;

use mysql;
-- options to set in the db
-- transaction-isolation = READ-COMMITTED

-- delete from user where user='Dirac';
--
-- Must set passwords for database user by replacing "must_be_set".
--


GRANT SELECT,INSERT,LOCK TABLES,CREATE TEMPORARY TABLES,UPDATE,DELETE,CREATE,DROP,ALTER ON FileCatalogDB.* TO Dirac@'%' IDENTIFIED BY  'to_be_changed';
GRANT ALTER ROUTINE, CREATE ROUTINE, EXECUTE ON FileCatalogDB.* TO  Dirac@'%' IDENTIFIED BY 'to_be_changed';
GRANT TRIGGER ON FileCatalogDB.* TO  Dirac@'%' IDENTIFIED BY 'to_be_changed';

FLUSH PRIVILEGES;

USE FileCatalogDB;
SET FOREIGN_KEY_CHECKS = 0;

-- ------------------------------------------------------------------------------
DROP TABLE IF EXISTS FC_Statuses;
CREATE TABLE FC_Statuses (
    StatusID INT AUTO_INCREMENT,
    Status VARCHAR(32),
    
    PRIMARY KEY (StatusID),
    
    UNIQUE(Status)
) ENGINE = INNODB;

INSERT INTO FC_Statuses (StatusID, Status) values (1, 'FakeStatus');

-- -----------------------------------------------------------------------------

drop table if exists FC_StorageElements;
CREATE TABLE FC_StorageElements (
    SEID INTEGER AUTO_INCREMENT,
    SEName VARCHAR(127) CHARACTER SET latin1 COLLATE latin1_bin NOT NULL,
    AliasName VARCHAR(127) DEFAULT '',
    
    PRIMARY KEY (SEID),
    
    UNIQUE (SEName)
) ENGINE = INNODB;

INSERT INTO FC_StorageElements (SEID, SEName) values (1, 'FakeSE');

-- ------------------------------------------------------------------------------

drop table if exists FC_Groups;
CREATE TABLE FC_Groups (
    GID INTEGER NOT NULL AUTO_INCREMENT,
    GroupName VARCHAR(127) NOT NULL,
    
    PRIMARY KEY (GID),
    
    UNIQUE (GroupName)
) ENGINE = INNODB;

INSERT INTO FC_Groups (GID, GroupName) values (1, 'root');

-- ------------------------------------------------------------------------------
drop table if exists FC_Users;
CREATE TABLE FC_Users (
    UID INTEGER NOT NULL AUTO_INCREMENT,
    UserName VARCHAR(127) NOT NULL,
    
    PRIMARY KEY (UID),
    
    UNIQUE (UserName)
) ENGINE = INNODB;

INSERT INTO FC_Users (UID, UserName) values (1, 'root');
-- ------------------------------------------------------------------------------



drop table if exists FC_DirectoryList;
create table FC_DirectoryList (
  DirID INT NOT NULL AUTO_INCREMENT,
  Name varchar(255)CHARACTER SET latin1 COLLATE latin1_bin NOT NULL,

  PRIMARY KEY (DirID),
  
  UNIQUE (Name)
) ENGINE=INNODB;

-- ------------------------------------------------------------------------------
drop table if exists FC_DirectoryClosure;
CREATE TABLE FC_DirectoryClosure (
 ClosureID INT NOT NULL AUTO_INCREMENT,
 ParentID INT NOT NULL,
 ChildID INT NOT NULL,
 Depth INT NOT NULL DEFAULT 0,
 
 PRIMARY KEY (ClosureID),
 FOREIGN KEY (ParentID) REFERENCES FC_DirectoryList(DirID) ON DELETE CASCADE,
 FOREIGN KEY (ChildID) REFERENCES FC_DirectoryList(DirID) ON DELETE CASCADE,
 
 INDEX (ParentID),
 INDEX (ChildID)
) ENGINE = INNODB;
-- ------------------------------------------------------------------------------
-- insert into FC_DirectoryList (DirID, Name) values (1, '/'); 
-- insert into FC_DirectoryClosure (ParentID, ChildID) values (1,1);



drop table if exists FC_DirectoryInfo;
CREATE TABLE FC_DirectoryInfo (
    DirID INTEGER NOT NULL,
    UID INTEGER NOT NULL DEFAULT 0,
    GID INTEGER NOT NULL DEFAULT 0,
    CreationDate DATETIME,
    ModificationDate DATETIME,
    Mode SMALLINT UNSIGNED NOT NULL DEFAULT 775,
    Status INTEGER NOT NULL DEFAULT 0,
    
    PRIMARY KEY (DirID),
    FOREIGN KEY (DirID) REFERENCES FC_DirectoryList(DirID) ON DELETE CASCADE,
    FOREIGN KEY (UID) REFERENCES FC_Users(UID),
    FOREIGN KEY (GID) REFERENCES FC_Groups(GID),
    FOREIGN KEY (Status) REFERENCES FC_Statuses(StatusID)
) ENGINE = INNODB;

-- -------------------------------------------------------------------------------


drop table if exists FC_Files;
CREATE TABLE FC_Files(
    FileID INT AUTO_INCREMENT,
    DirID INT NOT NULL,
    Size BIGINT UNSIGNED NOT NULL,
    UID INT NOT NULL,
    GID INT NOT NULL,
    Status INT NOT NULL,
    FileName VARCHAR(128) CHARACTER SET latin1 COLLATE latin1_bin NOT NULL,

    PRIMARY KEY (FileID),
    FOREIGN KEY (DirID) REFERENCES FC_DirectoryList(DirID) ON DELETE CASCADE,
    FOREIGN KEY (Status) REFERENCES FC_Statuses(StatusID),
    FOREIGN KEY (UID) REFERENCES FC_Users(UID),
    FOREIGN KEY (GID) REFERENCES FC_Groups(GID),
    
    UNIQUE (DirID, FileName),
    
    INDEX (UID,GID),
    INDEX (Status),
    INDEX (FileName)

) ENGINE = INNODB;

-- ------------------------------------------------------------------------------
drop table if exists FC_FileInfo;
CREATE TABLE FC_FileInfo (
    FileID INTEGER NOT NULL,
    GUID char(36) NOT NULL,
    Checksum VARCHAR(32),
    CheckSumType ENUM('Adler32','MD5'),
    Type ENUM('File','Link') NOT NULL DEFAULT 'File',
    CreationDate DATETIME,
    ModificationDate DATETIME,
    Mode SMALLINT UNSIGNED NOT NULL DEFAULT 775,
    
    PRIMARY KEY (FileID),
    FOREIGN KEY (FileID) REFERENCES FC_Files(FileID) ON DELETE CASCADE,
    
    UNIQUE(GUID)


) ENGINE = INNODB;

-- -----------------------------------------------------------------------------
-- do we want the delete on cascade on the SE?
drop table if exists FC_Replicas;
CREATE TABLE FC_Replicas (
    RepID INT AUTO_INCREMENT,
    FileID INT NOT NULL,
    SEID INTEGER NOT NULL,
    Status INTEGER NOT NULL,

    
    PRIMARY KEY (RepID),
    FOREIGN KEY (FileID) REFERENCES FC_Files(FileID),
    FOREIGN KEY (SEID) REFERENCES FC_StorageElements(SEID), 
    FOREIGN KEY (Status) REFERENCES FC_Statuses(StatusID),
    
    UNIQUE (FileID,SEID)


) ENGINE = INNODB;

-- ------------------------------------------------------------------------------
drop table if exists FC_ReplicaInfo;
CREATE TABLE FC_ReplicaInfo (
    RepID INTEGER NOT NULL,
    RepType ENUM ('Master','Replica') NOT NULL DEFAULT 'Master',
    CreationDate DATETIME,
    ModificationDate DATETIME,
    PFN VARCHAR(1024),  
    
    PRIMARY KEY (RepID),
    FOREIGN KEY (RepID) REFERENCES FC_Replicas(RepID) ON DELETE CASCADE
) ENGINE = INNODB;




-- ------------------------------------------------------------------------------

DROP TABLE IF EXISTS FC_DirectoryUsage;
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

drop table if exists FC_DirMeta;
CREATE TABLE FC_DirMeta (
    DirID INTEGER NOT NULL,
    MetaKey VARCHAR(31) CHARACTER SET latin1 COLLATE latin1_bin NOT NULL DEFAULT 'Noname',
    MetaValue VARCHAR(31) NOT NULL DEFAULT 'Noname',
    PRIMARY KEY (DirID,MetaKey)
) ENGINE = INNODB;

-- ------------------------------------------------------------------------------
drop table if exists FC_FileMeta;
CREATE TABLE FC_FileMeta (
    FileID INTEGER NOT NULL,
    MetaKey VARCHAR(31) CHARACTER SET latin1 COLLATE latin1_bin NOT NULL DEFAULT 'Noname',
    MetaValue VARCHAR(31) NOT NULL DEFAULT 'Noname',
    PRIMARY KEY (FileID,MetaKey)
) ENGINE = INNODB;


-- ------------------------------------------------------------------------------
drop table if exists FC_MetaFields;
CREATE TABLE FC_MetaFields (
  MetaID INT AUTO_INCREMENT PRIMARY KEY,
  MetaName VARCHAR(64) CHARACTER SET latin1 COLLATE latin1_bin NOT NULL,
  MetaType VARCHAR(128) NOT NULL
) ENGINE = INNODB;

-- ------------------------------------------------------------------------------
drop table if exists FC_FileMetaFields;
CREATE TABLE FC_FileMetaFields (
  MetaID INT AUTO_INCREMENT PRIMARY KEY,
  MetaName VARCHAR(64) CHARACTER SET latin1 COLLATE latin1_bin NOT NULL,
  MetaType VARCHAR(128) NOT NULL
) ENGINE = INNODB;

-- ------------------------------------------------------------------------------
drop table if exists FC_MetaSetNames;
CREATE TABLE FC_MetaSetNames (
  MetaSetID INT AUTO_INCREMENT PRIMARY KEY,
  MetaSetName VARCHAR(64)  NOT NULL,
  UNIQUE INDEX (MetaSetName)
) ENGINE = INNODB;

-- ------------------------------------------------------------------------------
drop table if exists FC_FileAncestors;
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
drop table if exists FC_MetaDatasets;
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
drop table if exists FC_MetaDatasetFiles;
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
drop table if exists FC_DatasetAnnotations;
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
  SELECT DirID INTO dir_id from FC_DirectoryList where Name = dirName;
  IF dir_id IS NULL THEN
    SET dir_id = 0;
  END IF;
  SELECT max(Depth) INTO dir_lvl FROM FC_DirectoryClosure WHERE ChildID = dir_id;
END //
DELIMITER ;

-- ps_find_dirs : returns the directory ids from multiple directory names
-- dirNames : list of directory name, coma separated

DROP PROCEDURE IF EXISTS ps_find_dirs;
DELIMITER //
CREATE PROCEDURE ps_find_dirs
(IN dirNames TEXT)
BEGIN
--   SELECT DirID from FC_DirectoryList where Name in (dirNames);
  SET @sql = CONCAT('SELECT Name, DirID from FC_DirectoryList where Name in (', dirNames, ')');
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
  
  DECLARE EXIT HANDLER FOR 1062 BEGIN
    ROLLBACK;
    SELECT 0 as dir_id, 'Error, duplicate key occurred' as msg;
  END;

  DECLARE EXIT HANDLER FOR 1452 BEGIN
    ROLLBACK;
    SELECT 0 as dir_id, 'Cannot add or update a child row: a foreign key constraint fails' as msg;
  END;
  
  DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN
      ROLLBACK;
      SELECT 0 as dir_id, 'Unknown error occured' as msg;
  END;
    
  
  START TRANSACTION;
   
    INSERT INTO FC_DirectoryList (Name) values (child_name);
    SELECT LAST_INSERT_ID() INTO dir_id; 
  
    INSERT INTO FC_DirectoryClosure (ParentID, ChildID, Depth ) VALUES (dir_id, dir_id, 0);
    
    IF parent_id != 0 THEN
      INSERT INTO FC_DirectoryClosure(ParentID, ChildID, depth)
        SELECT p.ParentID, c.ChildID, p.depth + c.depth + 1
        FROM FC_DirectoryClosure p, FC_DirectoryClosure c
        WHERE p.ChildID = parent_id AND c.ParentID = dir_id;
    END IF;
    
    INSERT INTO FC_DirectoryInfo (DirID, UID, GID, CreationDate, ModificationDate, Mode, Status) VALUES (dir_id, UID, GID, UTC_TIMESTAMP(), UTC_TIMESTAMP(), Mode, Status);
    
    SELECT dir_id, 'OK';
    
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
   SELECT Name INTO dirName from FC_DirectoryList where DirID = dir_id;
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
  SET @sql = CONCAT('SELECT DirID, Name from FC_DirectoryList where DirID in (', dirIds, ')');
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
   SELECT ParentID FROM FC_DirectoryClosure WHERE ChildID = dir_id order by Depth desc;
END //
DELIMITER ;


-- ps_get_direct_children : returns athe direct children of a directory
-- dir_id : directory id
-- returns : (directory id)
DROP PROCEDURE IF EXISTS ps_get_direct_children;
DELIMITER //
CREATE PROCEDURE ps_get_direct_children
(IN dir_id INT )
BEGIN
   SELECT d.DirID from FC_DirectoryList d JOIN FC_DirectoryClosure c on (d.DirID = c.ChildID) where c.ParentID = dir_id and c.Depth = 1;
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
      SELECT c1.ChildID, max(c1.Depth) AS lvl
      FROM FC_DirectoryClosure c1 
      JOIN FC_DirectoryClosure c2 ON c1.ChildID = c2.ChildID
      WHERE c2.ParentID = dir_id
      GROUP BY c1.ChildID 
      ORDER BY NULL;
    ELSE
      SELECT c1.ChildID, max(c1.Depth) AS lvl
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
  SET @sql = CONCAT('select distinct(ChildID) from FC_DirectoryClosure where ParentID in (',dirIds ,')');
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

  SELECT count(ChildID) INTO countDir FROM FC_DirectoryClosure WHERE ParentID = dir_id;
  
    IF NOT includeParent THEN
      IF countDir != 0 THEN
        set countDir = countDir - 1;
      END IF;
    END IF;
END //
DELIMITER ;

DROP PROCEDURE IF EXISTS ps_count_files_in_dir;
DELIMITER //
CREATE PROCEDURE ps_count_files_in_dir
(IN dir_id INT, OUT countFile INT )
BEGIN

  SELECT count(FileID) INTO countFile FROM FC_Files WHERE DirID = dir_id;
 
END //
DELIMITER ;




-- drop trigger if exists after_insert_replica_increase_size;
-- DELIMITER //
-- CREATE TRIGGER after_insert_replica_increase_size AFTER INSERT ON FC_Replicas
-- FOR EACH ROW
-- BEGIN
--   DECLARE dir_id INT;
--   DECLARE file_size BIGINT;
--   DECLARE done INT DEFAULT FALSE;
--   DECLARE cur1 CURSOR FOR SELECT ParentID FROM FC_DirectoryClosure c JOIN (SELECT DirID FROM FC_Files where FileID = new.FileID) f on f.DirID = c.ChildID;
--   DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = True;
--   
--   SELECT Size INTO file_size FROM FC_Files where FileID = new.FileID;
--   
--   OPEN cur1;
--   
--   update_loop: LOOP
--     FETCH cur1 INTO dir_id;
--     IF done THEN
--       LEAVE update_loop;
--     END IF;
--     
-- --     -- creates the initial line if it does not exist yet
-- --     INSERT IGNORE INTO FC_DirectoryUsage (DirID, SEID) values (dir_id, new.SEID);  
-- --     -- then add the values
-- --     UPDATE FC_DirectoryUsage SET SESize = SESize + file_size, SEFiles = SEFiles + 1  WHERE DirID = dir_id AND SEID = new.SEID;
--     
--     -- alternative
--     INSERT INTO FC_DirectoryUsage (DirID, SEID, SESize, SEFiles) VALUES (dir_id, new.SEID, file_size, 1) ON DUPLICATE KEY UPDATE  SESize = SESize + file_size, SEFiles = SEFiles + 1;
--     
--   END LOOP;
--   
--   CLOSE cur1;
-- END //
-- DELIMITER ;
-- 





/*
drop PROCEDURE if exists update_directory_usage;
DELIMITER //
CREATE PROCEDURE update_directory_usage 
(IN file_id INT, IN se_id INT, IN size_diff INT, IN file_diff INT)
-- file_id : the id of the file inserted
-- se_id : the id of the SE in which the replica was inserted
-- size_diff : the modification to bring to the size (positif if adding a replica, negatif otherwise)
-- file_diff : + or - 1 depending whether we add or remove a replica 
BEGIN
  DECLARE dir_id INT;
  DECLARE done INT DEFAULT FALSE;
--   DECLARE cur1 CURSOR FOR SELECT ParentID FROM FC_DirectoryClosure c JOIN (SELECT DirID FROM FC_Files where FileID = file_id) f on f.DirID = c.ChildID;
  DECLARE cur1 CURSOR FOR
    SELECT ParentID FROM FC_DirectoryClosure c
    JOIN FC_Files f ON f.DirID = c.ChildID
    WHERE FileID = file_id;

  DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = True;
  

  OPEN cur1;
  
  update_loop: LOOP
    FETCH cur1 INTO dir_id;
    IF done THEN
      LEAVE update_loop;
    END IF;
    
--     -- creates the initial line if it does not exist yet
--     INSERT IGNORE INTO FC_DirectoryUsage (DirID, SEID) values (dir_id, new.SEID);  
--     -- then add the values
--     UPDATE FC_DirectoryUsage SET SESize = SESize + file_size, SEFiles = SEFiles + 1  WHERE DirID = dir_id AND SEID = new.SEID;
    
    -- alternative
    -- If it is the first replica inserted for the given SE, then we insert the new row, otherwise we do an update
    INSERT INTO FC_DirectoryUsage (DirID, SEID, SESize, SEFiles) VALUES (dir_id, se_id, size_diff, file_diff) ON DUPLICATE KEY UPDATE  SESize = SESize + size_diff, SEFiles = SEFiles + file_diff;
    
  END LOOP;
  
  CLOSE cur1;
END //
DELIMITER ;
*/


drop PROCEDURE if exists update_directory_usage;
DELIMITER //
CREATE PROCEDURE update_directory_usage 
(IN top_dir_id INT, IN se_id INT, IN size_diff INT, IN file_diff INT)
-- top_dir_id : the id of the dir in which the mouvement starts
-- se_id : the id of the SE in which the replica was inserted
-- size_diff : the modification to bring to the size (positif if adding a replica, negatif otherwise)
-- file_diff : + or - 1 depending whether we add or remove a replica 
BEGIN
  DECLARE dir_id INT;
  DECLARE done INT DEFAULT FALSE;
--   DECLARE cur1 CURSOR FOR SELECT ParentID FROM FC_DirectoryClosure c JOIN (SELECT DirID FROM FC_Files where FileID = file_id) f on f.DirID = c.ChildID;
  DECLARE cur1 CURSOR FOR
    SELECT ParentID FROM FC_DirectoryClosure c
    WHERE c.ChildID = top_dir_id;

  DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = True;
  

  OPEN cur1;
  
  update_loop: LOOP
    FETCH cur1 INTO dir_id;
    IF done THEN
      LEAVE update_loop;
    END IF;
    
--     -- creates the initial line if it does not exist yet
--     INSERT IGNORE INTO FC_DirectoryUsage (DirID, SEID) values (dir_id, new.SEID);  
--     -- then add the values
--     UPDATE FC_DirectoryUsage SET SESize = SESize + file_size, SEFiles = SEFiles + 1  WHERE DirID = dir_id AND SEID = new.SEID;
    
    -- alternative
    -- If it is the first replica inserted for the given SE, then we insert the new row, otherwise we do an update
    INSERT INTO FC_DirectoryUsage (DirID, SEID, SESize, SEFiles) VALUES (dir_id, se_id, size_diff, file_diff) ON DUPLICATE KEY UPDATE  SESize = SESize + size_diff, SEFiles = SEFiles + file_diff;
    
  END LOOP;
  
  CLOSE cur1;
END //
DELIMITER ;




drop trigger if exists trg_after_insert_replica_increase_size;
DELIMITER //
CREATE TRIGGER trg_after_insert_replica_increase_size AFTER INSERT ON FC_Replicas
FOR EACH ROW
BEGIN

  DECLARE file_size BIGINT;
  DECLARE dir_id INT;
  
 
  SELECT Size, DirID INTO file_size, dir_id FROM FC_Files where FileID = new.FileID;
  
  
--   call update_directory_usage (new.FileID, new.SEID, file_size, 1);
  call update_directory_usage (dir_id, new.SEID, file_size, 1);

END //
DELIMITER ;


drop trigger if exists trg_after_delete_replica_decrease_size;
DELIMITER //
CREATE TRIGGER trg_after_delete_replica_decrease_size AFTER DELETE ON FC_Replicas
FOR EACH ROW
BEGIN

  DECLARE file_size BIGINT;
  DECLARE dir_id INT;
 
  SELECT Size, DirID INTO file_size, dir_id FROM FC_Files where FileID = old.FileID;
  
--   call update_directory_usage (old.FileID, old.SEID, -file_size, -1);
  call update_directory_usage (dir_id, old.SEID, -file_size, -1);

END //
DELIMITER ;


drop trigger if exists trg_after_update_replica_move_size;
DELIMITER //
CREATE TRIGGER trg_after_update_replica_move_size AFTER UPDATE ON FC_Replicas
FOR EACH ROW
BEGIN

  DECLARE file_size BIGINT;
  DECLARE dir_id INT;

  -- We only update if the replica was moved
  IF new.SEID <> old.SEID THEN

    SELECT Size, DirID INTO file_size, dir_id FROM FC_Files where FileID = old.FileID;
  

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





drop trigger if exists trg_after_insert_file_increase_size;
DELIMITER //
CREATE TRIGGER trg_after_insert_file_increase_size AFTER INSERT ON FC_Files
FOR EACH ROW
BEGIN
  DECLARE se_id INT;
  
  SELECT SEID INTO se_id FROM FC_StorageElements WHERE SEName = 'FakeSE';
  
  
  -- Use the fake SE
--   call update_directory_usage (new.FileID, se_id, new.Size, 1);
  call update_directory_usage (new.DirID, se_id, new.Size, 1);

END //
DELIMITER ;


drop trigger if exists trg_after_delete_file_decrease_size;
DELIMITER //
CREATE TRIGGER trg_after_delete_file_decrease_size AFTER DELETE ON FC_Files
FOR EACH ROW
BEGIN
  DECLARE se_id INT;
  
  SELECT SEID INTO se_id FROM FC_StorageElements WHERE SEName = 'FakeSE';
    
--   call update_directory_usage (old.FileID, se_id, -old.Size, -1);
  call update_directory_usage (old.DirID, se_id, -old.Size, -1);

END //
DELIMITER ;







-- example call ps_get_replicas_for_files_in_dir(6, False, "'APrioriGood'","'Trash'");

DROP PROCEDURE IF EXISTS ps_get_replicas_for_files_in_dir;
DELIMITER //
CREATE PROCEDURE ps_get_replicas_for_files_in_dir
(IN dir_id INT, IN allStatus BOOLEAN, IN visibleFileStatus VARCHAR(255), IN visibleReplicaStatus VARCHAR(255) )
BEGIN
-- select f.FileName, f.FileID, s.SEName, ri.PFN from FC_Replicas r join  FC_ReplicaInfo ri on ri.RepID = r.RepID join FC_Files f on f.FileID = r.FileID join FC_StorageElements s on s.SEID = r.SEID where DirID = 6;
  set @sql = 'select f.FileName, f.FileID, s.SEName, ri.PFN from FC_Replicas r join  FC_ReplicaInfo ri on ri.RepID = r.RepID join FC_Files f on f.FileID = r.FileID join FC_StorageElements s on s.SEID = r.SEID ';

  
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


--  explain SELECT f.FileName, f.FileID, SEName, PFN FROM FC_Files f JOIN (select FileID, RepId, Status, SEID from FC_Replicas) r on f.FileID = r.FileID  JOIN (SELECT RepID, PFN from FC_ReplicaInfo ) ri on r.RepID = ri.RepID JOIN (select SEID, SEName from FC_StorageElements) se on se.SEID = r.SEID where f.DirID = 1 and r.Status in (select StatusID from FC_Statuses where Status in ('a','b'));



DROP PROCEDURE IF EXISTS ps_get_file_id_from_lfn;
DELIMITER //
CREATE PROCEDURE ps_get_file_id_from_lfn
(IN lfn VARCHAR(255), OUT file_id INT )
BEGIN
  DECLARE done INT;
  DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;

-- SELECT d.Name, f.FileName,f.FileID FROM FC_Files f join FC_DirectoryList d on f.DirID = d.DirID WHERE (d.Name = '/vo.formation.idgrilles.fr/user/a/atsareg' and FileName in ('another testFile') ) OR (d.DirID = 6 and FileName in ('testfile'));
  SELECT FileID INTO file_id FROM FC_Files f JOIN FC_DirectoryList d ON f.DirID = f.DirID WHERE CONCAT_WS('/', d.Name, f.FileName) = lfn;
  IF file_id IS NULL THEN
    SET file_id = 0;
  END IF;
 
END //
DELIMITER ;


DROP PROCEDURE IF EXISTS ps_get_file_ids_from_dir_id;
DELIMITER //
CREATE PROCEDURE ps_get_file_ids_from_dir_id
(IN dir_id INT, IN file_names TEXT)
BEGIN
-- SELECT d.Name, f.FileName,f.FileID FROM FC_Files f join FC_DirectoryList d on f.DirID = d.DirID WHERE (d.Name = '/vo.formation.idgrilles.fr/user/a/atsareg' and FileName in ('another testFile') ) OR (d.DirID = 6 and FileName in ('testfile'));
  SET @sql = CONCAT('SELECT FileID, FileName FROM FC_Files f WHERE DirID = ', dir_id, ' AND FileName IN (', file_names, ')');
  PREPARE stmt FROM @sql;
  EXECUTE stmt;
  DEALLOCATE PREPARE stmt;
  
 
END //
DELIMITER ;



--  call ps_get_all_info_for_files_in_dir(6, True, "'testfile'", False, "'APrioriGood'");
--  call ps_get_all_info_for_files_in_dir(6, False, "useless", True, 'useless');

DROP PROCEDURE IF EXISTS ps_get_all_info_for_files_in_dir;
DELIMITER //
CREATE PROCEDURE ps_get_all_info_for_files_in_dir
(IN dir_id INT, IN specificFiles BOOLEAN, IN file_names TEXT, IN allStatus BOOLEAN, IN visibleFileStatus VARCHAR(255))
BEGIN
-- select FileName, DirID, f.FileID, Size, UserName, GroupName, f.Status, fi.* from FC_Files f join FC_FileInfo fi on f.FileID = fi.FileID join FC_Users u on f.UID = u.UID join FC_Groups g on f.GID = g.GID join FC_Statuses s on f.Status = s.StatusID; 



  set @sql = CONCAT('SELECT FileName, DirID, f.FileID, Size, f.uid, UserName, f.gid, GroupName, s.Status,
                     GUID, Checksum, ChecksumType, Type, CreationDate,ModificationDate, Mode
                    FROM FC_Files f
                    JOIN FC_FileInfo fi ON f.FileID = fi.FileID
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




DROP PROCEDURE IF EXISTS ps_get_all_info_for_file_ids;
DELIMITER //
CREATE PROCEDURE ps_get_all_info_for_file_ids
(IN file_ids TEXT)
BEGIN
-- SELECT d.Name, f.FileName,f.FileID FROM FC_Files f join FC_DirectoryList d on f.DirID = d.DirID WHERE (d.Name = '/vo.formation.idgrilles.fr/user/a/atsareg' and FileName in ('another testFile') ) OR (d.DirID = 6 and FileName in ('testfile'));
  SET @sql = CONCAT('SELECT f.FileID, Size, UID, GID, s.Status, GUID, CreationDate
                     FROM FC_Files f
                     JOIN FC_FileInfo fi ON f.FileID = fi.FileID
                     JOIN FC_Statuses s ON f.Status = s.StatusID
                     WHERE f.FileID IN (', file_ids, ')');
  PREPARE stmt FROM @sql;
  EXECUTE stmt;
  DEALLOCATE PREPARE stmt;
  
 
END //
DELIMITER ;



DROP PROCEDURE IF EXISTS ps_insert_file;
DELIMITER //
CREATE PROCEDURE ps_insert_file
(IN dir_id INT, IN size BIGINT, IN UID INT, IN GID INT,
 IN status_id INT, IN filename VARCHAR(255), IN GUID VARCHAR(36),
 IN checksum VARCHAR(32), IN checksumtype ENUM('Adler32','MD5'), IN mode SMALLINT )
BEGIN
  DECLARE file_id INT DEFAULT 0;
  
  DECLARE EXIT HANDLER FOR 1062 BEGIN
    ROLLBACK;
    SELECT 0 as file_id, 'Error, duplicate key occurred' as msg;
  END;

  DECLARE EXIT HANDLER FOR 1452 BEGIN
    ROLLBACK;
    SELECT 0 as file_id, 'Cannot add or update a child row: a foreign key constraint fails' as msg;
  END;
  
  DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN
      ROLLBACK;
      SELECT 0 as file_id, 'Unknown error occured' as msg;
  END;
    
  
  START TRANSACTION;
  INSERT INTO FC_Files (DirID, Size, UID, GID, Status, FileName) VALUES (dir_id, size, UID, GID, status_id, filename);
  SELECT LAST_INSERT_ID() INTO file_id;
  INSERT INTO FC_FileInfo (FileID, GUID, Checksum, CheckSumType, CreationDate, ModificationDate, Mode)
         VALUES (file_id, GUID, checksum, checksumtype, UTC_TIMESTAMP(), UTC_TIMESTAMP(), mode);
  COMMIT;
  
  SELECT file_id, 'OK' as msg;
 
END //
DELIMITER ;



DROP PROCEDURE IF EXISTS ps_get_file_ids_from_guids;
DELIMITER //
CREATE PROCEDURE ps_get_file_ids_from_guids
(IN  guids TEXT)
BEGIN
  SET @sql = CONCAT('SELECT GUID, FileID FROM FC_FileInfo f WHERE GUID IN (', guids, ')');
  PREPARE stmt FROM @sql;
  EXECUTE stmt;
  DEALLOCATE PREPARE stmt;
  
 
END //
DELIMITER ;


DROP PROCEDURE IF EXISTS ps_delete_replicas_from_file_ids;
DELIMITER //
CREATE PROCEDURE ps_delete_replicas_from_file_ids
(IN  file_ids TEXT)
BEGIN

  SET @sql = CONCAT('DELETE FROM FC_Replicas  WHERE FileID IN (', file_ids, ')');
  PREPARE stmt FROM @sql;
  EXECUTE stmt;
  DEALLOCATE PREPARE stmt;
  
  SELECT 0, 'OK';
  
END //
DELIMITER ;


DROP PROCEDURE IF EXISTS ps_delete_files;
DELIMITER //
CREATE PROCEDURE ps_delete_files
(IN  file_ids TEXT)
BEGIN

  SET @sql = CONCAT('DELETE FROM FC_Files  WHERE FileID IN (', file_ids, ')');
  PREPARE stmt FROM @sql;
  EXECUTE stmt;
  DEALLOCATE PREPARE stmt;
  
  SELECT 0, 'OK';
  
END //
DELIMITER ;


DROP PROCEDURE IF EXISTS ps_insert_replica;
DELIMITER //
CREATE PROCEDURE ps_insert_replica
(IN file_id INT, IN se_id INT, IN status_id INT,
 IN rep_type ENUM ('Master','Replica') , IN pfn VARCHAR(1024)
)
BEGIN
  DECLARE replica_id INT DEFAULT 0;
  
  DECLARE EXIT HANDLER FOR 1062 BEGIN
    ROLLBACK;
    SELECT RepID as replica_id, 'Replica already exists' as msg from FC_Replicas where FileID = file_id and SEID = se_id;
  END;

  DECLARE EXIT HANDLER FOR 1452 BEGIN
    ROLLBACK;
    SELECT 0 as replica_id, 'Cannot add or update a child row: a foreign key constraint fails' as msg;
  END;
  
  DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN
      ROLLBACK;
      SELECT 0 as replica_id, 'Unknown error occured' as msg;
  END;
    
  
  START TRANSACTION;
  INSERT INTO FC_Replicas (FileID, SEID, Status) VALUES (file_id, se_id, status_id);
  SELECT LAST_INSERT_ID() INTO replica_id;
  INSERT INTO FC_ReplicaInfo (RepID, RepType, CreationDate, ModificationDate, PFN)
         VALUES (replica_id, rep_type, UTC_TIMESTAMP(), UTC_TIMESTAMP(), pfn);
  COMMIT;
  
  SELECT replica_id, 'OK' as msg;
 
END //
DELIMITER ;



DROP PROCEDURE IF EXISTS ps_get_replica_id;
DELIMITER //
CREATE PROCEDURE ps_get_replica_id
(IN  file_id INT, IN se_id INT, OUT rep_id INT) 
BEGIN

  SELECT RepID INTO rep_id FROM FC_Replicas WHERE FileID = file_id AND SEID = se_id;
  
  IF rep_id IS NULL THEN
    SET rep_id = 0;
  END IF;
  
END //
DELIMITER ;





DROP PROCEDURE IF EXISTS ps_delete_replica_from_file_and_se_ids;
DELIMITER //
CREATE PROCEDURE ps_delete_replica_from_file_and_se_ids
(IN  file_id INT, IN se_id INT) 
BEGIN

  DELETE FROM FC_Replicas WHERE FileID = file_id AND SEID = se_id;
  
  SELECT 0, 'OK';

END //
DELIMITER ;


DROP PROCEDURE IF EXISTS ps_set_replica_status;
DELIMITER //
CREATE PROCEDURE ps_set_replica_status
(IN  file_id INT, IN se_id INT, IN status_id INT) 
BEGIN

  UPDATE FC_Replicas SET Status = status_id WHERE FileID = file_id AND SEID = se_id;
  
  SELECT 0 as errno, ROW_COUNT() as affected, 'OK' as msg;

END //
DELIMITER ;


DROP PROCEDURE IF EXISTS ps_set_replica_host;
DELIMITER //
CREATE PROCEDURE ps_set_replica_host
(IN  file_id INT, IN old_se_id INT, IN new_se_id INT)
BEGIN

  UPDATE FC_Replicas SET SEID = new_se_id WHERE FileID = file_id AND SEID = old_se_id;

  SELECT 0 as errno, ROW_COUNT() as affected, 'OK' as msg;

END //
DELIMITER ;



DROP PROCEDURE IF EXISTS ps_set_file_uid;
DELIMITER //
CREATE PROCEDURE ps_set_file_uid
(IN  file_id INT, IN in_uid INT) 
BEGIN
  DECLARE EXIT HANDLER FOR 1452 BEGIN
    ROLLBACK;
    SELECT 1 , 'Cannot add or update a child row: a foreign key constraint fails' as msg;
  END;
  
  UPDATE FC_Files SET UID = in_uid where FileID = file_id;
  UPDATE FC_FileInfo SET ModificationDate = UTC_TIMESTAMP() WHERE FileID = file_id;
  
  SELECT 0, 'OK';

END //
DELIMITER ;


DROP PROCEDURE IF EXISTS ps_set_file_gid;
DELIMITER //
CREATE PROCEDURE ps_set_file_gid
(IN  file_id INT, IN in_gid INT) 
BEGIN
  DECLARE EXIT HANDLER FOR 1452 BEGIN
    ROLLBACK;
    SELECT 1 , 'Cannot add or update a child row: a foreign key constraint fails' as msg;
  END;
  
  UPDATE FC_Files SET GID = in_gid where FileID = file_id;
  UPDATE FC_FileInfo SET ModificationDate = UTC_TIMESTAMP() WHERE FileID = file_id;
  
  SELECT 0, 'OK';

END //
DELIMITER ;

DROP PROCEDURE IF EXISTS ps_set_file_status;
DELIMITER //
CREATE PROCEDURE ps_set_file_status
(IN  file_id INT, IN status_id INT) 
BEGIN
  DECLARE EXIT HANDLER FOR 1452 BEGIN
    ROLLBACK;
    SELECT 1 , 'Cannot add or update a child row: a foreign key constraint fails' as msg;
  END;
  
  UPDATE FC_Files SET Status = status_id where FileID = file_id;
  UPDATE FC_FileInfo SET ModificationDate = UTC_TIMESTAMP() WHERE FileID = file_id;
  
  SELECT 0, 'OK';

END //
DELIMITER ;


DROP PROCEDURE IF EXISTS ps_set_file_mode;
DELIMITER //
CREATE PROCEDURE ps_set_file_mode
(IN  file_id INT, IN in_mode SMALLINT UNSIGNED)
BEGIN

  UPDATE FC_FileInfo SET Mode = in_mode, ModificationDate = UTC_TIMESTAMP()  WHERE FileID = file_id;

  SELECT 0, 'OK';

END //
DELIMITER ;





DROP PROCEDURE IF EXISTS ps_get_all_info_of_replicas;
DELIMITER //
CREATE PROCEDURE ps_get_all_info_of_replicas
(IN file_id INT, IN allStatus BOOLEAN, IN visibleReplicaStatus TEXT)
BEGIN
-- select FileName, DirID, f.FileID, Size, UserName, GroupName, f.Status, fi.* from FC_Files f join FC_FileInfo fi on f.FileID = fi.FileID join FC_Users u on f.UID = u.UID join FC_Groups g on f.GID = g.GID join FC_Statuses s on f.Status = s.StatusID; 


  IF allStatus THEN

    SELECT FileID, se.SEName, st.Status, RepType, CreationDate, ModificationDate, PFN
    FROM FC_Replicas r
    JOIN FC_ReplicaInfo ri on r.RepID = ri.RepID
    JOIN FC_StorageElements se on r.SEID = se.SEID
    JOIN FC_Statuses st on r.Status = st.StatusID
    WHERE FileID = file_id;

  ELSE

    SET @sql = CONCAT(
                    'SELECT FileID, se.SEName, st.Status, RepType, CreationDate, ModificationDate, PFN
                    FROM FC_Replicas r
                    JOIN FC_ReplicaInfo ri on r.RepID = ri.RepID
                    JOIN FC_StorageElements se on r.SEID = se.SEID
                    JOIN FC_Statuses st on r.Status = st.StatusID
                    WHERE FileID =',file_id,
                    ' and st.Status  in (',visibleReplicaStatus,') ');

    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

  END IF;

END //
DELIMITER ;


DROP PROCEDURE IF EXISTS ps_get_all_directory_info;
DELIMITER //
CREATE PROCEDURE ps_get_all_directory_info
(IN dir_name VARCHAR(255))
BEGIN


    SELECT d.DirID, di.UID, u.UserName, di.GID, g.GroupName, di.Status, di.Mode, di.CreationDate, di.ModificationDate
    FROM FC_DirectoryList d
    JOIN FC_DirectoryInfo di on d.DirID = di.DirID
    JOIN FC_Users u on di.UID = u.UID
    JOIN FC_Groups g on di.GID = g.GID
    WHERE Name = dir_name;

END //
DELIMITER ;


DROP PROCEDURE IF EXISTS ps_get_all_directory_info_from_id;
DELIMITER //
CREATE PROCEDURE ps_get_all_directory_info_from_id
(IN dir_id INT)
BEGIN


    SELECT d.DirID, di.UID, u.UserName, di.GID, g.GroupName, di.Status, di.Mode, di.CreationDate, di.ModificationDate
    FROM FC_DirectoryList d
    JOIN FC_DirectoryInfo di on d.DirID = di.DirID
    JOIN FC_Users u on di.UID = u.UID
    JOIN FC_Groups g on di.GID = g.GID
    WHERE d.DirID = dir_id;

END //
DELIMITER ;


DROP PROCEDURE IF EXISTS ps_set_dir_gid;
DELIMITER //
CREATE PROCEDURE ps_set_dir_gid
(IN  dir_name VARCHAR(255), IN gid INT)
BEGIN

  UPDATE FC_DirectoryInfo di JOIN FC_DirectoryList d ON d.DirID = di.DirID SET GID = gid, ModificationDate = UTC_TIMESTAMP() WHERE Name = dir_name;

  SELECT 0 as errno, ROW_COUNT() as affected, 'OK' as errmsg;

END //
DELIMITER ;



DROP PROCEDURE IF EXISTS ps_set_dir_uid;
DELIMITER //
CREATE PROCEDURE ps_set_dir_uid
(IN  dir_name VARCHAR(255), IN uid INT)
BEGIN

  UPDATE FC_DirectoryInfo di JOIN FC_DirectoryList d ON d.DirID = di.DirID SET UID = uid, ModificationDate = UTC_TIMESTAMP() WHERE Name = dir_name;

  SELECT 0 as errno, ROW_COUNT() as affected, 'OK' as errmsg;

END //
DELIMITER ;



DROP PROCEDURE IF EXISTS ps_set_dir_status;
DELIMITER //
CREATE PROCEDURE ps_set_dir_status
(IN  dir_name VARCHAR(255), IN status_id INT)
BEGIN

  UPDATE FC_DirectoryInfo di JOIN FC_DirectoryList d ON d.DirID = di.DirID SET Status = status_id, ModificationDate = UTC_TIMESTAMP() WHERE Name = dir_name;

  SELECT 0 as errno, ROW_COUNT() as affected, 'OK' as errmsg;

END //
DELIMITER ;

DROP PROCEDURE IF EXISTS ps_set_dir_mode;
DELIMITER //
CREATE PROCEDURE ps_set_dir_mode
(IN  dir_name VARCHAR(255), IN mode SMALLINT UNSIGNED)
BEGIN

  UPDATE FC_DirectoryInfo di JOIN FC_DirectoryList d ON d.DirID = di.DirID SET Mode = mode, ModificationDate = UTC_TIMESTAMP() WHERE Name = dir_name;

  SELECT 0 as errno, ROW_COUNT() as affected, 'OK' as errmsg;

END //
DELIMITER ;


DROP PROCEDURE IF EXISTS ps_get_files_in_dir;
DELIMITER //
CREATE PROCEDURE ps_get_files_in_dir
(IN dir_id INT)
BEGIN

  SELECT FileID, DirID, FileName FROM FC_Files f WHERE DirID = dir_id;

END //
DELIMITER ;


DROP PROCEDURE IF EXISTS ps_get_dir_logical_size;
DELIMITER //
CREATE PROCEDURE ps_get_dir_logical_size
(IN dir_id INT)
BEGIN

  SELECT SESize, SEFiles FROM FC_DirectoryUsage u
  JOIN FC_StorageElements s ON s.SEID = u.SEID 
  WHERE s.SEName = 'FakeSE'
  AND u.DirID = dir_id;


END //
DELIMITER ;



DROP PROCEDURE IF EXISTS ps_calculate_dir_logical_size;
DELIMITER //
CREATE PROCEDURE ps_calculate_dir_logical_size
(IN dir_id INT)
BEGIN
  DECLARE log_size BIGINT DEFAULT 0;
  DECLARE log_files INT DEFAULT 0;
  
  SELECT SUM(f.Size), COUNT(*) INTO log_size, log_files FROM FC_Files f
  JOIN FC_DirectoryClosure d ON f.DirID = d.ChildID
  WHERE ParentID = dir_id;
  
  IF log_size IS NULL THEN
    SET log_size = 0;
  END IF;
  
  select log_size, log_files;
END //
DELIMITER ;



DROP PROCEDURE IF EXISTS ps_get_dir_physical_size;
DELIMITER //
CREATE PROCEDURE ps_get_dir_physical_size
(IN dir_id INT)
BEGIN

  SELECT SEName, SESize, SEFiles
  FROM FC_DirectoryUsage u
  JOIN FC_StorageElements se ON se.SEID = u.SEID
  WHERE DirID = dir_id
  AND SEName != 'FakeSE'
  AND (SESize != 0 OR SEFiles != 0);


END //
DELIMITER ;



DROP PROCEDURE IF EXISTS ps_calculate_dir_physical_size;
DELIMITER //
CREATE PROCEDURE ps_calculate_dir_physical_size
(IN dir_id INT)
BEGIN

  SELECT se.SEName, sum(f.Size), count(*)
  FROM FC_Replicas r
  JOIN FC_Files f ON f.FileID = r.FileID
  JOIN FC_StorageElements se ON se.SEID = r.SEID
  JOIN FC_DirectoryClosure dc ON dc.ChildID = f.DirID
  WHERE dc.ParentID = dir_id
  GROUP BY se.SEID
  ORDER BY NULL; 

END //
DELIMITER ;





SET FOREIGN_KEY_CHECKS = 1;
