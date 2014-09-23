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



-- drop table if exists FC_DirectoryList;
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
-- drop table if exists FC_DirectoryInfo;
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


drop table if exists FC_DirectoryList;
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



-- -------------------------------------------------------------------------------


-- drop table if exists FC_Files;
-- CREATE TABLE FC_Files(
--     FileID INT AUTO_INCREMENT,
--     DirID INT NOT NULL,
--     Size BIGINT UNSIGNED NOT NULL,
--     UID INT NOT NULL,
--     GID INT NOT NULL,
--     Status INT NOT NULL,
--     FileName VARCHAR(128) CHARACTER SET latin1 COLLATE latin1_bin NOT NULL,
-- 
--     PRIMARY KEY (FileID),
--     FOREIGN KEY (DirID) REFERENCES FC_DirectoryList(DirID) ON DELETE CASCADE,
--     FOREIGN KEY (Status) REFERENCES FC_Statuses(StatusID),
--     FOREIGN KEY (UID) REFERENCES FC_Users(UID),
--     FOREIGN KEY (GID) REFERENCES FC_Groups(GID),
--     
--     UNIQUE (DirID, FileName),
--     
--     INDEX (UID,GID),
--     INDEX (Status),
--     INDEX (FileName)
-- 
-- ) ENGINE = INNODB;
-- 
-- -- ------------------------------------------------------------------------------
-- drop table if exists FC_FileInfo;
-- CREATE TABLE FC_FileInfo (
--     FileID INTEGER NOT NULL,
--     GUID char(36) NOT NULL,
--     Checksum VARCHAR(32),
--     CheckSumType ENUM('Adler32','MD5'),
--     Type ENUM('File','Link') NOT NULL DEFAULT 'File',
--     CreationDate DATETIME,
--     ModificationDate DATETIME,
--     Mode SMALLINT UNSIGNED NOT NULL DEFAULT 775,
--     
--     PRIMARY KEY (FileID),
--     FOREIGN KEY (FileID) REFERENCES FC_Files(FileID) ON DELETE CASCADE,
--     
--     UNIQUE(GUID)
-- 
-- 
-- ) ENGINE = INNODB;




drop table if exists FC_Files;
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


-- -- -----------------------------------------------------------------------------
-- -- do we want the delete on cascade on the SE?
-- drop table if exists FC_Replicas;
-- CREATE TABLE FC_Replicas (
--     RepID INT AUTO_INCREMENT,
--     FileID INT NOT NULL,
--     SEID INTEGER NOT NULL,
--     Status INTEGER NOT NULL,
-- 
--     
--     PRIMARY KEY (RepID),
--     FOREIGN KEY (FileID) REFERENCES FC_Files(FileID),
--     FOREIGN KEY (SEID) REFERENCES FC_StorageElements(SEID), 
--     FOREIGN KEY (Status) REFERENCES FC_Statuses(StatusID),
--     
--     UNIQUE (FileID,SEID)
-- 
-- 
-- ) ENGINE = INNODB;
-- 
-- -- ------------------------------------------------------------------------------
-- drop table if exists FC_ReplicaInfo;
-- CREATE TABLE FC_ReplicaInfo (
--     RepID INTEGER NOT NULL,
--     RepType ENUM ('Master','Replica') NOT NULL DEFAULT 'Master',
--     CreationDate DATETIME,
--     ModificationDate DATETIME,
--     PFN VARCHAR(1024),  
--     
--     PRIMARY KEY (RepID),
--     FOREIGN KEY (RepID) REFERENCES FC_Replicas(RepID) ON DELETE CASCADE
-- ) ENGINE = INNODB;
-- 

-- -----------------------------------------------------------------------------
-- do we want the delete on cascade on the SE?
drop table if exists FC_Replicas;
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
(IN dirNames TEXT)
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


-- DROP PROCEDURE IF EXISTS ps_insert_dir;
-- DELIMITER //
-- CREATE PROCEDURE ps_insert_dir
-- (IN parent_id INT, IN child_name varchar(255), IN UID INT,
--  IN GID INT, IN Mode SMALLINT UNSIGNED, IN Status INT)
-- BEGIN
--   DECLARE dir_id INT DEFAULT 0;
--   
--   DECLARE EXIT HANDLER FOR 1062 BEGIN
--     ROLLBACK;
--     SELECT 0 as dir_id, 'Error, duplicate key occurred' as msg;
--   END;
-- 
--   DECLARE EXIT HANDLER FOR 1452 BEGIN
--     ROLLBACK;
--     SELECT 0 as dir_id, 'Cannot add or update a child row: a foreign key constraint fails' as msg;
--   END;
--   
--   DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN
--       ROLLBACK;
--       SELECT 0 as dir_id, 'Unknown error occured' as msg;
--   END;
--     
--   
--   START TRANSACTION;
--    
--     INSERT INTO FC_DirectoryList (Name) values (child_name);
--     SELECT LAST_INSERT_ID() INTO dir_id; 
--   
--     INSERT INTO FC_DirectoryClosure (ParentID, ChildID, Depth ) VALUES (dir_id, dir_id, 0);
--     
--     IF parent_id != 0 THEN
--       INSERT INTO FC_DirectoryClosure(ParentID, ChildID, depth)
--         SELECT SQL_NO_CACHE p.ParentID, c.ChildID, p.depth + c.depth + 1
--         FROM FC_DirectoryClosure p, FC_DirectoryClosure c
--         WHERE p.ChildID = parent_id AND c.ParentID = dir_id;
--     END IF;
--     
--     INSERT INTO FC_DirectoryInfo (DirID, UID, GID, CreationDate, ModificationDate, Mode, Status) VALUES (dir_id, UID, GID, UTC_TIMESTAMP(), UTC_TIMESTAMP(), Mode, Status);
--     
--     SELECT dir_id, 'OK';
--     
--    COMMIT;
-- END //
-- DELIMITER ;


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
   
    INSERT INTO FC_DirectoryList (UID, GID, CreationDate, ModificationDate, Mode, Status, Name) values ( UID, GID, UTC_TIMESTAMP(), UTC_TIMESTAMP(), Mode, Status, child_name);
    SELECT LAST_INSERT_ID() INTO dir_id; 
  
    INSERT INTO FC_DirectoryClosure (ParentID, ChildID, Depth ) VALUES (dir_id, dir_id, 0);
    
    IF parent_id != 0 THEN
      INSERT INTO FC_DirectoryClosure(ParentID, ChildID, depth)
        SELECT SQL_NO_CACHE p.ParentID, dir_id, p.depth + 1
        FROM FC_DirectoryClosure p
        WHERE p.ChildID = parent_id;
    END IF;
    
    
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

DROP PROCEDURE IF EXISTS ps_count_files_in_dir;
DELIMITER //
CREATE PROCEDURE ps_count_files_in_dir
(IN dir_id INT, OUT countFile INT )
BEGIN

  SELECT SQL_NO_CACHE count(FileID) INTO countFile FROM FC_Files WHERE DirID = dir_id;
 
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


-- drop PROCEDURE if exists update_directory_usage;
-- DELIMITER //
-- CREATE PROCEDURE update_directory_usage 
-- (IN top_dir_id INT, IN se_id INT, IN size_diff BIGINT, IN file_diff INT)
-- -- top_dir_id : the id of the dir in which the mouvement starts
-- -- se_id : the id of the SE in which the replica was inserted
-- -- size_diff : the modification to bring to the size (positif if adding a replica, negatif otherwise)
-- -- file_diff : + or - 1 depending whether we add or remove a replica 
-- BEGIN
--   DECLARE dir_id INT;
--   DECLARE exSize INT;
--   DECLARE done INT DEFAULT FALSE;
-- --   DECLARE cur1 CURSOR FOR SELECT ParentID FROM FC_DirectoryClosure c JOIN (SELECT DirID FROM FC_Files where FileID = file_id) f on f.DirID = c.ChildID;
--   DECLARE cur1 CURSOR FOR
--     SELECT SQL_NO_CACHE ParentID FROM FC_DirectoryClosure c
--     WHERE c.ChildID = top_dir_id;
-- 
--   DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = True;
--   
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
--     -- If it is the first replica inserted for the given SE, then we insert the new row, otherwise we do an update
--     -- INSERT INTO FC_DirectoryUsage (DirID, SEID, SESize, SEFiles) VALUES (dir_id, se_id, size_diff, file_diff) ON DUPLICATE KEY UPDATE  SESize = SESize + size_diff, SEFiles = SEFiles + file_diff;
--     
--     -- yet another alternative
--     SELECT SESize INTO exSize FROM FC_DirectoryUsage where DirID = dir_id and SEID = se_id  FOR UPDATE;
--     IF exSize IS NULL THEN
--       INSERT INTO FC_DirectoryUsage (DirID, SEID, SESize, SEFiles) VALUES (dir_id, se_id, size_diff, file_diff);
--     ELSE
--       UPDATE FC_DirectoryUsage SET SESize = SESize + size_diff, SEFiles = SEFiles + file_diff WHERE DirID = dir_id and SEID = se_id ;
--     END IF;
--     
--     
--   END LOOP;
--   
--   CLOSE cur1;
-- END //
-- DELIMITER ;
-- 


-- insert into FC_DirectoryUsage (DirID, SEID, SESize, SEFiles) select f.DirID, 1, f.Size as size_diff, 1 as file_diff from FC_Files f where (DirID = 1 and FileName = 'a.txt') OR (DirID = 1 and FileName = '1.txt') on duplicate key update SESize = SESize + f.Size, SEFiles = SEFiles + 1;



drop PROCEDURE if exists update_directory_usage;
DELIMITER //
CREATE PROCEDURE update_directory_usage 
(IN dir_id INT, IN se_id INT, IN size_diff BIGINT, IN file_diff INT)
-- dir_id : the id of the dir in which we insert/remove
-- se_id : the id of the SE in which the replica was inserted
-- size_diff : the modification to bring to the size (positif if adding a replica, negatif otherwise)
-- file_diff : + or - 1 depending whether we add or remove a replica 
BEGIN

  
    
    -- alternative
    -- If it is the first replica inserted for the given SE, then we insert the new row, otherwise we do an update
    INSERT INTO FC_DirectoryUsage (DirID, SEID, SESize, SEFiles) VALUES (dir_id, se_id, size_diff, file_diff) ON DUPLICATE KEY UPDATE  SESize = SESize + size_diff, SEFiles = SEFiles + file_diff;
    
END //
DELIMITER ;



-- drop trigger if exists trg_after_insert_replica_increase_size;
-- DELIMITER //
-- CREATE TRIGGER trg_after_insert_replica_increase_size AFTER INSERT ON FC_Replicas
-- FOR EACH ROW
-- BEGIN
-- 
--   DECLARE file_size BIGINT;
--   DECLARE dir_id INT;
--   
--  
--   SELECT SQL_NO_CACHE Size, DirID INTO file_size, dir_id FROM FC_Files where FileID = new.FileID;
--   
--   
-- --   call update_directory_usage (new.FileID, new.SEID, file_size, 1);
--   call update_directory_usage (dir_id, new.SEID, file_size, 1);
-- 
-- END //
-- DELIMITER ;


-- drop trigger if exists trg_after_delete_replica_decrease_size;
-- DELIMITER //
-- CREATE TRIGGER trg_after_delete_replica_decrease_size AFTER DELETE ON FC_Replicas
-- FOR EACH ROW
-- BEGIN
-- 
--   DECLARE file_size BIGINT;
--   DECLARE dir_id INT;
--  
--   SELECT SQL_NO_CACHE Size, DirID INTO file_size, dir_id FROM FC_Files where FileID = old.FileID;
--   
-- --   call update_directory_usage (old.FileID, old.SEID, -file_size, -1);
--   call update_directory_usage (dir_id, old.SEID, -file_size, -1);
-- 
-- END //
-- DELIMITER ;


drop trigger if exists trg_after_update_replica_move_size;
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





-- drop trigger if exists trg_after_insert_file_increase_size;
-- DELIMITER //
-- CREATE TRIGGER trg_after_insert_file_increase_size AFTER INSERT ON FC_Files
-- FOR EACH ROW
-- BEGIN
--   DECLARE se_id INT;
--   
--   SELECT SEID INTO se_id FROM FC_StorageElements WHERE SEName = 'FakeSE';
--   
--   
--   -- Use the fake SE
-- --   call update_directory_usage (new.FileID, se_id, new.Size, 1);
--   call update_directory_usage (new.DirID, se_id, new.Size, 1);
-- 
-- END //
-- DELIMITER ;


-- drop trigger if exists trg_after_delete_file_decrease_size;
-- DELIMITER //
-- CREATE TRIGGER trg_after_delete_file_decrease_size AFTER DELETE ON FC_Files
-- FOR EACH ROW
-- BEGIN
--   DECLARE se_id INT;
--   
--   SELECT SEID INTO se_id FROM FC_StorageElements WHERE SEName = 'FakeSE';
--     
-- --   call update_directory_usage (old.FileID, se_id, -old.Size, -1);
--   call update_directory_usage (old.DirID, se_id, -old.Size, -1);
-- 
-- END //
-- DELIMITER ;
-- 






-- example call ps_get_replicas_for_files_in_dir(6, False, "'APrioriGood'","'Trash'");

DROP PROCEDURE IF EXISTS ps_get_replicas_for_files_in_dir;
DELIMITER //
CREATE PROCEDURE ps_get_replicas_for_files_in_dir
(IN dir_id INT, IN allStatus BOOLEAN, IN visibleFileStatus VARCHAR(255), IN visibleReplicaStatus VARCHAR(255) )
BEGIN
-- select f.FileName, f.FileID, s.SEName, ri.PFN from FC_Replicas r join  FC_ReplicaInfo ri on ri.RepID = r.RepID join FC_Files f on f.FileID = r.FileID join FC_StorageElements s on s.SEID = r.SEID where DirID = 6;
--   set @sql = 'select SQL_NO_CACHE f.FileName, f.FileID, s.SEName, ri.PFN from FC_Replicas r join  FC_ReplicaInfo ri on ri.RepID = r.RepID join FC_Files f on f.FileID = r.FileID join FC_StorageElements s on s.SEID = r.SEID ';
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


--  explain SELECT f.FileName, f.FileID, SEName, PFN FROM FC_Files f JOIN (select FileID, RepId, Status, SEID from FC_Replicas) r on f.FileID = r.FileID  JOIN (SELECT RepID, PFN from FC_ReplicaInfo ) ri on r.RepID = ri.RepID JOIN (select SEID, SEName from FC_StorageElements) se on se.SEID = r.SEID where f.DirID = 1 and r.Status in (select StatusID from FC_Statuses where Status in ('a','b'));



DROP PROCEDURE IF EXISTS ps_get_file_id_from_lfn;
DELIMITER //
CREATE PROCEDURE ps_get_file_id_from_lfn
(IN dirName VARCHAR(255), IN fileName VARCHAR(255), OUT file_id INT )
BEGIN
  DECLARE done INT;
  DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;

-- SELECT d.Name, f.FileName,f.FileID FROM FC_Files f join FC_DirectoryList d on f.DirID = d.DirID WHERE (d.Name = '/vo.formation.idgrilles.fr/user/a/atsareg' and FileName in ('another testFile') ) OR (d.DirID = 6 and FileName in ('testfile'));
  SELECT SQL_NO_CACHE FileID INTO file_id FROM FC_Files f JOIN FC_DirectoryList d ON f.DirID = f.DirID WHERE d.Name = dirName and f.FileName = fileName;
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
  SET @sql = CONCAT('SELECT SQL_NO_CACHE FileID, FileName FROM FC_Files f WHERE DirID = ', dir_id, ' AND FileName IN (', file_names, ')');
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



--   set @sql = CONCAT('SELECT SQL_NO_CACHE FileName, DirID, f.FileID, Size, f.uid, UserName, f.gid, GroupName, s.Status,
--                      GUID, Checksum, ChecksumType, Type, CreationDate,ModificationDate, Mode
--                     FROM FC_Files f
--                     JOIN FC_FileInfo fi ON f.FileID = fi.FileID
--                     JOIN FC_Users u ON f.UID = u.UID
--                     JOIN FC_Groups g ON f.GID = g.GID
--                     JOIN FC_Statuses s ON f.Status = s.StatusID
--                     WHERE DirID = ', dir_id, ' ' );

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




DROP PROCEDURE IF EXISTS ps_get_all_info_for_file_ids;
DELIMITER //
CREATE PROCEDURE ps_get_all_info_for_file_ids
(IN file_ids TEXT)
BEGIN
--   SET @sql = CONCAT('SELECT SQL_NO_CACHE f.FileID, Size, UID, GID, s.Status, GUID, CreationDate
--                      FROM FC_Files f
--                      JOIN FC_FileInfo fi ON f.FileID = fi.FileID
--                      JOIN FC_Statuses s ON f.Status = s.StatusID
--                      WHERE f.FileID IN (', file_ids, ')');

  SET @sql = CONCAT('SELECT SQL_NO_CACHE f.FileID, Size, UID, GID, s.Status, GUID, CreationDate
                     FROM FC_Files f
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
    
  
--   START TRANSACTION;
--   INSERT INTO FC_Files (DirID, Size, UID, GID, Status, FileName) VALUES (dir_id, size, UID, GID, status_id, filename);
--   SELECT LAST_INSERT_ID() INTO file_id;
--   INSERT INTO FC_FileInfo (FileID, GUID, Checksum, CheckSumType, CreationDate, ModificationDate, Mode)
--          VALUES (file_id, GUID, checksum, checksumtype, UTC_TIMESTAMP(), UTC_TIMESTAMP(), mode);
--   COMMIT;

  START TRANSACTION;
  INSERT INTO FC_Files (DirID, Size, UID, GID, Status, FileName,GUID, Checksum, ChecksumType, CreationDate, ModificationDate, Mode )
  VALUES (dir_id, size, UID, GID, status_id, filename, GUID, checksum, checksumtype, UTC_TIMESTAMP(), UTC_TIMESTAMP(), mode);
  SELECT LAST_INSERT_ID() INTO file_id;
  
  INSERT INTO FC_DirectoryUsage (DirID, SEID, SESize, SEFiles) VALUES (dir_id, 1, size, 1) ON DUPLICATE KEY UPDATE  SESize = SESize + size, SEFiles = SEFiles + 1;

  COMMIT;

  SELECT file_id, 'OK' as msg;
 
END //
DELIMITER ;



-- fileValues formated like "(a,y,z), (u,v,w)"
-- fileDesc formated like " (DirID = x and FileName = y) OR (DirID = u and FileName = v)"
DROP PROCEDURE IF EXISTS ps_insert_multiple_file;
DELIMITER //
CREATE PROCEDURE ps_insert_multiple_file
(IN fileValues LONGTEXT, IN fileDesc LONGTEXT )
BEGIN

  START TRANSACTION;
  SET @sql = CONCAT('INSERT INTO FC_Files (DirID, Size, UID, GID, Status, FileName, GUID, Checksum, ChecksumType, CreationDate, ModificationDate, Mode) VALUES ', fileValues);


  PREPARE stmt FROM @sql;
  EXECUTE stmt;
  DEALLOCATE PREPARE stmt;

  
  SET @sql = CONCAT('INSERT INTO FC_DirectoryUsage (DirID, SEID, SESize, SEFiles) SELECT SQL_NO_CACHE f.DirID, 1, f.Size, 1 FROM FC_Files f WHERE ', fileDesc);
  SET @sql = CONCAT(@sql, ' ON DUPLICATE KEY UPDATE SESize = SESize + f.Size, SEFiles = SEFiles + 1');
  -- insert into FC_DirectoryUsage (DirID, SEID, SESize, SEFiles) select f.DirID, 1, f.Size as size_diff, 1 as file_diff from FC_Files f where (DirID = 1 and FileName = 'a.txt') OR (DirID = 1 and FileName = '1.txt') on duplicate key update SESize = SESize + f.Size, SEFiles = SEFiles + 1;
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




DROP PROCEDURE IF EXISTS ps_get_file_ids_from_guids;
DELIMITER //
CREATE PROCEDURE ps_get_file_ids_from_guids
(IN  guids TEXT)
BEGIN
--   SET @sql = CONCAT('SELECT SQL_NO_CACHE GUID, FileID FROM FC_FileInfo f WHERE GUID IN (', guids, ')');
  SET @sql = CONCAT('SELECT SQL_NO_CACHE GUID, FileID FROM FC_Files f WHERE GUID IN (', guids, ')');

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

  START TRANSACTION;
  SET @sql = CONCAT('UPDATE FC_DirectoryUsage d, FC_Files f, FC_Replicas r
                    SET d.SESize = d.SESize - f.Size, d.SEFiles = d.SEFiles - 1
                    WHERE r.FileID = f.FileID 
                    AND f.DirID = d.DirID 
                    AND r.SEID = d.SEID
                    AND f.FileID IN (', file_ids, ')');
  PREPARE stmt FROM @sql;
  EXECUTE stmt;
  DEALLOCATE PREPARE stmt;
  


  SET @sql = CONCAT('DELETE FROM FC_Replicas  WHERE FileID IN (', file_ids, ')');
  PREPARE stmt FROM @sql;
  EXECUTE stmt;
  DEALLOCATE PREPARE stmt;
  
  COMMIT;
  
  SELECT 0, 'OK';
  
END //
DELIMITER ;


DROP PROCEDURE IF EXISTS ps_delete_files;
DELIMITER //
CREATE PROCEDURE ps_delete_files
(IN  file_ids TEXT)
BEGIN
  START TRANSACTION;
  SET @sql = CONCAT('UPDATE FC_DirectoryUsage d, FC_Files f
                    SET d.SESize = d.SESize - f.Size, d.SEFiles = d.SEFiles - 1
                    WHERE f.DirID = d.DirID 
                    AND d.SEID = 1
                    AND f.FileID IN (', file_ids, ')');
  PREPARE stmt FROM @sql;
  EXECUTE stmt;
  DEALLOCATE PREPARE stmt;
  
  SET @sql = CONCAT('DELETE FROM FC_Files  WHERE FileID IN (', file_ids, ')');
  PREPARE stmt FROM @sql;
  EXECUTE stmt;
  DEALLOCATE PREPARE stmt;
  
  COMMIT;
  
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
    
  
--   START TRANSACTION;
--   INSERT INTO FC_Replicas (FileID, SEID, Status) VALUES (file_id, se_id, status_id);
--   SELECT LAST_INSERT_ID() INTO replica_id;
--   INSERT INTO FC_ReplicaInfo (RepID, RepType, CreationDate, ModificationDate, PFN)
--          VALUES (replica_id, rep_type, UTC_TIMESTAMP(), UTC_TIMESTAMP(), pfn);
--   COMMIT;
--   
  START TRANSACTION;
  INSERT INTO FC_Replicas (FileID, SEID, Status, RepType, CreationDate, ModificationDate, PFN)
  VALUES (file_id, se_id, status_id, rep_type, UTC_TIMESTAMP(), UTC_TIMESTAMP(), pfn);
  SELECT LAST_INSERT_ID() INTO replica_id;
  INSERT INTO FC_DirectoryUsage (DirID, SEID, SESize, SEFiles)  SELECT DirID, se_id, Size, 1 from FC_Files f where f.FileID = file_id ON DUPLICATE KEY UPDATE  SESize = SESize + Size, SEFiles = SEFiles + 1;

  COMMIT;
  
  SELECT replica_id, 'OK' as msg;
 
END //
DELIMITER ;




-- replicaValues formated like "(a,y,z), (u,v,w)"
-- replicaDesc formated like " (FileID = x and SEID = y) OR (FileID = u and SEID = v)"
DROP PROCEDURE IF EXISTS ps_insert_multiple_replica;
DELIMITER //
CREATE PROCEDURE ps_insert_multiple_replica
(IN replicaValues LONGTEXT, IN replicaDesc LONGTEXT )
BEGIN

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





-- DROP PROCEDURE IF EXISTS ps_delete_replica_from_file_and_se_ids;
-- DELIMITER //
-- CREATE PROCEDURE ps_delete_replica_from_file_and_se_ids
-- (IN  file_id INT, IN se_id INT) 
-- BEGIN
-- 
--   DELETE FROM FC_Replicas WHERE FileID = file_id AND SEID = se_id;
--   
--   SELECT 0, 'OK';
-- 
-- END //
-- DELIMITER ;
DROP PROCEDURE IF EXISTS ps_delete_replica_from_file_and_se_ids;
DELIMITER //
CREATE PROCEDURE ps_delete_replica_from_file_and_se_ids
(IN  file_id INT, IN se_id INT) 
BEGIN
  DECLARE file_size INT DEFAULT 0;
  DECLARE dir_id INT DEFAULT 0;

  SELECT Size, DirID INTO file_size, dir_id from FC_Files WHERE FileID = file_id;
  
  START TRANSACTION;
    UPDATE FC_DirectoryUsage SET SESize = SESize - file_size, SEFiles = SEFiles - 1 WHERE DirID = dir_id and SEID = se_id;
    DELETE FROM FC_Replicas WHERE FileID = file_id AND SEID = se_id;
  COMMIT;
  
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
  
--   UPDATE FC_Files SET UID = in_uid where FileID = file_id;
--   UPDATE FC_FileInfo SET ModificationDate = UTC_TIMESTAMP() WHERE FileID = file_id;
  
  UPDATE FC_Files SET UID = in_uid, ModificationDate = UTC_TIMESTAMP() where FileID = file_id;
 
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
  
--   UPDATE FC_Files SET GID = in_gid where FileID = file_id;
--   UPDATE FC_FileInfo SET ModificationDate = UTC_TIMESTAMP() WHERE FileID = file_id;

  UPDATE FC_Files SET GID = in_gid, ModificationDate = UTC_TIMESTAMP() where FileID = file_id;
  
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
  
--   UPDATE FC_Files SET Status = status_id where FileID = file_id;
--   UPDATE FC_FileInfo SET ModificationDate = UTC_TIMESTAMP() WHERE FileID = file_id;
  UPDATE FC_Files SET Status = status_id, ModificationDate = UTC_TIMESTAMP() where FileID = file_id;

  SELECT 0, 'OK';

END //
DELIMITER ;


DROP PROCEDURE IF EXISTS ps_set_file_mode;
DELIMITER //
CREATE PROCEDURE ps_set_file_mode
(IN  file_id INT, IN in_mode SMALLINT UNSIGNED)
BEGIN

--   UPDATE FC_FileInfo SET Mode = in_mode, ModificationDate = UTC_TIMESTAMP()  WHERE FileID = file_id;
  UPDATE FC_Files SET Mode = in_mode, ModificationDate = UTC_TIMESTAMP()  WHERE FileID = file_id;


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

--     SELECT SQL_NO_CACHE FileID, se.SEName, st.Status, RepType, CreationDate, ModificationDate, PFN
--     FROM FC_Replicas r
--     JOIN FC_ReplicaInfo ri on r.RepID = ri.RepID
--     JOIN FC_StorageElements se on r.SEID = se.SEID
--     JOIN FC_Statuses st on r.Status = st.StatusID
--     WHERE FileID = file_id;
    SELECT SQL_NO_CACHE FileID, se.SEName, st.Status, RepType, CreationDate, ModificationDate, PFN
    FROM FC_Replicas r
    JOIN FC_StorageElements se on r.SEID = se.SEID
    JOIN FC_Statuses st on r.Status = st.StatusID
    WHERE FileID = file_id;

  ELSE

--     SET @sql = CONCAT(
--                     'SELECT SQL_NO_CACHE FileID, se.SEName, st.Status, RepType, CreationDate, ModificationDate, PFN
--                     FROM FC_Replicas r
--                     JOIN FC_ReplicaInfo ri on r.RepID = ri.RepID
--                     JOIN FC_StorageElements se on r.SEID = se.SEID
--                     JOIN FC_Statuses st on r.Status = st.StatusID
--                     WHERE FileID =',file_id,
--                     ' and st.Status  in (',visibleReplicaStatus,') ');
    SET @sql = CONCAT(
                    'SELECT SQL_NO_CACHE FileID, se.SEName, st.Status, RepType, CreationDate, ModificationDate, PFN
                    FROM FC_Replicas r
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


--     SELECT SQL_NO_CACHE d.DirID, di.UID, u.UserName, di.GID, g.GroupName, di.Status, di.Mode, di.CreationDate, di.ModificationDate
--     FROM FC_DirectoryList d
--     JOIN FC_DirectoryInfo di on d.DirID = di.DirID
--     JOIN FC_Users u on di.UID = u.UID
--     JOIN FC_Groups g on di.GID = g.GID
--     WHERE Name = dir_name;
    SELECT SQL_NO_CACHE d.DirID, d.UID, u.UserName, d.GID, g.GroupName, d.Status, d.Mode, d.CreationDate, d.ModificationDate
    FROM FC_DirectoryList d
    JOIN FC_Users u on d.UID = u.UID
    JOIN FC_Groups g on d.GID = g.GID
    WHERE Name = dir_name;
END //
DELIMITER ;


DROP PROCEDURE IF EXISTS ps_get_all_directory_info_from_id;
DELIMITER //
CREATE PROCEDURE ps_get_all_directory_info_from_id
(IN dir_id INT)
BEGIN


--     SELECT SQL_NO_CACHE d.DirID, di.UID, u.UserName, di.GID, g.GroupName, di.Status, di.Mode, di.CreationDate, di.ModificationDate
--     FROM FC_DirectoryList d
--     JOIN FC_DirectoryInfo di on d.DirID = di.DirID
--     JOIN FC_Users u on di.UID = u.UID
--     JOIN FC_Groups g on di.GID = g.GID
--     WHERE d.DirID = dir_id;
    SELECT SQL_NO_CACHE d.DirID, d.UID, u.UserName, d.GID, g.GroupName, d.Status, d.Mode, d.CreationDate, d.ModificationDate
    FROM FC_DirectoryList d
    JOIN FC_Users u on d.UID = u.UID
    JOIN FC_Groups g on d.GID = g.GID
    WHERE d.DirID = dir_id;
END //
DELIMITER ;


DROP PROCEDURE IF EXISTS ps_set_dir_gid;
DELIMITER //
CREATE PROCEDURE ps_set_dir_gid
(IN  dir_name VARCHAR(255), IN gid INT)
BEGIN

--   UPDATE FC_DirectoryInfo di JOIN FC_DirectoryList d ON d.DirID = di.DirID SET GID = gid, ModificationDate = UTC_TIMESTAMP() WHERE Name = dir_name;
  UPDATE FC_DirectoryList SET GID = gid, ModificationDate = UTC_TIMESTAMP() WHERE Name = dir_name;

  SELECT 0 as errno, ROW_COUNT() as affected, 'OK' as errmsg;

END //
DELIMITER ;



DROP PROCEDURE IF EXISTS ps_set_dir_uid;
DELIMITER //
CREATE PROCEDURE ps_set_dir_uid
(IN  dir_name VARCHAR(255), IN uid INT)
BEGIN

--   UPDATE FC_DirectoryInfo di JOIN FC_DirectoryList d ON d.DirID = di.DirID SET UID = uid, ModificationDate = UTC_TIMESTAMP() WHERE Name = dir_name;
  UPDATE FC_DirectoryList SET UID = uid, ModificationDate = UTC_TIMESTAMP() WHERE Name = dir_name;

  SELECT 0 as errno, ROW_COUNT() as affected, 'OK' as errmsg;

END //
DELIMITER ;



DROP PROCEDURE IF EXISTS ps_set_dir_status;
DELIMITER //
CREATE PROCEDURE ps_set_dir_status
(IN  dir_name VARCHAR(255), IN status_id INT)
BEGIN

--   UPDATE FC_DirectoryInfo di JOIN FC_DirectoryList d ON d.DirID = di.DirID SET Status = status_id, ModificationDate = UTC_TIMESTAMP() WHERE Name = dir_name;
  UPDATE FC_DirectoryList SET Status = status_id, ModificationDate = UTC_TIMESTAMP() WHERE Name = dir_name;

  SELECT 0 as errno, ROW_COUNT() as affected, 'OK' as errmsg;

END //
DELIMITER ;

DROP PROCEDURE IF EXISTS ps_set_dir_mode;
DELIMITER //
CREATE PROCEDURE ps_set_dir_mode
(IN  dir_name VARCHAR(255), IN mode SMALLINT UNSIGNED)
BEGIN

--   UPDATE FC_DirectoryInfo di JOIN FC_DirectoryList d ON d.DirID = di.DirID SET Mode = mode, ModificationDate = UTC_TIMESTAMP() WHERE Name = dir_name;
  UPDATE FC_DirectoryList SET Mode = mode, ModificationDate = UTC_TIMESTAMP() WHERE Name = dir_name;

  SELECT 0 as errno, ROW_COUNT() as affected, 'OK' as errmsg;

END //
DELIMITER ;


DROP PROCEDURE IF EXISTS ps_get_files_in_dir;
DELIMITER //
CREATE PROCEDURE ps_get_files_in_dir
(IN dir_id INT)
BEGIN

  SELECT SQL_NO_CACHE FileID, DirID, FileName FROM FC_Files f WHERE DirID = dir_id;

END //
DELIMITER ;


-- DROP PROCEDURE IF EXISTS ps_get_dir_logical_size;
-- DELIMITER //
-- CREATE PROCEDURE ps_get_dir_logical_size
-- (IN dir_id INT)
-- BEGIN
-- 
--   SELECT SQL_NO_CACHE SESize, SEFiles FROM FC_DirectoryUsage u
--   JOIN FC_StorageElements s ON s.SEID = u.SEID 
--   WHERE s.SEName = 'FakeSE'
--   AND u.DirID = dir_id;
-- 
-- 
-- END //
-- DELIMITER ;

DROP PROCEDURE IF EXISTS ps_get_dir_logical_size;
DELIMITER //
CREATE PROCEDURE ps_get_dir_logical_size
(IN dir_id INT)
BEGIN

  SELECT SQL_NO_CACHE SUM(SESize), SUM(SEFiles) FROM FC_DirectoryUsage u
  JOIN FC_DirectoryClosure c on c.ChildID = u.DirID
  JOIN FC_StorageElements s ON s.SEID = u.SEID 
  WHERE s.SEName = 'FakeSE'
  AND c.ParentID = dir_id;


END //
DELIMITER ;


DROP PROCEDURE IF EXISTS ps_calculate_dir_logical_size;
DELIMITER //
CREATE PROCEDURE ps_calculate_dir_logical_size
(IN dir_id INT)
BEGIN
  DECLARE log_size BIGINT DEFAULT 0;
  DECLARE log_files INT DEFAULT 0;
  
  SELECT SQL_NO_CACHE SUM(f.Size), COUNT(*) INTO log_size, log_files FROM FC_Files f
  JOIN FC_DirectoryClosure d ON f.DirID = d.ChildID
  WHERE ParentID = dir_id;
  
  IF log_size IS NULL THEN
    SET log_size = 0;
  END IF;
  
  select log_size, log_files;
END //
DELIMITER ;



-- DROP PROCEDURE IF EXISTS ps_get_dir_physical_size;
-- DELIMITER //
-- CREATE PROCEDURE ps_get_dir_physical_size
-- (IN dir_id INT)
-- BEGIN
-- 
--   SELECT SQL_NO_CACHE SEName, SESize, SEFiles
--   FROM FC_DirectoryUsage u
--   JOIN FC_StorageElements se ON se.SEID = u.SEID
--   WHERE DirID = dir_id
--   AND SEName != 'FakeSE'
--   AND (SESize != 0 OR SEFiles != 0);
-- 
-- 
-- END //
-- DELIMITER ;


DROP PROCEDURE IF EXISTS ps_get_dir_physical_size;
DELIMITER //
CREATE PROCEDURE ps_get_dir_physical_size
(IN dir_id INT)
BEGIN

  SELECT SQL_NO_CACHE SEName, SESize, SEFiles
  FROM FC_DirectoryUsage u
  JOIN FC_DirectoryClosure c on u.DirID = c.ChildID
  JOIN FC_StorageElements se ON se.SEID = u.SEID
  WHERE c.ParentID = dir_id
  AND SEName != 'FakeSE'
  AND (SESize != 0 OR SEFiles != 0);


END //
DELIMITER ;



DROP PROCEDURE IF EXISTS ps_calculate_dir_physical_size;
DELIMITER //
CREATE PROCEDURE ps_calculate_dir_physical_size
(IN dir_id INT)
BEGIN

  SELECT SQL_NO_CACHE se.SEName, sum(f.Size), count(*)
  FROM FC_Replicas r
  JOIN FC_Files f ON f.FileID = r.FileID
  JOIN FC_StorageElements se ON se.SEID = r.SEID
  JOIN FC_DirectoryClosure dc ON dc.ChildID = f.DirID
  WHERE dc.ParentID = dir_id
  GROUP BY se.SEID
  ORDER BY NULL; 

END //
DELIMITER ;



-- DROP PROCEDURE IF EXISTS ps_rebuild_directory_usage;
-- DELIMITER //
-- 
-- CREATE PROCEDURE ps_rebuild_directory_usage()
-- BEGIN
--   DECLARE dir_id INT UNSIGNED;
--   DECLARE se_id INT UNSIGNED;
--   DECLARE _size BIGINT UNSIGNED;
--   DECLARE fileCur CURSOR FOR SELECT DirID, Size from FC_Files;
--   DECLARE repCur CURSOR FOR SELECT f.DirID, f.Size, r.SEID from FC_Files f, FC_Replicas r where r.FileID = f.FileID;
--   DECLARE cur CURSOR FOR SELECT DirID, SEID, SESize, SEFiles from TMP_UsageSum;
-- 
--   START TRANSACTION;
--   
--   
--   CREATE TEMPORARY TABLE TMP_UsageSum AS SELECT DirID, 1 as SEID, sum(Size) as SESize, count(*) as SEFiles from FC_Files group by DirID;
-- 
--   INSERT INTO TMP_UsageSum (DirID, SEID, SESize, SEFiles)  select DirID, SEID, sum(Size), count(*) from FC_Replicas r join FC_Files f on r.FileID = f.FileID group by DirID, SEID;
--   
--   DELETE FROM FC_DirectoryUsage;
--   
--   BLOCKFILE: begin
--     DECLARE doneFile BOOLEAN DEFAULT FALSE;
-- 
--     DECLARE CONTINUE HANDLER FOR NOT FOUND SET doneFile := TRUE;
-- 
--     OPEN fileCur;
--     
--     fileLoop: LOOP
--       FETCH fileCur INTO dir_id, _size;
--       IF doneFile THEN
--         LEAVE fileLoop;
--       END IF;
--       CALL update_directory_usage (dir_id, 1, _size, 1);
--     END LOOP fileLoop;
-- 
--     CLOSE fileCur;
--     
--   END BLOCKFILE;
--   
--   BLOCKREP: begin
--     DECLARE doneRep BOOLEAN DEFAULT FALSE;
--     DECLARE CONTINUE HANDLER FOR NOT FOUND SET doneRep := TRUE;
-- 
--     OPEN repCur;
--     
--     repLoop: LOOP
--       FETCH repCur INTO dir_id, _size, se_id;
--       IF doneRep THEN
--         LEAVE repLoop;
--       END IF;
--       CALL update_directory_usage (dir_id, se_id, _size, 1);
--     END LOOP repLoop;
-- 
--     CLOSE repCur;
--     
--   END BLOCKREP;
--   
--   COMMIT;
-- END //
-- DELIMITER ;
/*
DROP PROCEDURE IF EXISTS ps_rebuild_directory_usage;
DELIMITER //

CREATE PROCEDURE ps_rebuild_directory_usage()
BEGIN
  DECLARE dir_id INT UNSIGNED;
  DECLARE se_id INT UNSIGNED;
  DECLARE _size BIGINT UNSIGNED;
  DECLARE _count INT;
  DECLARE done BOOLEAN DEFAULT FALSE;

  DECLARE cur CURSOR FOR SELECT DirID, SEID, SESize, SEFiles from TMP_UsageSum;
  DECLARE CONTINUE HANDLER FOR NOT FOUND SET done := TRUE;
  START TRANSACTION;
  
  
  CREATE TEMPORARY TABLE TMP_UsageSum AS SELECT DirID, 1 as SEID, sum(Size) as SESize, count(*) as SEFiles from FC_Files group by DirID ORDER BY NULL;

  INSERT INTO TMP_UsageSum (DirID, SEID, SESize, SEFiles)  select DirID, SEID, sum(Size), count(*) from FC_Replicas r join FC_Files f on r.FileID = f.FileID group by DirID, SEID ORDER BY NULL;
  
  DELETE FROM FC_DirectoryUsage;
  
  OPEN cur;
    
  fileLoop: LOOP
    FETCH cur INTO dir_id, se_id, _size, _count;
    IF done THEN
      LEAVE fileLoop;
    END IF;
    CALL update_directory_usage (dir_id, se_id, _size, _count);
  END LOOP fileLoop;

  CLOSE cur;
  
  DROP TABLE TMP_UsageSum;
  
  COMMIT;
END //
DELIMITER ;*/


DROP PROCEDURE IF EXISTS ps_rebuild_directory_usage;
DELIMITER //

CREATE PROCEDURE ps_rebuild_directory_usage()
BEGIN

  START TRANSACTION;
  
  DELETE FROM FC_DirectoryUsage;

  INSERT INTO FC_DirectoryUsage (DirID, SEID, SESize, SEFiles)
    SELECT SQL_NO_CACHE DirID, 1 as SEID, sum(Size) as SESize, count(*) as SEFiles
    FROM FC_Files
    GROUP BY DirID
    ORDER BY NULL;

  INSERT INTO FC_DirectoryUsage (DirID, SEID, SESize, SEFiles)
    SELECT SQL_NO_CACHE DirID, SEID, sum(Size), count(*)
    FROM FC_Replicas r
    JOIN FC_Files f ON r.FileID = f.FileID
    GROUP BY DirID, SEID
    ORDER BY NULL;
   
  COMMIT;
END //
DELIMITER ;

-- Consistency checks

-- entry in FileInfo not in Files
-- SELECT i.* from FC_Files f RIGHT OUTER JOIN FC_FileInfo i ON f.FileID = i.FileID WHERE f.FileID IS NULL;

-- entry in Files not in FileInfo
-- SELECT f.* from FC_Files f LEFT OUTER JOIN FC_FileInfo i ON f.FileID = i.FileID WHERE i.FileID IS NULL;


-- entry in DirectoryInfo not in DirectoryList
-- SELECT i.* from FC_DirectoryList d RIGHT OUTER JOIN FC_DirectoryInfo i ON d.DirID = i.DirID WHERE d.DirID IS NULL;

-- entry in DirectoryList not in DirectoryInfo
-- SELECT d.* from FC_DirectoryList d LEFT OUTER JOIN FC_DirectoryInfo i ON d.DirID = i.DirID WHERE i.DirID IS NULL;

-- entry in ReplicaInfo not in Replicas
-- This might give some result, since in the lfc it is possible to have several replica at the same SE for a given LFN
-- SELECT i.* from FC_Replicas f RIGHT OUTER JOIN FC_ReplicaInfo i ON f.RepID = i.RepID WHERE f.RepID IS NULL;

-- entry in Replicas not in ReplicaInfo
-- SELECT f.* from FC_Replicas f LEFT OUTER JOIN FC_ReplicaInfo i ON f.RepID = i.RepID WHERE i.RepID IS NULL;

-- useless users
-- we have some. What to do?
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

SET FOREIGN_KEY_CHECKS = 1;
