-- $HeaderURL $
-- ------------------------------------------------------------------------------
-- DROP DATABASE IF EXISTS FileCatalogDB;
-- ------------------------------------------------------------------------------
-- CREATE DATABASE FileCatalogDB;

-- use mysql;
-- delete from user where user='Dirac';
--
-- Must set passwords for database user by replacing "must_be_set".
--
-- GRANT SELECT,INSERT,LOCK TABLES,CREATE TEMPORARY TABLES,UPDATE,DELETE,CREATE,DROP,ALTER ON FileCatalogDB.* TO Dirac@localhost IDENTIFIED BY 'must_be_set';

FLUSH PRIVILEGES;

USE FileCatalogDB;


CREATE TABLE FC_Files(
    FileID INT AUTO_INCREMENT PRIMARY KEY,
    DirID INT NOT NULL,
    Size BIGINT UNSIGNED NOT NULL,
    UID SMALLINT UNSIGNED NOT NULL,
    GID TINYINT UNSIGNED NOT NULL,
    Status SMALLINT UNSIGNED NOT NULL,
    FileName VARCHAR(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    INDEX (DirID),
    INDEX (UID,GID),
    INDEX (Status),
    INDEX (FileName),
    INDEX (DirID,FileName)
) ENGINE = INNODB;

-- ------------------------------------------------------------------------------

CREATE TABLE FC_FileInfo (
    FileID INTEGER NOT NULL PRIMARY KEY,
    INDEX(FileID),
    GUID char(36) NOT NULL,
    INDEX(GUID),
    Checksum VARCHAR(32),
    CheckSumType ENUM('Adler32','MD5'),
    Type ENUM('File','Link') NOT NULL DEFAULT 'File',
    CreationDate DATETIME,
    ModificationDate DATETIME,
    Mode SMALLINT UNSIGNED NOT NULL DEFAULT 775
) ENGINE = INNODB;

-- Make additions to the FC_Files table to include the FC_FileInfo information
-- ALTER TABLE FC_Files ADD COLUMN GUID CHAR(36) NOT NULL AFTER FileName;
-- ALTER TABLE FC_Files ADD INDEX (GUID);
-- ALTER TABLE FC_Files ADD COLUMN Checksum VARCHAR(32) AFTER GUID;
-- ALTER TABLE FC_Files ADD COLUMN CheckSumType ENUM('Adler32','MD5') AFTER Checksum;
-- ALTER TABLE FC_Files ADD COLUMN Type ENUM('File','Link') NOT NULL DEFAULT 'File' AFTER CheckSumType;
-- ALTER TABLE FC_Files ADD COLUMN CreationDate DATETIME AFTER Type;
-- ALTER TABLE FC_Files ADD COLUMN ModificationDate DATETIME AFTER CreationDate;
-- ALTER TABLE FC_Files ADD COLUMN Mode SMALLINT UNSIGNED NOT NULL DEFAULT 775 AFTER ModificationDate;

-- ------------------------------------------------------------------------------

CREATE TABLE FC_Statuses (
    StatusID INT AUTO_INCREMENT PRIMARY KEY,
    Status VARCHAR(32),
    INDEX(Status),
    INDEX(StatusID)
) ENGINE = INNODB;

-- -----------------------------------------------------------------------------

CREATE TABLE FC_Replicas (
    RepID INT AUTO_INCREMENT PRIMARY KEY,
    FileID INT NOT NULL,
    SEID INTEGER NOT NULL,
    Status SMALLINT UNSIGNED NOT NULL,
    INDEX (FileID),
    INDEX (SEID),
    UNIQUE INDEX (FileID,SEID),
    INDEX (Status)
) ENGINE = INNODB;

-- ------------------------------------------------------------------------------

CREATE TABLE FC_ReplicaInfo (
    RepID INTEGER NOT NULL AUTO_INCREMENT,
    RepType ENUM ('Master','Replica') NOT NULL DEFAULT 'Master',
    CreationDate DATETIME,
    ModificationDate DATETIME,
    PFN VARCHAR(1024),
    PRIMARY KEY (RepID)
) ENGINE = INNODB;

-- Make additions to the FC_Replicas table to include the FC_ReplicaInfo information
-- ALTER TABLE FC_Replicas ADD COLUMN RepType ENUM ('Master','Replica') NOT NULL DEFAULT 'Master' AFTER Status;
-- ALTER TABLE FC_Replicas ADD COLUMN CreationDate DATETIME AFTER RepType;
-- ALTER TABLE FC_Replicas ADD COLUMN ModificationDate DATETIME AFTER CreationDate;
-- ALTER TABLE FC_Replicas ADD COLUMN PFN VARCHAR(1024) AFTER ModificationDate;

-- ------------------------------------------------------------------------------


CREATE TABLE FC_Groups (
    GID INTEGER NOT NULL AUTO_INCREMENT,
    GroupName VARCHAR(127) NOT NULL,
    PRIMARY KEY (GID),
    UNIQUE KEY (GroupName)
) ENGINE = INNODB;

-- ------------------------------------------------------------------------------

CREATE TABLE FC_Users (
    UID INTEGER NOT NULL AUTO_INCREMENT,
    UserName VARCHAR(127) NOT NULL,
    PRIMARY KEY (UID),
    UNIQUE KEY (UserName)
) ENGINE = INNODB;

-- ------------------------------------------------------------------------------


CREATE TABLE FC_StorageElements (
    SEID INTEGER AUTO_INCREMENT PRIMARY KEY,
    SEName VARCHAR(127) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    AliasName VARCHAR(127) DEFAULT '',
    UNIQUE KEY (SEName)
) ENGINE = INNODB;

-- -----------------------------------------------------------------------------


CREATE TABLE FC_DirectoryInfo (
    DirID INTEGER NOT NULL,
    UID SMALLINT UNSIGNED NOT NULL DEFAULT 0,
    GID SMALLINT UNSIGNED NOT NULL DEFAULT 0,
    CreationDate DATETIME,
    ModificationDate DATETIME,
    Mode SMALLINT UNSIGNED NOT NULL DEFAULT 775,
    Status SMALLINT UNSIGNED NOT NULL DEFAULT 0,
    PRIMARY KEY (DirID)
) ENGINE = INNODB;

-- ------------------------------------------------------------------------------
--
-- CREATE TABLE DirectoryInfo(
--   DirID INTEGER AUTO_INCREMENT PRIMARY KEY,
--   Parent INTEGER NOT NULL,
--   Status SMALLINT UNSIGNED NOT NULL DEFAULT 0,
--   DirName VARCHAR(1024) NOT NULL,
--   CreationDate DATETIME,
--   ModificationDate DATETIME,
--   UID CHAR(8) NOT NULL,
--   GID CHAR(8) NOT NULL,
--   Mode SMALLINT UNSIGNED NOT NULL DEFAULT 775,
--   INDEX(Parent),
--   INDEX(Status),
--   INDEX(DirName)
-- );

-- ------------------------------------------------------------------------------

CREATE TABLE FC_DirMeta (
    DirID INTEGER NOT NULL,
    MetaKey VARCHAR(31) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL DEFAULT 'Noname',
    MetaValue VARCHAR(31) NOT NULL DEFAULT 'Noname',
    PRIMARY KEY (DirID,MetaKey)
) ENGINE = INNODB;

-- ------------------------------------------------------------------------------

CREATE TABLE FC_FileMeta (
    FileID INTEGER NOT NULL,
    MetaKey VARCHAR(31) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL DEFAULT 'Noname',
    MetaValue VARCHAR(31) NOT NULL DEFAULT 'Noname',
    PRIMARY KEY (FileID,MetaKey)
) ENGINE = INNODB;

-- ------------------------------------------------------------------------------

CREATE TABLE FC_DirectoryTree (
 DirID INT AUTO_INCREMENT PRIMARY KEY,
 DirName VARCHAR(1024) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
 Parent INT NOT NULL DEFAULT 0,
 INDEX (Parent),
 INDEX (DirName)
) ENGINE = INNODB;

-- ------------------------------------------------------------------------------

CREATE TABLE FC_DirectoryTreeM (
 DirID INT AUTO_INCREMENT PRIMARY KEY,
 DirName VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
 Parent INT NOT NULL DEFAULT 0,
 Level INT NOT NULL,
 INDEX (Level),
 INDEX (Parent),
 INDEX (DirName)
) ENGINE = INNODB;

-- ------------------------------------------------------------------------------

CREATE TABLE FC_DirectoryLevelTree (
 DirID INT AUTO_INCREMENT PRIMARY KEY,
 DirName VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
 Parent INT NOT NULL DEFAULT 0,
 Level INT NOT NULL,
 LPATH1 INT NOT NULL DEFAULT 0,
 LPATH2 INT NOT NULL DEFAULT 0,
 LPATH3 INT NOT NULL DEFAULT 0,
 LPATH4 INT NOT NULL DEFAULT 0,
 LPATH5 INT NOT NULL DEFAULT 0,
 LPATH6 INT NOT NULL DEFAULT 0,
 LPATH7 INT NOT NULL DEFAULT 0,
 LPATH8 INT NOT NULL DEFAULT 0,
 LPATH9 INT NOT NULL DEFAULT 0,
 LPATH10 INT NOT NULL DEFAULT 0,
 LPATH11 INT NOT NULL DEFAULT 0,
 LPATH12 INT NOT NULL DEFAULT 0,
 LPATH13 INT NOT NULL DEFAULT 0,
 LPATH14 INT NOT NULL DEFAULT 0,
 LPATH15 INT NOT NULL DEFAULT 0,
 INDEX (Level),
 INDEX (Parent),
 UNIQUE INDEX (DirName)
) ENGINE = INNODB;

-- ------------------------------------------------------------------------------


CREATE TABLE FC_DirectoryUsage(
   DirID INTEGER NOT NULL,
   INDEX(DirID),
   SEID INTEGER NOT NULL,
   INDEX(SEID),
   SESize BIGINT NOT NULL,
   SEFiles BIGINT NOT NULL,
   LastUpdate DATETIME NOT NULL,
   PRIMARY KEY (DirID,SEID)
) ENGINE = INNODB;

-- ------------------------------------------------------------------------------

CREATE TABLE FC_MetaFields (
  MetaID INT AUTO_INCREMENT PRIMARY KEY,
  MetaName VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
  MetaType VARCHAR(128) NOT NULL
) ENGINE = INNODB;

-- ------------------------------------------------------------------------------

CREATE TABLE FC_FileMetaFields (
  MetaID INT AUTO_INCREMENT PRIMARY KEY,
  MetaName VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
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
