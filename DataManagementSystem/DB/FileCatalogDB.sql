-- $HeaderURL: $
-- ------------------------------------------------------------------------------
DROP DATABASE IF EXISTS FileCatalogDB;
-- ------------------------------------------------------------------------------
CREATE DATABASE FileCatalogDB;

use mysql;
delete from user where user='Dirac';
--
-- Must set passwords for database user by replacing "must_be_set".
--
GRANT SELECT,INSERT,LOCK TABLES,UPDATE,DELETE,CREATE,DROP,ALTER ON FileCatalogDB.* TO Dirac@localhost IDENTIFIED BY 'must_be_set';

FLUSH PRIVILEGES;

USE FileCatalogDB;

drop table if exists FC_Files;
CREATE TABLE FC_Files(
    FileID INT AUTO_INCREMENT PRIMARY KEY,
    DirID INT NOT NULL,
    FileName VARCHAR(128) NOT NULL,
    INDEX (DirID),
    INDEX (FileName),
    INDEX (DirID,FileName)
);

-- ------------------------------------------------------------------------------
drop table if exists FC_FileInfo;
CREATE TABLE FC_FileInfo (
    FileID INTEGER NOT NULL PRIMARY KEY,
    INDEX(FileID),
    GUID char(36) NOT NULL,
    INDEX(GUID),
    UID SMALLINT UNSIGNED NOT NULL DEFAULT 0,
    GID SMALLINT UNSIGNED NOT NULL DEFAULT 0,
    Size BIGINT UNSIGNED NOT NULL DEFAULT 0,
    Checksum VARCHAR(32),
    CheckSumType ENUM('Adler32','MD5'),
    Type ENUM('File','Link') NOT NULL DEFAULT 'File',
    CreationDate DATETIME,
    ModificationDate DATETIME,
    Mode SMALLINT UNSIGNED NOT NULL DEFAULT 775,
    Status SMALLINT UNSIGNED NOT NULL DEFAULT 0,
    INDEX(Status)
);

-- Make additions to the FC_Files table to include the FC_FileInfo information
ALTER TABLE FC_Files ADD COLUMN GUID CHAR(36) NOT NULL AFTER FileName;
ALTER TABLE FC_Files ADD INDEX (GUID);
ALTER TABLE FC_Files ADD COLUMN UID SMALLINT UNSIGNED NOT NULL DEFAULT 0 AFTER GUID;
ALTER TABLE FC_Files ADD COLUMN GID SMALLINT UNSIGNED NOT NULL DEFAULT 0 AFTER UID;
ALTER TABLE FC_Files ADD COLUMN Size BIGINT UNSIGNED NOT NULL DEFAULT 0 AFTER GID;
ALTER TABLE FC_Files ADD COLUMN Checksum VARCHAR(32) AFTER Size;
ALTER TABLE FC_Files ADD COLUMN CheckSumType ENUM('Adler32','MD5') AFTER Checksum;
ALTER TABLE FC_Files ADD COLUMN Type ENUM('File','Link') NOT NULL DEFAULT 'File' AFTER CheckSumType;
ALTER TABLE FC_Files ADD COLUMN CreationDate DATETIME AFTER Type;
ALTER TABLE FC_Files ADD COLUMN ModificationDate DATETIME AFTER CreationDate;
ALTER TABLE FC_Files ADD COLUMN Mode SMALLINT UNSIGNED NOT NULL DEFAULT 775 AFTER ModificationDate;
ALTER TABLE FC_Files ADD COLUMN Status SMALLINT UNSIGNED NOT NULL DEFAULT 0 AFTER Mode;
ALTER TABLE FC_Files ADD INDEX (Status);

-- Make additions to the FC_FileInfo table to include the FC_Files information
-- ALTER TABLE FC_FileInfo MODIFY COLUMN FileID INT AUTO_INCREMENT PRIMARY KEY;
-- ALTER TABLE FC_FileInfo ADD COLUMN DirID INTEGER NOT NULL AFTER FileID;
-- ALTER TABLE FC_FileInfo ADD INDEX (DirID);
-- ALTER TABLE FC_FileInfo ADD COLUMN FileName VARCHAR(128) NOT NULL AFTER DirID;
-- ALTER TABLE FC_FileInfo ADD INDEX (FileName);

-- ------------------------------------------------------------------------------
DROP TABLE IF EXISTS FC_Statuses;
CREATE TABLE FC_Statuses (
    StatusID INT AUTO_INCREMENT PRIMARY KEY,
    Status VARCHAR(32),
    INDEX(Status),
    INDEX(StatusID)
);

-- -----------------------------------------------------------------------------
drop table if exists FC_Replicas;
CREATE TABLE FC_Replicas (
    RepID INT AUTO_INCREMENT PRIMARY KEY,
    FileID INT NOT NULL,
    SEID INTEGER NOT NULL,
    INDEX (FileID),
    INDEX (SEID),
    INDEX (FileID,SEID)
);

-- ------------------------------------------------------------------------------
drop table if exists FC_ReplicaInfo;
CREATE TABLE FC_ReplicaInfo (
    RepID INTEGER NOT NULL AUTO_INCREMENT,
    RepType ENUM ('Master','Replica') NOT NULL DEFAULT 'Master',
    Status SMALLINT UNSIGNED NOT NULL DEFAULT 0,
    CreationDate DATETIME,
    ModificationDate DATETIME,
    PFN VARCHAR(1024),
    PRIMARY KEY (RepID),
    INDEX (Status),
    INDEX (RepID)
);

-- Make additions to the FC_Replicas table to include the FC_ReplicaInfo information
ALTER TABLE FC_Replicas ADD COLUMN RepType ENUM ('Master','Replica') NOT NULL DEFAULT 'Master' AFTER SEID;
ALTER TABLE FC_Replicas ADD COLUMN Status SMALLINT UNSIGNED NOT NULL DEFAULT 0 AFTER RepType;
ALTER TABLE FC_Replicas ADD INDEX (Status);
ALTER TABLE FC_Replicas ADD COLUMN CreationDate DATETIME AFTER Status;
ALTER TABLE FC_Replicas ADD COLUMN ModificationDate DATETIME AFTER CreationDate;
ALTER TABLE FC_Replicas ADD COLUMN PFN VARCHAR(1024) AFTER ModificationDate;

-- Make additions to the FC_ReplicaInfo table to include the FC_Replicas information
-- ALTER TABLE FC_ReplicaInfo ADD COLUMN FileID INTEGER NOT NULL AFTER RepID;
-- ALTER TABLE FC_ReplicaInfo ADD INDEX (FileID);
-- ALTER TABLE FC_ReplicaInfo ADD COLUMN SEID INTEGER NOT NULL AFTER FileID;
-- ALTER TABLE FC_ReplicaInfo ADD INDEX (SEID);

-- ------------------------------------------------------------------------------

drop table if exists FC_Groups;
CREATE TABLE FC_Groups (
    GID INTEGER NOT NULL AUTO_INCREMENT,
    GroupName VARCHAR(127) NOT NULL,
    PRIMARY KEY (GID),
    UNIQUE KEY (GroupName)
);

-- ------------------------------------------------------------------------------
drop table if exists FC_Users;
CREATE TABLE FC_Users (
    UID INTEGER NOT NULL AUTO_INCREMENT,
    UserName VARCHAR(127) NOT NULL,
    PRIMARY KEY (UID),
    UNIQUE KEY (UserName)
);

-- ------------------------------------------------------------------------------

drop table if exists FC_StorageElements;
CREATE TABLE FC_StorageElements (
    SEID INTEGER AUTO_INCREMENT PRIMARY KEY,
    SEName VARCHAR(127) NOT NULL,
    AliasName VARCHAR(127) DEFAULT '',
    UNIQUE KEY (SEName)
);

-- -----------------------------------------------------------------------------

drop table if exists FC_DirectoryInfo;
CREATE TABLE FC_DirectoryInfo (
    DirID INTEGER NOT NULL,
    UID SMALLINT UNSIGNED NOT NULL DEFAULT 0,
    GID SMALLINT UNSIGNED NOT NULL DEFAULT 0,
    CreationDate DATETIME,
    ModificationDate DATETIME,
    Mode SMALLINT UNSIGNED NOT NULL DEFAULT 775,
    Status SMALLINT UNSIGNED NOT NULL DEFAULT 0,
    PRIMARY KEY (DirID)
);

-- ------------------------------------------------------------------------------
DROP TABLE IF EXISTS DirectoryInfo;
CREATE TABLE DirectoryInfo(
  DirID INTEGER AUTO_INCREMENT PRIMARY KEY,
  Parent INTEGER NOT NULL,
  Status SMALLINT UNSIGNED NOT NULL DEFAULT 0,
  DirName VARCHAR(1024) NOT NULL,
  CreationDate DATETIME,
  ModificationDate DATETIME,
  UID CHAR(8) NOT NULL,
  GID CHAR(8) NOT NULL,
  Mode SMALLINT UNSIGNED NOT NULL DEFAULT 775,
  INDEX(Parent),
  INDEX(Status),
  INDEX(DirName)
);

-- ------------------------------------------------------------------------------
drop table if exists FC_DirMeta;
CREATE TABLE FC_DirMeta (
    DirID INTEGER NOT NULL,
    MetaKey VARCHAR(31) NOT NULL DEFAULT 'Noname',
    MetaValue VARCHAR(31) NOT NULL DEFAULT 'Noname',
    PRIMARY KEY (DirID,MetaKey)
);

-- ------------------------------------------------------------------------------
drop table if exists FC_DirectoryTree;
CREATE TABLE FC_DirectoryTree (
 DirID INT AUTO_INCREMENT PRIMARY KEY,
 DirName VARCHAR(1024) NOT NULL,
 Parent INT NOT NULL DEFAULT 0,
 INDEX (Parent),
 INDEX (DirName)
);

-- ------------------------------------------------------------------------------
drop table if exists FC_DirectoryTreeM;
CREATE TABLE FC_DirectoryTreeM (
 DirID INT AUTO_INCREMENT PRIMARY KEY,
 DirName VARCHAR(255) NOT NULL,
 Parent INT NOT NULL DEFAULT 0,
 Level INT NOT NULL,
 INDEX (Level),
 INDEX (Parent),
 INDEX (DirName)
);

-- ------------------------------------------------------------------------------
drop table if exists FC_DirectoryLevelTree;
CREATE TABLE FC_DirectoryLevelTree (
 DirID INT AUTO_INCREMENT PRIMARY KEY,
 DirName VARCHAR(255) NOT NULL,
 Parent INT NOT NULL DEFAULT 0,
 Level INT NOT NULL,
 LPATH1 SMALLINT NOT NULL DEFAULT 0,
 LPATH2 SMALLINT NOT NULL DEFAULT 0,
 LPATH3 SMALLINT NOT NULL DEFAULT 0,
 LPATH4 SMALLINT NOT NULL DEFAULT 0,
 LPATH5 SMALLINT NOT NULL DEFAULT 0,
 LPATH6 SMALLINT NOT NULL DEFAULT 0,
 LPATH7 SMALLINT NOT NULL DEFAULT 0,
 LPATH8 SMALLINT NOT NULL DEFAULT 0,
 LPATH9 SMALLINT NOT NULL DEFAULT 0,
 LPATH10 SMALLINT NOT NULL DEFAULT 0,
 INDEX (Level),
 INDEX (Parent),
 UNIQUE INDEX (DirName)
);

-- ------------------------------------------------------------------------------
drop table if exists FC_Meta_Fields;
CREATE TABLE FC_Meta_Fields (
  MetaID INT AUTO_INCREMENT PRIMARY KEY,
  MetaName VARCHAR(64) NOT NULL,
  MetaType VARCHAR(128) NOT NULL
);

-- ------------------------------------------------------------------------------
drop table if exists FC_FileAncestors;
CREATE TABLE FC_FileAncestors (
  FileID INT NOT NULL DEFAULT 0,
  AncestorID INT NOT NULL DEFAULT 0,
  AncestorDepth INT NOT NULL DEFAULT 0,
  INDEX (FileID),
  INDEX (AncestorID),
  INDEX (AncestorDepth)
);
