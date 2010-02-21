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
-- -----------------------------------------------------------------------------

drop table if exists FC_DirectoryInfo;
CREATE TABLE FC_DirectoryInfo (
    DirID INTEGER NOT NULL,
    UID SMALLINT UNSIGNED NOT NULL default 0,
    GID SMALLINT UNSIGNED NOT NULL default 0,
    CreationDate DATETIME,
    ModificationDate DATETIME,
    Mode SMALLINT UNSIGNED NOT NULL default 509,
    Status SMALLINT UNSIGNED NOT NULL default 0,
    PRIMARY KEY (DirID)
);

-- ------------------------------------------------------------------------------

drop table if exists FC_FileInfo;
CREATE TABLE FC_FileInfo (
    FileID INTEGER NOT NULL,
    UID SMALLINT UNSIGNED NOT NULL default 0,
    GID SMALLINT UNSIGNED NOT NULL default 0,
    Size BIGINT UNSIGNED NOT NULL default 0,
    CheckSum VARCHAR(32),
    CheckSumType ENUM('Adler32','MD5'),
    Type ENUM('File','Link') NOT NULL default 'File',
    CreationDate DATETIME,
    ModificationDate DATETIME,
    Mode SMALLINT UNSIGNED NOT NULL default 509,
    Status SMALLINT UNSIGNED NOT NULL default 0,
    PRIMARY KEY (FileID)
);

-- ------------------------------------------------------------------------------

drop table if exists FC_DirMeta;
CREATE TABLE FC_DirMeta (
    DirID INTEGER NOT NULL,
    MetaKey VARCHAR(31) NOT NULL default 'Noname',
    MetaValue VARCHAR(31) NOT NULL default 'Noname',
    PRIMARY KEY (DirID,MetaKey)
);
-- ------------------------------------------------------------------------------

drop table if exists FC_Groups;
CREATE TABLE FC_Groups (
    GID INTEGER NOT NULL default -1,
    GroupName VARCHAR(127) NOT NULL default 'Noname',
    PRIMARY KEY (GID),
    UNIQUE KEY (GroupName)
);
-- ------------------------------------------------------------------------------

drop table if exists FC_Users;
CREATE TABLE FC_Users (
    UID INTEGER NOT NULL default -1,
    UserName VARCHAR(127) NOT NULL default 'Noname',
    PRIMARY KEY (UID),
    UNIQUE KEY (UserName)
);
-- ------------------------------------------------------------------------------

drop table if exists FC_StorageElements;
CREATE TABLE FC_StorageElements (
    SEID INTEGER AUTO_INCREMENT PRIMARY KEY,
    SEName VARCHAR(127) NOT NULL default 'Noname',
    AliasName VARCHAR(127) NOT NULL default 'Noname'
);
-- ------------------------------------------------------------------------------

drop table if exists FC_GUID_to_LFN;
CREATE TABLE FC_GUID_to_LFN (
    GUID char(36) NOT NULL,
    LFN VARCHAR(255),
    PRIMARY KEY (GUID)
);

-- ------------------------------------------------------------------------------

drop table if exists FC_GUID_to_File;
CREATE TABLE FC_GUID_to_File (
    GUID char(36) NOT NULL,
    FileID INTEGER UNSIGNED NOT NULL,
    PRIMARY KEY (GUID),
    INDEX (FileID)
);
-- ------------------------------------------------------------------------------

drop table if exists FC_ReplicaInfo;
CREATE TABLE FC_ReplicaInfo (
  RepID INTEGER NOT NULL AUTO_INCREMENT,
  RepType ENUM ('Master','Slave') NOT NULL DEFAULT 'Master',
  Status SMALLINT UNSIGNED NOT NULL default 0,
  CreationDate DATETIME,
  ModificationDate DATETIME,
  PFN varchar(256),
  PRIMARY KEY ( RepID ),
  INDEX (Status)
);

drop table if exists FC_DirectoryTree;
CREATE TABLE FC_DirectoryTree (
 DirID INT AUTO_INCREMENT PRIMARY KEY,
 DirName VARCHAR(1024) NOT NULL,
 Parent INT NOT NULL DEFAULT 0,
 INDEX (Parent),
 INDEX (DirName)
);

drop table if exists FC_Files;
CREATE TABLE FC_Files (
 FileID INT AUTO_INCREMENT PRIMARY KEY,
 DirID INT NOT NULL,
 FileName VARCHAR(128) NOT NULL,
 INDEX (DirID),
 INDEX (FileName)
);

drop table if exists FC_Replicas;
CREATE TABLE FC_Replicas (
 RepID INT AUTO_INCREMENT PRIMARY KEY,
 FileID INT NOT NULL,
 SEID INTEGER NOT NULL,
 INDEX (FileID),
 INDEX (SEID)
);

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
 INDEX (DirName)
);

drop table if exists FC_Meta_Fields;
CREATE TABLE FC_Meta_Fields (
  MetaID INT AUTO_INCREMENT PRIMARY KEY,
  MetaName VARCHAR(64) NOT NULL,
  MetaType VARCHAR(128) NOT NULL
);
