
--------------------------------------------------------------------------------
DROP DATABASE IF EXISTS FileCatalog;
--------------------------------------------------------------------------------
CREATE DATABASE FileCatalog;

use mysql;
delete from user where user='Dirac';
--
-- Must set passwords for database user by replacing "must_be_set".
--
GRANT SELECT,INSERT,LOCK TABLES,UPDATE,DELETE,CREATE,DROP,ALTER ON FileCatalog.* TO Dirac@localhost IDENTIFIED BY 'must_be_set';

FLUSH PRIVILEGES;

-------------------------------------------------------------------------------
USE FileCatalog;

drop table if exists FC_Directories;
CREATE TABLE FC_Directories (
    DirID INTEGER NOT NULL auto_increment,
    DirPath VARCHAR(255) NOT NULL default '/',
    GUID char(36),
    UID INTEGER NOT NULL default 0,
    GID INTEGER NOT NULL default 0,
    ParentID INTEGER NOT NULL default -1,
    Name VARCHAR(127) NOT NULL default 'Noname',
    CreateDate DATETIME,
    ModifyDate DATETIME,
    AccessDate DATETIME,
    Umask INTEGER(12),
    MinACL INTEGER(12),
    PRIMARY KEY (DirID)
);
--------------------------------------------------------------------------------

drop table if exists FC_Subdirectories;
CREATE TABLE FC_Subdirectories (
    DirID INTEGER NOT NULL,
    SubDirID INTEGER NOT NULL
);
--------------------------------------------------------------------------------

drop table if exists FC_DirMeta;
CREATE TABLE FC_DirMeta (
    DirID INTEGER NOT NULL,
    MetaKey VARCHAR(31) NOT NULL default 'Noname',
    MetaValue VARCHAR(31) NOT NULL default 'Noname',
    PRIMARY KEY (DirID,MetaKey)
);
--------------------------------------------------------------------------------

drop table if exists FC_Groups;
CREATE TABLE FC_Groups (
    GID INTEGER NOT NULL default 0,
    GroupName VARCHAR(127) NOT NULL default 'Noname',
    PRIMARY KEY (GID)
);
--------------------------------------------------------------------------------

drop table if exists FC_Users;
CREATE TABLE FC_Users (
    UID INTEGER NOT NULL default 0,
    UserDN VARCHAR(255) NOT NULL default 'Noname',
    NickName VARCHAR(127) NOT NULL default 'Noname',
    PRIMARY KEY (UID)
);
--------------------------------------------------------------------------------

drop table if exists FC_StorageElements;
CREATE TABLE FC_StorageElements (
    SEID INTEGER NOT NULL default 0,
    SEName VARCHAR(127) NOT NULL default 'Noname',
    AliasName VARCHAR(127) NOT NULL default 'Noname'
);
--------------------------------------------------------------------------------

drop table if exists FC_SE_Access;
CREATE TABLE FC_SE_Access (
    SEID INTEGER NOT NULL default 0,
    Host VARCHAR(127) NOT NULL,
    Port VARCHAR(127) NOT NULL,
    Path VARCHAR(127) NOT NULL,
    Protocol VARCHAR(127) NOT NULL,
    AccessUser VARCHAR(127) NOT NULL default 'Noname',
    AccessPassword VARCHAR(127) NOT NULL default ''
);
--------------------------------------------------------------------------------

drop table if exists FC_GUID_to_LFN;
CREATE TABLE FC_GUID_to_LFN (
    GUID char(36) NOT NULL,
    LFN VARCHAR(255),
    PRIMARY KEY (GUID)
);
--------------------------------------------------------------------------------

drop table if exists FC_Replicas;
CREATE TABLE FC_Replicas (
  RepID INTEGER NOT NULL AUTO_INCREMENT,
  FileID INTEGER NOT NULL,
  DirID INTEGER NOT NULL,
  SEID INTEGER NOT NULL,
  RepType ENUM ('Master','Slave'),
  PFN varchar(256),
  PRIMARY KEY ( RepID, DirID, FileID )
);
