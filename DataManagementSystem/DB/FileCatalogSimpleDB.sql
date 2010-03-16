-- $HeaderURL: $
-- ------------------------------------------------------------------------------
DROP DATABASE IF EXISTS FileCatalogDB;
-- ------------------------------------------------------------------------------
CREATE DATABASE FileCatalogDB;

use mysql;
delete from user where user='Dirac';
GRANT SELECT,INSERT,LOCK TABLES,UPDATE,DELETE,CREATE,DROP,ALTER ON FileCatalogDB.* TO Dirac@localhost IDENTIFIED BY 'must_be_set';

FLUSH PRIVILEGES;

USE FileCatalogDB;
-- -----------------------------------------------------------------------------

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
  Mode SMALLINT UNSIGNED NOT NULL DEFAULT 509,
  INDEX(Parent),
  INDEX(Status),
  INDEX(DirName)
);

DROP TABLE IF EXISTS FileInfo;
CREATE TABLE FileInfo(
  FileID INTEGER AUTO_INCREMENT PRIMARY KEY,
  DirID INTEGER NOT NULL,
  Size BIGINT UNSIGNED NOT NULL DEFAULT 0,
  GUID CHAR(36) NOT NULL,
  FileName VARCHAR(128) NOT NULL,
  Checksum VARCHAR(32),
  ChecksumType CHAR(8) DEFAULT 'Adler32',
  Type CHAR(4) NOT NULL DEFAULT 'File',
  UID CHAR(8) NOT NULL,
  GID CHAR(8) NOT NULL,
  CreationDate DATETIME,
  ModificationDate DATETIME,
  Mode SMALLINT UNSIGNED NOT NULL DEFAULT 509,
  Status SMALLINT UNSIGNED NOT NULL DEFAULT 0,
  INDEX(DirID),
  INDEX(FileName),
  INDEX(Status)
);

DROP TABLE IF EXISTS ReplicaInfo;
CREATE TABLE ReplicaInfo(
  FileID INTEGER NOT NULL,
  SEName VARCHAR(127) NOT NULL,
  RepType CHAR(8) NOT NULL DEFAULT 'Replica',
  Status CHAR(1) NOT NULL DEFAULT 'U',
  CreationDate DATETIME,
  ModificationDate DATETIME,
  PFN VARCHAR(1024),
  PRIMARY KEY (FileID,SEName),
  INDEX(FileID),
  INDEX(SEName),
  INDEX (Status)
);