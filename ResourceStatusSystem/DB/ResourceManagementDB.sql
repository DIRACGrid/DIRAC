-- -----------------------------------------------------------
-- Resource Management database definition
-- -----------------------------------------------------------

DROP DATABASE IF EXISTS ResourceManagementDB;
CREATE DATABASE ResourceManagementDB;
--
-- Must set passwords for database user by replacing "must_be_set".
--
-- Create user DIRAC
USE mysql;
GRANT SELECT,INSERT,LOCK TABLES,UPDATE,DELETE,CREATE,DROP,ALTER ON ResourceManagementDB.* TO Dirac@localhost IDENTIFIED BY 'must_be_set';
GRANT SELECT,INSERT,LOCK TABLES,UPDATE,DELETE,CREATE,DROP,ALTER ON ResourceManagementDB.* TO Dirac@'%' IDENTIFIED BY 'must_be_set';

USE ResourceManagementDB;

DROP TABLE IF EXISTS PolicyResult;
CREATE TABLE PolicyResult(
  PolicyResultID INT UNSIGNED NOT NULL AUTO_INCREMENT,
  Granularity VARCHAR(32) NOT NULL,
  Name VARCHAR(64) NOT NULL,
  INDEX (Name),
  PolicyName VARCHAR(64) NOT NULL,
  INDEX (PolicyName),
  StatusType VARCHAR(16) NOT NULL DEFAULT '',
  INDEX (StatusType),
  Status VARCHAR(8) NOT NULL,
  Index(Status),
  Reason VARCHAR(255) NOT NULL DEFAULT 'Unspecified',
  DateEffective DATETIME NOT NULL,
  LastCheckTime DATETIME NOT NULL,
  UNIQUE KEY( Name, StatusType, PolicyName ),
  PRIMARY KEY(PolicyResultID)
) Engine=InnoDB;

DROP TABLE IF EXISTS ClientCache;
CREATE TABLE ClientCache(
  ClientCacheID INT UNSIGNED NOT NULL AUTO_INCREMENT,
  Name VARCHAR(64) NOT NULL,
  INDEX (Name),
  CommandName VARCHAR(64) NOT NULL,
  INDEX (CommandName),
  Opt_ID VARCHAR(64),
  Value VARCHAR(16) NOT NULL,
  Result VARCHAR(255) NOT NULL,
  DateEffective DATETIME NOT NULL,
  LastCheckTime DATETIME NOT NULL,
  UNIQUE KEY( Name, CommandName, Value ),
  PRIMARY KEY( ClientCacheID )
) Engine=InnoDB;

DROP TABLE IF EXISTS AccountingCache;
CREATE TABLE AccountingCache(
  AccountingCacheID INT UNSIGNED NOT NULL AUTO_INCREMENT,
  Name VARCHAR(64) NOT NULL,
  INDEX (Name),
  PlotType VARCHAR(16) NOT NULL,
  PlotName VARCHAR(64) NOT NULL,
  INDEX (PlotName),
  Result TEXT NOT NULL,
  DateEffective DATETIME NOT NULL,
  LastCheckTime DATETIME NOT NULL,
  UNIQUE KEY( Name, PlotType, PlotName ),
  PRIMARY KEY(AccountingCacheID)
) Engine=InnoDB;

DROP TABLE IF EXISTS EnvironmentCache;
CREATE TABLE EnvironmentCache(
  HashEnv VARCHAR(128) NOT NULL,
  INDEX ( HashEnv ),
  SiteName VARCHAR(64) NOT NULL,
  INDEX ( SiteName ),
  Environment BLOB,
  PRIMARY KEY( HashEnv, SiteName )
) Engine=InnoDB;

DROP TABLE IF EXISTS UserRegistryCache;
CREATE TABLE UserRegistryCache(
  Login VARCHAR(16),
  Name VARCHAR(64) NOT NULL,
  Email VARCHAR(64) NOT NULL,
  PRIMARY KEY( Login )
) Engine=InnoDB;
