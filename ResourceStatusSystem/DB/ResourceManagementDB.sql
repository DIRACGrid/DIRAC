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
GRANT SELECT,INSERT,LOCK TABLES,UPDATE,DELETE,CREATE,DROP,ALTER ON ResourceManagementDB.* TO 'Dirac'@'localhost' IDENTIFIED BY 'must_be_set';
GRANT SELECT,INSERT,LOCK TABLES,UPDATE,DELETE,CREATE,DROP,ALTER ON ResourceManagementDB.* TO 'Dirac'@'%' IDENTIFIED BY 'must_be_set';

FLUSH PRIVILEGES;

-- -----------------------------------------------------------

USE ResourceManagementDB;

DROP TABLE IF EXISTS Status;
CREATE TABLE Status(
  Status VARCHAR(8) NOT NULL,
  Description BLOB,
  PRIMARY KEY(Status)
) Engine=InnoDB;

DROP TABLE IF EXISTS PolicyRes;
CREATE TABLE PolicyRes(
  prID INT UNSIGNED NOT NULL AUTO_INCREMENT,
  Granularity VARCHAR(32) NOT NULL,
  Name VARCHAR(64) NOT NULL,
  INDEX (Name),
  PolicyName VARCHAR(64) NOT NULL,
  INDEX (PolicyName),
  Status VARCHAR(8) NOT NULL,
  Index(Status),
  Reason VARCHAR(255) NOT NULL DEFAULT 'Unspecified',
  DateEffective DATETIME NOT NULL,
  LastCheckTime DATETIME NOT NULL,
  FOREIGN KEY (Status) REFERENCES Status(Status),
  PRIMARY KEY(prID)
) Engine=InnoDB;

DROP TABLE IF EXISTS ClientsCache;
CREATE TABLE ClientsCache(
  ccID INT UNSIGNED NOT NULL AUTO_INCREMENT,
  Name VARCHAR(64) NOT NULL,
  INDEX (Name),
  CommandName VARCHAR(64) NOT NULL,
  INDEX (CommandName),
  Opt_ID VARCHAR(64),
  Value VARCHAR(16) NOT NULL,
  Result VARCHAR(255) NOT NULL,
  DateEffective DATETIME NOT NULL,
  LastCheckTime DATETIME NOT NULL,
  PRIMARY KEY(ccID)
) Engine=InnoDB;

DROP TABLE IF EXISTS AccountingCache;
CREATE TABLE AccountingCache(
  acID INT UNSIGNED NOT NULL AUTO_INCREMENT,
  Name VARCHAR(64) NOT NULL,
  INDEX (Name),
  PlotType VARCHAR(16) NOT NULL,
  PlotName VARCHAR(64) NOT NULL,
  INDEX (PlotName),
  Result TEXT NOT NULL,
  DateEffective DATETIME NOT NULL,
  LastCheckTime DATETIME NOT NULL,
  PRIMARY KEY(acID)
) Engine=InnoDB;


DROP TABLE IF EXISTS EnvironmentCache;
CREATE TABLE EnvironmentCache(
  Hash VARCHAR(128) NOT NULL,
  INDEX (Hash),
  SiteName VARCHAR(64) NOT NULL,
  INDEX (SiteName),
  DateEffective DATETIME NOT NULL,
  Environment BLOB,
  PRIMARY KEY(Hash)
) Engine=InnoDB;
