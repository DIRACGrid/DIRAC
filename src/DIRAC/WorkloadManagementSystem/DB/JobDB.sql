-- ------------------------------------------------------------------------------
--
--  Schema definition for the JobDB database - the main database of the DIRAC
--  Workload Management System
-- -
-- ------------------------------------------------------------------------------

-- When installing via dirac tools, the following is not needed (still here for reference)
--
-- DROP DATABASE IF EXISTS JobDB;
-- CREATE DATABASE JobDB;
-- ------------------------------------------------------------------------------
-- Database owner definition
-- USE mysql;
-- Must set passwords for database user by replacing "must_be_set".
--
-- GRANT SELECT,INSERT,LOCK TABLES,UPDATE,DELETE,CREATE,DROP,ALTER,REFERENCES ON JobDB.* TO Dirac@'%' IDENTIFIED BY 'must_be_set';
-- FLUSH PRIVILEGES;

-- -----------------------------------------------------------------------------
USE JobDB;

-- ------------------------------------------------------------------------------
DROP TABLE IF EXISTS `JobJDLs`;
CREATE TABLE `JobJDLs` (
  `JobID` INT(11) UNSIGNED NOT NULL AUTO_INCREMENT,
  `JDL` MEDIUMBLOB NOT NULL,
  `JobRequirements` BLOB NOT NULL,
  `OriginalJDL` MEDIUMBLOB NOT NULL,
  PRIMARY KEY (`JobID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- ------------------------------------------------------------------------------
DROP TABLE IF EXISTS `Jobs`;
CREATE TABLE `Jobs` (
  `JobID` INT(11) UNSIGNED NOT NULL DEFAULT 0,
  `JobType` VARCHAR(32) NOT NULL DEFAULT 'user',
  `DIRACSetup` VARCHAR(32) NOT NULL DEFAULT 'test',
  `JobGroup` VARCHAR(32) NOT NULL DEFAULT '00000000',
  `JobSplitType` ENUM('Single','Master','Subjob','DAGNode') NOT NULL DEFAULT 'Single',
  `MasterJobID` INT(11) UNSIGNED NOT NULL DEFAULT 0,
  `Site` VARCHAR(100) NOT NULL DEFAULT 'ANY',
  `JobName` VARCHAR(128) NOT NULL DEFAULT 'Unknown',
  `Owner` VARCHAR(64) NOT NULL DEFAULT 'Unknown',
  `OwnerDN` VARCHAR(255) NOT NULL DEFAULT 'Unknown',
  `OwnerGroup` VARCHAR(128) NOT NULL DEFAULT 'Unknown',
  `SubmissionTime` DATETIME DEFAULT NULL,
  `RescheduleTime` DATETIME DEFAULT NULL,
  `LastUpdateTime` DATETIME DEFAULT NULL,
  `StartExecTime` DATETIME DEFAULT NULL,
  `HeartBeatTime` DATETIME DEFAULT NULL,
  `EndExecTime` DATETIME DEFAULT NULL,
  `Status` VARCHAR(32) NOT NULL DEFAULT 'Received',
  `MinorStatus` VARCHAR(128) NOT NULL DEFAULT 'Unknown',
  `ApplicationStatus` VARCHAR(255) DEFAULT 'Unknown',
  `ApplicationNumStatus` INT(11) NOT NULL DEFAULT 0,
  `UserPriority` INT(11) NOT NULL DEFAULT 0,
  `SystemPriority` INT(11) NOT NULL DEFAULT 0,
  `RescheduleCounter` INT(11) NOT NULL DEFAULT 0,
  `VerifiedFlag` ENUM('True','False') NOT NULL DEFAULT 'False',
  `DeletedFlag` ENUM('True','False') NOT NULL DEFAULT 'False',
  `KilledFlag` ENUM('True','False') NOT NULL DEFAULT 'False',
  `FailedFlag` ENUM('True','False') NOT NULL DEFAULT 'False',
  `ISandboxReadyFlag` ENUM('True','False') NOT NULL DEFAULT 'False',
  `OSandboxReadyFlag` ENUM('True','False') NOT NULL DEFAULT 'False',
  `RetrievedFlag` ENUM('True','False') NOT NULL DEFAULT 'False',
  `AccountedFlag` ENUM('True','False','Failed') NOT NULL DEFAULT 'False',
  PRIMARY KEY (`JobID`),
  FOREIGN KEY (`JobID`) REFERENCES `JobJDLs`(`JobID`),
  KEY `JobType` (`JobType`),
  KEY `DIRACSetup` (`DIRACSetup`),
  KEY `JobGroup` (`JobGroup`),
  KEY `JobSplitType` (`JobSplitType`),
  KEY `Site` (`Site`),
  KEY `Owner` (`Owner`),
  KEY `OwnerDN` (`OwnerDN`),
  KEY `OwnerGroup` (`OwnerGroup`),
  KEY `Status` (`Status`),
  KEY `MinorStatus` (`MinorStatus`),
  KEY `ApplicationStatus` (`ApplicationStatus`),
  KEY `StatusSite` (`Status`,`Site`),
  KEY `LastUpdateTime` (`LastUpdateTime`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- ------------------------------------------------------------------------------
DROP TABLE IF EXISTS `InputData`;
CREATE TABLE `InputData` (
  `JobID` INT(11) UNSIGNED NOT NULL,
  `LFN` VARCHAR(255) NOT NULL DEFAULT '',
  `Status` VARCHAR(32) NOT NULL DEFAULT 'AprioriGood',
  PRIMARY KEY (`JobID`,`LFN`),
  FOREIGN KEY (`JobID`) REFERENCES `Jobs`(`JobID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- ------------------------------------------------------------------------------
DROP TABLE IF EXISTS `JobParameters`;
CREATE TABLE `JobParameters` (
  `JobID` INT(11) UNSIGNED NOT NULL,
  `Name` VARCHAR(100) NOT NULL,
  `Value` BLOB NOT NULL,
  PRIMARY KEY (`JobID`,`Name`),
  FOREIGN KEY (`JobID`) REFERENCES `Jobs`(`JobID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- ------------------------------------------------------------------------------
DROP TABLE IF EXISTS `OptimizerParameters`;
CREATE TABLE `OptimizerParameters` (
  `JobID` INT(11) UNSIGNED NOT NULL,
  `Name` VARCHAR(100) NOT NULL,
  `Value` MEDIUMBLOB NOT NULL,
  PRIMARY KEY (`JobID`,`Name`),
  FOREIGN KEY (`JobID`) REFERENCES `Jobs`(`JobID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- ------------------------------------------------------------------------------
DROP TABLE IF EXISTS `AtticJobParameters`;
CREATE TABLE `AtticJobParameters` (
  `JobID` INT(11) UNSIGNED NOT NULL,
  `Name` VARCHAR(100) NOT NULL,
  `Value` BLOB NOT NULL,
  `RescheduleCycle` INT(11) UNSIGNED NOT NULL,
  PRIMARY KEY (`JobID`,`Name`,`RescheduleCycle`),
  FOREIGN KEY (`JobID`) REFERENCES `Jobs`(`JobID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- ------------------------------------------------------------------------------
DROP TABLE IF EXISTS `SiteMask`;
CREATE TABLE `SiteMask` (
  `Site` VARCHAR(64) NOT NULL,
  `Status` VARCHAR(64) NOT NULL,
  `LastUpdateTime` DATETIME NOT NULL,
  `Author` VARCHAR(255) NOT NULL,
  `Comment` BLOB NOT NULL,
  PRIMARY KEY (`Site`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

DROP TABLE IF EXISTS `SiteMaskLogging`;
CREATE TABLE `SiteMaskLogging` (
  `Site` VARCHAR(64) NOT NULL,
  `Status` VARCHAR(64) NOT NULL,
  `UpdateTime` DATETIME NOT NULL,
  `Author` VARCHAR(255) NOT NULL,
  `Comment` BLOB NOT NULL,
  PRIMARY KEY (`Site`,`UpdateTime`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- ------------------------------------------------------------------------------
DROP TABLE IF EXISTS `HeartBeatLoggingInfo`;
CREATE TABLE `HeartBeatLoggingInfo` (
  `JobID` INT(11) UNSIGNED NOT NULL,
  `Name` VARCHAR(100) NOT NULL,
  `Value` BLOB NOT NULL,
  `HeartBeatTime` DATETIME NOT NULL,
  PRIMARY KEY (`JobID`,`Name`,`HeartBeatTime`),
  FOREIGN KEY (`JobID`) REFERENCES `Jobs`(`JobID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- ------------------------------------------------------------------------------
DROP TABLE IF EXISTS `JobCommands`;
CREATE TABLE `JobCommands` (
  `JobID` INT(11) UNSIGNED NOT NULL,
  `Command` VARCHAR(100) NOT NULL,
  `Arguments` VARCHAR(100) NOT NULL,
  `Status` VARCHAR(64) NOT NULL DEFAULT 'Received',
  `ReceptionTime` DATETIME NOT NULL,
  `ExecutionTime` DATETIME DEFAULT NULL,
  PRIMARY KEY (`JobID`,`Arguments`,`ReceptionTime`),
  FOREIGN KEY (`JobID`) REFERENCES `Jobs`(`JobID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
