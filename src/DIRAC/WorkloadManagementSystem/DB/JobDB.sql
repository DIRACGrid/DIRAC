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
  `JDL` MEDIUMTEXT NOT NULL,
  `JobRequirements` TEXT NOT NULL,
  `OriginalJDL` MEDIUMTEXT NOT NULL,
  PRIMARY KEY (`JobID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ------------------------------------------------------------------------------
DROP TABLE IF EXISTS `Jobs`;
CREATE TABLE `Jobs` (
  `JobID` INT(11) UNSIGNED NOT NULL DEFAULT 0,
  `JobType` VARCHAR(32) NOT NULL DEFAULT 'user',
  `JobGroup` VARCHAR(32) NOT NULL DEFAULT '00000000',
  `Site` VARCHAR(100) NOT NULL DEFAULT 'ANY',
  `JobName` VARCHAR(128) NOT NULL DEFAULT 'Unknown',
  `Owner` VARCHAR(64) NOT NULL DEFAULT 'Unknown',
  `OwnerGroup` VARCHAR(128) NOT NULL DEFAULT 'Unknown',
  `VO` VARCHAR(32) NOT NULL DEFAULT 'Unknown',
  `SubmissionTime` DATETIME DEFAULT NULL,
  `RescheduleTime` DATETIME DEFAULT NULL,
  `LastUpdateTime` DATETIME DEFAULT NULL,
  `StartExecTime` DATETIME DEFAULT NULL,
  `HeartBeatTime` DATETIME DEFAULT NULL,
  `EndExecTime` DATETIME DEFAULT NULL,
  `Status` VARCHAR(32) NOT NULL DEFAULT 'Received',
  `MinorStatus` VARCHAR(128) NOT NULL DEFAULT 'Unknown',
  `ApplicationStatus` VARCHAR(255) DEFAULT 'Unknown',
  `UserPriority` INT(11) NOT NULL DEFAULT 0,
  `RescheduleCounter` INT(11) NOT NULL DEFAULT 0,
  `VerifiedFlag` ENUM('True','False') NOT NULL DEFAULT 'False',
  `AccountedFlag` ENUM('True','False','Failed') NOT NULL DEFAULT 'False',
  PRIMARY KEY (`JobID`),
  FOREIGN KEY (`JobID`) REFERENCES `JobJDLs`(`JobID`),
  KEY `JobType` (`JobType`),
  KEY `JobGroup` (`JobGroup`),
  KEY `Site` (`Site`),
  KEY `Owner` (`Owner`),
  KEY `OwnerGroup` (`OwnerGroup`),
  KEY `Status` (`Status`),
  KEY `MinorStatus` (`MinorStatus`),
  KEY `ApplicationStatus` (`ApplicationStatus`),
  KEY `StatusSite` (`Status`,`Site`),
  KEY `LastUpdateTime` (`LastUpdateTime`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ------------------------------------------------------------------------------
DROP TABLE IF EXISTS `InputData`;
CREATE TABLE `InputData` (
  `JobID` INT(11) UNSIGNED NOT NULL,
  `LFN` VARCHAR(255) NOT NULL DEFAULT '',
  `Status` VARCHAR(32) NOT NULL DEFAULT 'AprioriGood',
  PRIMARY KEY (`JobID`,`LFN`),
  FOREIGN KEY (`JobID`) REFERENCES `Jobs`(`JobID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ------------------------------------------------------------------------------
DROP TABLE IF EXISTS `JobParameters`;
CREATE TABLE `JobParameters` (
  `JobID` INT(11) UNSIGNED NOT NULL,
  `Name` VARCHAR(100) NOT NULL,
  `Value` TEXT NOT NULL,
  PRIMARY KEY (`JobID`,`Name`),
  FOREIGN KEY (`JobID`) REFERENCES `Jobs`(`JobID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ------------------------------------------------------------------------------
DROP TABLE IF EXISTS `OptimizerParameters`;
CREATE TABLE `OptimizerParameters` (
  `JobID` INT(11) UNSIGNED NOT NULL,
  `Name` VARCHAR(100) NOT NULL,
  `Value` MEDIUMTEXT NOT NULL,
  PRIMARY KEY (`JobID`,`Name`),
  FOREIGN KEY (`JobID`) REFERENCES `Jobs`(`JobID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ------------------------------------------------------------------------------
DROP TABLE IF EXISTS `AtticJobParameters`;
CREATE TABLE `AtticJobParameters` (
  `JobID` INT(11) UNSIGNED NOT NULL,
  `Name` VARCHAR(100) NOT NULL,
  `Value` TEXT NOT NULL,
  `RescheduleCycle` INT(11) UNSIGNED NOT NULL,
  PRIMARY KEY (`JobID`,`Name`,`RescheduleCycle`),
  FOREIGN KEY (`JobID`) REFERENCES `Jobs`(`JobID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ------------------------------------------------------------------------------
DROP TABLE IF EXISTS `HeartBeatLoggingInfo`;
CREATE TABLE `HeartBeatLoggingInfo` (
  `JobID` INT(11) UNSIGNED NOT NULL,
  `Name` VARCHAR(100) NOT NULL,
  `Value` TEXT NOT NULL,
  `HeartBeatTime` DATETIME NOT NULL,
  PRIMARY KEY (`JobID`,`Name`,`HeartBeatTime`),
  FOREIGN KEY (`JobID`) REFERENCES `Jobs`(`JobID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

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
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
