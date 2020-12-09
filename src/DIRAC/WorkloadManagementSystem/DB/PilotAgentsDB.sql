-- $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/WorkloadManagementSystem/DB/PilotAgentsDB.sql,v 1.20 2009/08/26 09:39:53 rgracian Exp $

-- ------------------------------------------------------------------------------
--
--  Schema definition for the PilotAgentsDB database - containing the Pilots status
--  history ( logging ) information
--
-- ------------------------------------------------------------------------------

-- When installing via dirac tools, the following is not needed (still here for reference)
-- 
-- DROP DATABASE IF EXISTS PilotAgentsDB;
-- CREATE DATABASE PilotAgentsDB;
-- ------------------------------------------------------------------------------
-- Database owner definition
-- USE mysql;
--
-- Must set passwords for database user by replacing "must_be_set".
--
-- GRANT SELECT,INSERT,LOCK TABLES,UPDATE,DELETE,CREATE,DROP,ALTER ON PilotAgentsDB.* TO Dirac@localhost IDENTIFIED BY 'must_be_set';
-- GRANT SELECT,INSERT,LOCK TABLES,UPDATE,DELETE,CREATE,DROP,ALTER ON PilotAgentsDB.* TO Dirac@'%' IDENTIFIED BY 'must_be_set';
-- FLUSH PRIVILEGES;

-- -----------------------------------------------------------------------------
USE PilotAgentsDB;

-- ------------------------------------------------------------------------------
DROP TABLE IF EXISTS `PilotAgents`;
CREATE TABLE `PilotAgents` (
  `PilotID` INT(11) UNSIGNED NOT NULL AUTO_INCREMENT,
  `InitialJobID` INT(11) UNSIGNED NOT NULL DEFAULT 0,
  `CurrentJobID` INT(11) UNSIGNED NOT NULL DEFAULT 0,
  `TaskQueueID` INT(11) UNSIGNED NOT NULL DEFAULT 0,
  `PilotJobReference` VARCHAR(255) NOT NULL DEFAULT 'Unknown',
  `PilotStamp` VARCHAR(32) NOT NULL DEFAULT '',
  `DestinationSite` VARCHAR(128) NOT NULL DEFAULT 'NotAssigned',
  `Queue` VARCHAR(128) NOT NULL DEFAULT 'Unknown',
  `GridSite` VARCHAR(128) NOT NULL DEFAULT 'Unknown',
  `Broker` VARCHAR(128) NOT NULL DEFAULT 'Unknown',
  `OwnerDN` VARCHAR(255) NOT NULL,
  `OwnerGroup` VARCHAR(128) NOT NULL,
  `GridType` VARCHAR(32) NOT NULL DEFAULT 'LCG',
  `GridRequirements` blob,
  `BenchMark` DOUBLE NOT NULL DEFAULT 0.0,
  `SubmissionTime` DATETIME DEFAULT NULL,
  `LastUpdateTime` DATETIME DEFAULT NULL,
  `Status` VARCHAR(32) NOT NULL DEFAULT 'Unknown',
  `StatusReason` VARCHAR(255) NOT NULL DEFAULT 'Unknown',
  `ParentID` INT(11) UNSIGNED NOT NULL DEFAULT 0,
  `OutputReady` ENUM('True','False') NOT NULL DEFAULT 'False',
  `AccountingSent` ENUM('True','False') NOT NULL DEFAULT 'False',
  PRIMARY KEY (`PilotID`),
  KEY `PilotJobReference` (`PilotJobReference`),
  KEY `Status` (`Status`),
  KEY `Statuskey` (`GridSite`,`DestinationSite`,`Status`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;


DROP TABLE IF EXISTS `JobToPilotMapping`;
CREATE TABLE `JobToPilotMapping` (
  `PilotID` INT(11) UNSIGNED NOT NULL,
  `JobID` INT(11) UNSIGNED NOT NULL,
  `StartTime` DATETIME NOT NULL,
  KEY `JobID` (`JobID`),
  KEY `PilotID` (`PilotID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

DROP TABLE IF EXISTS `PilotOutput`;
CREATE TABLE `PilotOutput` (
  `PilotID` INT(11) UNSIGNED NOT NULL,
  `StdOutput` MEDIUMBLOB,
  `StdError` MEDIUMBLOB,
  PRIMARY KEY (`PilotID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

