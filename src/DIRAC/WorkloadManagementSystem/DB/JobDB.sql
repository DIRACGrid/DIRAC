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
  `VO` VARCHAR(64) NOT NULL DEFAULT 'Unknown',
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
  `Status` VARCHAR(32) NOT NULL DEFAULT 'Received',
  `ReceptionTime` DATETIME NOT NULL,
  `ExecutionTime` DATETIME DEFAULT NULL,
  PRIMARY KEY (`JobID`,`Arguments`,`ReceptionTime`),
  FOREIGN KEY (`JobID`) REFERENCES `Jobs`(`JobID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


-- ------------------------------------------------------------------------------
-- summary table and triggers
-- ------------------------------------------------------------------------------

-- summary for JobsHistory

DROP TABLE IF EXISTS `JobsHistorySummary`;
CREATE TABLE `JobsHistorySummary` (
  `ID` INT AUTO_INCREMENT PRIMARY KEY,
  `Status` VARCHAR(32),
  `Site` VARCHAR(100),
  `Owner` VARCHAR(32),
  `OwnerGroup` VARCHAR(128),
  `VO` VARCHAR(64),
  `JobGroup` VARCHAR(32),
  `JobType` VARCHAR(32),
  `ApplicationStatus` VARCHAR(255),
  `MinorStatus` VARCHAR(128),
  `JobCount` INT,
  RescheduleSum INT,
  UNIQUE KEY uq_summary (
    `Status`,
    `Site`,
    `Owner`,
    `OwnerGroup`(32),
    `VO`,
    `JobGroup`,
    `JobType`,
    `ApplicationStatus`(128),
    `MinorStatus`
  )
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


-- now the triggers

DELIMITER //

CREATE TRIGGER trg_Jobs_insert
AFTER INSERT ON Jobs
FOR EACH ROW
BEGIN
  INSERT INTO JobsHistorySummary (Status, Site, Owner, OwnerGroup, VO, JobGroup, JobType, ApplicationStatus, MinorStatus, JobCount, RescheduleSum)
  VALUES (NEW.Status, NEW.Site, NEW.Owner, NEW.OwnerGroup, NEW.VO, NEW.JobGroup, NEW.JobType, NEW.ApplicationStatus, NEW.MinorStatus, 1, NEW.RescheduleCounter)
  ON DUPLICATE KEY UPDATE JobCount = JobCount + 1, RescheduleSum = RescheduleSum + NEW.RescheduleCounter;
END;
//

CREATE TRIGGER trg_Jobs_delete
AFTER DELETE ON Jobs
FOR EACH ROW
BEGIN
  UPDATE JobsHistorySummary
  SET JobCount = JobCount - 1, RescheduleSum = RescheduleSum - OLD.RescheduleCounter
  WHERE Status = OLD.Status
    AND Site = OLD.Site
    AND Owner = OLD.Owner
    AND OwnerGroup = OLD.OwnerGroup
    AND VO = OLD.VO
    AND JobGroup = OLD.JobGroup
    AND JobType = OLD.JobType
    AND ApplicationStatus = OLD.ApplicationStatus
    AND MinorStatus = OLD.MinorStatus;

  -- Remove zero rows
  DELETE FROM JobsHistorySummary
  WHERE JobCount = 0
    AND Status = OLD.Status
    AND Site = OLD.Site
    AND Owner = OLD.Owner
    AND OwnerGroup = OLD.OwnerGroup
    AND VO = OLD.VO
    AND JobGroup = OLD.JobGroup
    AND JobType = OLD.JobType
    AND ApplicationStatus = OLD.ApplicationStatus
    AND MinorStatus = OLD.MinorStatus;
END;
//

CREATE TRIGGER trg_Jobs_update_status
AFTER UPDATE ON Jobs
FOR EACH ROW
BEGIN
  IF OLD.Status != NEW.Status THEN

    -- Decrease count from old status
    UPDATE JobsHistorySummary
    SET JobCount = JobCount - 1, RescheduleSum = RescheduleSum - OLD.RescheduleCounter
    WHERE Status = OLD.Status
      AND Site = OLD.Site
      AND Owner = OLD.Owner
      AND OwnerGroup = OLD.OwnerGroup
      AND VO = OLD.VO
      AND JobGroup = OLD.JobGroup
      AND JobType = OLD.JobType
      AND ApplicationStatus = OLD.ApplicationStatus
      AND MinorStatus = OLD.MinorStatus;

    -- Delete row if count drops to zero
    DELETE FROM JobsHistorySummary WHERE JobCount = 0;

    -- Increase count for new status
    INSERT INTO JobsHistorySummary (Status, Site, Owner, OwnerGroup, JobGroup, VO, JobType, ApplicationStatus, MinorStatus, JobCount, RescheduleSum)
    VALUES (NEW.Status, NEW.Site, NEW.Owner, NEW.OwnerGroup, NEW.JobGroup, NEW.VO, NEW.JobType, NEW.ApplicationStatus, NEW.MinorStatus, 1, NEW.RescheduleCounter)
    ON DUPLICATE KEY UPDATE JobCount = JobCount + 1, RescheduleSum = RescheduleSum + NEW.RescheduleCounter;

  END IF;
END;
//

DELIMITER ;
