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
  `PilotJobReference` VARCHAR(255) NOT NULL DEFAULT 'Unknown',
  `PilotStamp` VARCHAR(32) NOT NULL DEFAULT '',
  `DestinationSite` VARCHAR(128) NOT NULL DEFAULT 'NotAssigned',
  `Queue` VARCHAR(128) NOT NULL DEFAULT 'Unknown',
  `GridSite` VARCHAR(128) NOT NULL DEFAULT 'Unknown',
  `VO` VARCHAR(64) NOT NULL,
  `GridType` VARCHAR(32) NOT NULL DEFAULT 'LCG',
  `BenchMark` DOUBLE NOT NULL DEFAULT 0.0,
  `SubmissionTime` DATETIME DEFAULT NULL,
  `LastUpdateTime` DATETIME DEFAULT NULL,
  `Status` VARCHAR(32) NOT NULL DEFAULT 'Unknown',
  `StatusReason` VARCHAR(255) NOT NULL DEFAULT 'Unknown',
  `AccountingSent` ENUM('True','False') NOT NULL DEFAULT 'False',
  PRIMARY KEY (`PilotID`),
  KEY `PilotJobReference` (`PilotJobReference`),
  KEY `Status` (`Status`),
  KEY `Statuskey` (`GridSite`,`DestinationSite`,`Status`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

DROP TABLE IF EXISTS `JobToPilotMapping`;
CREATE TABLE `JobToPilotMapping` (
  `PilotID` INT(11) UNSIGNED NOT NULL,
  `JobID` INT(11) UNSIGNED NOT NULL,
  `StartTime` DATETIME NOT NULL,
  KEY `JobID` (`JobID`),
  KEY `PilotID` (`PilotID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

DROP TABLE IF EXISTS `PilotOutput`;
CREATE TABLE `PilotOutput` (
  `PilotID` INT(11) UNSIGNED NOT NULL,
  `StdOutput` MEDIUMTEXT,
  `StdError` MEDIUMTEXT,
  PRIMARY KEY (`PilotID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


-- ------------------------------------------------------------------------------
-- summary table and triggers
-- ------------------------------------------------------------------------------

-- summary for PilotsHistory

DROP TABLE IF EXISTS `PilotsHistorySummary`;
CREATE TABLE `PilotsHistorySummary` (
  `GridSite` VARCHAR(128),
  `DestinationSite` VARCHAR(128),
  `Status` VARCHAR(32),
  `VO` VARCHAR(64),
  `PilotCount` INT,
  PRIMARY KEY (`GridSite`,`DestinationSite`,`Status`, `VO`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


-- now the triggers

DELIMITER //

CREATE TRIGGER trg_PilotAgents_insert
AFTER INSERT ON PilotAgents
FOR EACH ROW
BEGIN
  INSERT INTO PilotsHistorySummary (GridSite, DestinationSite, Status, VO, PilotCount)
  VALUES (NEW.GridSite, NEW.DestinationSite, NEW.Status, NEW.VO, 1)
  ON DUPLICATE KEY UPDATE PilotCount = PilotCount + 1;
END;
//

CREATE TRIGGER trg_PilotAgents_delete
AFTER DELETE ON PilotAgents
FOR EACH ROW
BEGIN
  UPDATE PilotsHistorySummary
  SET PilotCount = PilotCount - 1
  WHERE GridSite = OLD.GridSite
    AND DestinationSite = OLD.DestinationSite
    AND Status = OLD.Status
    AND VO = OLD.VO;

  -- Remove zero rows
  DELETE FROM PilotsHistorySummary
  WHERE PilotCount = 0
    AND GridSite = OLD.GridSite
    AND DestinationSite = OLD.DestinationSite
    AND Status = OLD.Status
    AND VO = OLD.VO;
END;
//

CREATE TRIGGER trg_PilotAgents_update_status
AFTER UPDATE ON PilotAgents
FOR EACH ROW
BEGIN
  IF OLD.Status != NEW.Status THEN

    -- Decrease count from old status
    UPDATE PilotsHistorySummary
    SET PilotCount = PilotCount - 1
    WHERE GridSite = OLD.GridSite
      AND DestinationSite = OLD.DestinationSite
      AND Status = OLD.Status
      AND VO = OLD.VO;

    -- Delete row if count drops to zero
    DELETE FROM PilotsHistorySummary WHERE PilotCount = 0;

    -- Increase count for new status
    INSERT INTO PilotsHistorySummary (GridSite, DestinationSite, Status, VO, PilotCount)
    VALUES (NEW.GridSite, NEW.DestinationSite, NEW.Status, NEW.VO, 1)
    ON DUPLICATE KEY UPDATE PilotCount = PilotCount + 1;

  END IF;
END;
//

DELIMITER ;
