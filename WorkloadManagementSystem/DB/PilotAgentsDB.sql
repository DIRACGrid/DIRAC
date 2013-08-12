-- $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/WorkloadManagementSystem/DB/PilotAgentsDB.sql,v 1.20 2009/08/26 09:39:53 rgracian Exp $

-- ------------------------------------------------------------------------------
--
--  Schema definition for the PilotAgentsDB database - containing the job status
--  history ( logging ) information
-- -
-- ------------------------------------------------------------------------------

-- DROP DATABASE IF EXISTS PilotAgentsDB;

-- CREATE DATABASE PilotAgentsDB;

-- ------------------------------------------------------------------------------
-- Database owner definition

-- USE mysql;
-- DELETE FROM user WHERE user='Dirac';

--
-- Must set passwords for database user by replacing "must_be_set".
--

-- GRANT SELECT,INSERT,LOCK TABLES,UPDATE,DELETE,CREATE,DROP,ALTER ON PilotAgentsDB.* TO Dirac@localhost IDENTIFIED BY 'must_be_set';
-- GRANT SELECT,INSERT,LOCK TABLES,UPDATE,DELETE,CREATE,DROP,ALTER ON PilotAgentsDB.* TO Dirac@'%' IDENTIFIED BY 'must_be_set';

-- FLUSH PRIVILEGES;

-- -----------------------------------------------------------------------------
USE PilotAgentsDB;

-- ------------------------------------------------------------------------------
-- DROP TABLE IF EXISTS PilotAgents;
-- CREATE TABLE PilotAgents (
--    PilotID INTEGER NOT NULL AUTO_INCREMENT,
--    InitialJobID INTEGER NOT NULL DEFAULT 0,
--    CurrentJobID INTEGER NOT NULL DEFAULT 0,
--    TaskQueueID INTEGER NOT NULL DEFAULT '0',
--    PilotJobReference VARCHAR(255) NOT NULL DEFAULT 'Unknown',
--    PilotStamp VARCHAR(32) NOT NULL DEFAULT '',
--    DestinationSite VARCHAR(128) NOT NULL DEFAULT 'NotAssigned',
--    Queue VARCHAR(128) NOT NULL DEFAULT 'Unknown',
--    GridSite VARCHAR(128) NOT NULL DEFAULT 'Unknown',
--    Broker VARCHAR(128) NOT NULL DEFAULT 'Unknown',
--    OwnerDN VARCHAR(255) NOT NULL,
--    OwnerGroup VARCHAR(128) NOT NULL,
--    GridType VARCHAR(32) NOT NULL DEFAULT 'LCG',
--    BenchMark DOUBLE NOT NULL DEFAULT 0.0,
--    SubmissionTime DATETIME,
--    LastUpdateTime DATETIME,
--    Status VARCHAR(32) NOT NULL DEFAULT 'Unknown',
--    StatusReason VARCHAR(255) NOT NULL DEFAULT 'Unknown',
--    ParentID INTEGER NOT NULL DEFAULT 0,
--    OutputReady ENUM ('True','False') NOT NULL DEFAULT 'False',
--    AccountingSent ENUM ('True','False') NOT NULL DEFAULT 'False',
--    PRIMARY KEY (PilotID),
--    INDEX (PilotJobReference),
--    INDEX (Status)
--) ENGINE = InnoDB;

-- DROP TABLE IF EXISTS JobToPilotMapping;
-- CREATE TABLE JobToPilotMapping (
--     PilotID INTEGER NOT NULL,
--     JobID INTEGER NOT NULL,
--     StartTime DATETIME NOT NULL,
--     INDEX (PilotID),
--     INDEX (JobID)
-- ) ENGINE = InnoDB;

-- DROP TABLE IF EXISTS PilotOutput;
-- CREATE TABLE PilotOutput (
--    PilotID INTEGER NOT NULL,
--     StdOutput MEDIUMBLOB,
--     StdError MEDIUMBLOB,
--     PRIMARY KEY (PilotID)
-- ) ENGINE = InnoDB;

-- DROP TABLE IF EXISTS PilotRequirements;
-- CREATE TABLE PilotRequirements (
--     PilotID INTEGER NOT NULL,
--     Requirements BLOB,
--    PRIMARY KEY (PilotID)
-- );
