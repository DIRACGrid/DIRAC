-- $Header $

--------------------------------------------------------------------------------
--
--  Schema definition for the PilotAgentsDB database - containing the job status
--  history ( logging ) information
---
--------------------------------------------------------------------------------

DROP DATABASE IF EXISTS PilotAgentsDB;

CREATE DATABASE PilotAgentsDB;

--------------------------------------------------------------------------------
-- Database owner definition

USE mysql;
DELETE FROM user WHERE user='Dirac';

--
-- Must set passwords for database user by replacing "must_be_set".
--

GRANT SELECT,INSERT,LOCK TABLES,UPDATE,DELETE,CREATE,DROP,ALTER ON PilotAgentsDB.* TO Dirac@localhost IDENTIFIED BY 'lhcbMySQL';

FLUSH PRIVILEGES;

-------------------------------------------------------------------------------
USE PilotAgentsDB;

--------------------------------------------------------------------------------
DROP TABLE IF EXISTS PilotAgents;
CREATE TABLE PilotAgents (
    PilotID INTEGER NOT NULL AUTO_INCREMENT,
    InitialJobID INTEGER NOT NULL DEFAULT 0,
    PilotJobReference VARCHAR(255) NOT NULL DEFAULT 'Unknown',
    DestinationSite VARCHAR(128) NOT NULL DEFAULT 'Multiple',
    OwnerDN VARCHAR(255) NOT NULL,
    OwnerGroup VARCHAR(128) NOT NULL,
    GridType VARCHAR(32) NOT NULL DEFAULT 'LCG',
    SubmissionTime DATETIME,
    LastUpdateTime DATETIME,
    Status VARCHAR(32) NOT NULL DEFAULT 'Unknown',
    StdOutput BLOB,
    StdError BLOB,
    PRIMARY KEY (PilotID),
    INDEX (PilotJobReference)
);

DROP TABLE IF EXISTS JobToPilotMapping;
CREATE TABLE JobToPilotMapping (
    PilotID INTEGER NOT NULL,
    JobID INTEGER NOT NULL,
    StartTime DATETIME NOT NULL
);
