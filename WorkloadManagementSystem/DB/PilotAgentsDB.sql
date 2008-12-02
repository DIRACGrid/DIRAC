-- $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/WorkloadManagementSystem/DB/PilotAgentsDB.sql,v 1.14 2008/12/02 14:06:32 atsareg Exp $

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

GRANT SELECT,INSERT,LOCK TABLES,UPDATE,DELETE,CREATE,DROP,ALTER ON PilotAgentsDB.* TO Dirac@localhost IDENTIFIED BY 'must_be_set';
GRANT SELECT,INSERT,LOCK TABLES,UPDATE,DELETE,CREATE,DROP,ALTER ON PilotAgentsDB.* TO Dirac@'%' IDENTIFIED BY 'must_be_set';

FLUSH PRIVILEGES;

-------------------------------------------------------------------------------
USE PilotAgentsDB;

--------------------------------------------------------------------------------
DROP TABLE IF EXISTS PilotAgents;
CREATE TABLE PilotAgents (
    PilotID INTEGER NOT NULL AUTO_INCREMENT,
    InitialJobID INTEGER NOT NULL DEFAULT 0,
    CurrentJobID INTEGER NOT NULL DEFAULT 0,
    TaskQueueID INTEGER NOT NULL DEFAULT '0',
    PilotJobReference VARCHAR(255) NOT NULL DEFAULT 'Unknown',
    DestinationSite VARCHAR(128) NOT NULL DEFAULT 'Multiple',
    Broker VARCHAR(128) NOT NULL DEFAULT 'Unknown',
    OwnerDN VARCHAR(255) NOT NULL,
    OwnerGroup VARCHAR(128) NOT NULL,
    GridType VARCHAR(32) NOT NULL DEFAULT 'LCG',
    GridRequirements BLOB,
    BenchMark DOUBLE NOT NULL DEFAULT 0.0,
    SubmissionTime DATETIME,
    LastUpdateTime DATETIME,
    Status VARCHAR(32) NOT NULL DEFAULT 'Unknown',
    ParentID INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (PilotID),
    INDEX (PilotJobReference)
) ENGINE = InnoDB;

DROP TABLE IF EXISTS JobToPilotMapping;
CREATE TABLE JobToPilotMapping (
    PilotID INTEGER NOT NULL,
    JobID INTEGER NOT NULL,
    StartTime DATETIME NOT NULL
) ENGINE = InnoDB;

DROP TABLE IF EXISTS PilotOutput;
CREATE TABLE PilotOutput (
    PilotID INTEGER NOT NULL,
    StdOutput MEDIUMBLOB,
    StdError MEDIUMBLOB
);

DROP TABLE IF EXISTS PilotRequirements;
CREATE TABLE PilotRequirements (
    PilotID INTEGER NOT NULL,
    Requirements BLOB
);
