-- $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/WorkloadManagementSystem/DB/JobDB.sql,v 1.5 2008/01/11 15:25:52 atsareg Exp $

--------------------------------------------------------------------------------
--
--  Schema definition for the JobDB database - the main database of the DIRAC
--  Workload Management System
---
--------------------------------------------------------------------------------

DROP DATABASE IF EXISTS JobDB;

CREATE DATABASE JobDB;

--------------------------------------------------------------------------------
-- Database owner definition

USE mysql;
DELETE FROM user WHERE user='Dirac';

--
-- Must set passwords for database user by replacing "must_be_set".
--

GRANT SELECT,INSERT,LOCK TABLES,UPDATE,DELETE,CREATE,DROP,ALTER ON JobDB.* TO Dirac@localhost IDENTIFIED BY 'lhcbMySQL';
GRANT SELECT,INSERT,LOCK TABLES,UPDATE,DELETE,CREATE,DROP,ALTER ON JobDB.* TO Dirac@volhcb03.cern.ch IDENTIFIED BY 'lhcbMySQL';
GRANT SELECT,INSERT,LOCK TABLES,UPDATE,DELETE,CREATE,DROP,ALTER ON JobDB.* TO Dirac@'%' IDENTIFIED BY 'lhcbMySQL';

FLUSH PRIVILEGES;

-------------------------------------------------------------------------------
USE JobDB;

--------------------------------------------------------------------------------
DROP TABLE IF EXISTS Jobs;
CREATE TABLE Jobs (
    JobID INTEGER NOT NULL AUTO_INCREMENT,
    JobType VARCHAR(32) NOT NULL DEFAULT 'user',
    DIRACSetup VARCHAR(32) NOT NULL DEFAULT 'test',
    JobGroup VARCHAR(32) NOT NULL DEFAULT '00000000',
    JobSplitType ENUM ('Single','Master','Subjob','DAGNode') NOT NULL DEFAULT 'Single',
    MasterJobID INTEGER NOT NULL DEFAULT 0,
    Site VARCHAR(100),
    JobName VARCHAR(128) NOT NULL DEFAULT 'noname',
    Owner VARCHAR(32) NOT NULL DEFAULT 'unknown',
    OwnerDN VARCHAR(255) NOT NULL DEFAULT 'unknown',
    OwnerGroup varchar(128) NOT NULL DEFAULT '/lhcb',
    SubmissionTime DATETIME,
    RescheduleTime DATETIME,
    LastUpdateTime DATETIME,
    StartExecTime DATETIME,
    HeartBeatTime DATETIME,
    Status VARCHAR(32) NOT NULL DEFAULT 'received',
    MinorStatus VARCHAR(128) NOT NULL DEFAULT 'unknown',
    ApplicationStatus VARCHAR(256) NOT NULL DEFAULT 'unknown',
    ApplicationNumStatus INTEGER NOT NULL DEFAULT 0,
    CPUTime FLOAT NOT NULL DEFAULT 0.0,
    UserPriority INTEGER NOT NULL DEFAULT 0,
    SystemPriority INTEGER NOT NULL DEFAULT 0,
    RescheduleCounter INTEGER NOT NULL DEFAULT 0,
    VerifiedFlag  ENUM ('True','False') NOT NULL DEFAULT 'False',
    DeletedFlag  ENUM ('True','False') NOT NULL DEFAULT 'False',
    KilledFlag ENUM ('True','False') NOT NULL DEFAULT 'False',
    FailedFlag ENUM ('True','False') NOT NULL DEFAULT 'False',
    ISandboxReadyFlag ENUM ('True','False') NOT NULL DEFAULT 'False',
    OSandboxReadyFlag ENUM ('True','False') NOT NULL DEFAULT 'False',
    RetrievedFlag ENUM ('True','False') NOT NULL DEFAULT 'False',
    AccountedFlag ENUM ('True','False','Failed') NOT NULL DEFAULT 'False',
    PRIMARY KEY (JobID)
);

--------------------------------------------------------------------------------
DROP TABLE IF EXISTS JobJDLs;
CREATE TABLE JobJDLs (
    JobID INTEGER NOT NULL,
    JDL BLOB NOT NULL DEFAULT '',
    JobRequirements BLOB,
    OriginalJDL BLOB,
    PRIMARY KEY (JobID)
);

--------------------------------------------------------------------------------
DROP TABLE IF EXISTS SubJobs;
CREATE TABLE SubJobs (
    JobID INTEGER NOT NULL,
    SubJobID INTEGER NOT NULL
);

--------------------------------------------------------------------------------
DROP TABLE IF EXISTS PrecursorJobs;
CREATE TABLE PrecursorJobs (
    JobID INTEGER NOT NULL,
    PreJobID INTEGER NOT NULL
);

--------------------------------------------------------------------------------
DROP TABLE IF EXISTS InputData;
CREATE TABLE InputData (
    JobID INTEGER NOT NULL,
    Status VARCHAR(32) NOT NULL DEFAULT 'AprioriGood',
    LFN VARCHAR(255),
    PRIMARY KEY(JobID, LFN)
);

--------------------------------------------------------------------------------
DROP TABLE IF EXISTS JobParameters;
CREATE TABLE JobParameters (
    JobID INTEGER NOT NULL,
    Name VARCHAR(100) NOT NULL,
    Value BLOB NOT NULL,
    PRIMARY KEY(JobID, Name)
);

--------------------------------------------------------------------------------
DROP TABLE IF EXISTS OptimizerParameters;
CREATE TABLE OptimizerParameters (
    JobID INTEGER NOT NULL,
    Name VARCHAR(100) NOT NULL,
    Value BLOB NOT NULL,
    PRIMARY KEY(JobID, Name)
);

--------------------------------------------------------------------------------
DROP TABLE IF EXISTS AtticJobParameters;
CREATE TABLE AtticJobParameters (
    JobID INTEGER NOT NULL,
    RescheduleCycle INTEGER NOT NULL,
    Name VARCHAR(100) NOT NULL,
    Value BLOB NOT NULL,
    PRIMARY KEY(JobID, Name)
);

--------------------------------------------------------------------------------
DROP TABLE IF EXISTS TaskQueues;
CREATE TABLE TaskQueues (
    TaskQueueID INTEGER NOT NULL AUTO_INCREMENT,
    Priority INTEGER NOT NULL DEFAULT 0,
    Requirements BLOB NOT NULL,
    PRIMARY KEY (TaskQueueID)
);

--------------------------------------------------------------------------------
DROP TABLE IF EXISTS TaskQueue;
CREATE TABLE TaskQueue (
    TaskQueueID INTEGER NOT NULL,
    JobID       INTEGER NOT NULL,
    Rank        INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (JobID, TaskQueueID)
);

--------------------------------------------------------------------------------
DROP TABLE IF EXISTS SiteMask;
CREATE TABLE SiteMask (
    Site   VARCHAR(64) NOT NULL,
    Status VARCHAR(64) NOT NULL,
    LastUpdateTime DATETIME NOT NULL,
    Author VARCHAR(255) NOT NULL,
    PRIMARY KEY (Site)
);
