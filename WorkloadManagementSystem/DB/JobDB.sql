-- $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/WorkloadManagementSystem/DB/JobDB.sql,v 1.22 2009/08/26 09:39:53 rgracian Exp $

-- ------------------------------------------------------------------------------
--
--  Schema definition for the JobDB database - the main database of the DIRAC
--  Workload Management System
-- -
-- ------------------------------------------------------------------------------

-- DROP DATABASE IF EXISTS JobDB;

-- CREATE DATABASE JobDB;

-- ------------------------------------------------------------------------------
-- Database owner definition

-- USE mysql;
-- DELETE FROM user WHERE user='Dirac';

--
-- Must set passwords for database user by replacing "must_be_set".
--

-- GRANT SELECT,INSERT,LOCK TABLES,UPDATE,DELETE,CREATE,DROP,ALTER ON JobDB.* TO Dirac@localhost IDENTIFIED BY 'must_be_set';
-- GRANT SELECT,INSERT,LOCK TABLES,UPDATE,DELETE,CREATE,DROP,ALTER ON JobDB.* TO Dirac@'%' IDENTIFIED BY 'must_be_set';

-- FLUSH PRIVILEGES;

-- -----------------------------------------------------------------------------
USE JobDB;

-- ------------------------------------------------------------------------------
-- DROP TABLE IF EXISTS Jobs;
-- CREATE TABLE Jobs (
--    JobID INTEGER NOT NULL AUTO_INCREMENT,
--    JobType VARCHAR(32) NOT NULL DEFAULT 'normal',
--    INDEX (JobType),
--    DIRACSetup VARCHAR(32) NOT NULL,
--    INDEX (DIRACSetup),
--    JobGroup VARCHAR(32) NOT NULL DEFAULT 'NoGroup',
--    INDEX (JobGroup),
--    JobSplitType ENUM ('Single','Master','Subjob','DAGNode') NOT NULL DEFAULT 'Single',
--    INDEX (JobSplitType),
--    MasterJobID INTEGER NOT NULL DEFAULT 0,
--    Site VARCHAR(100) NOT NULL DEFAULT 'ANY',
--    INDEX (Site),
--    JobName VARCHAR(128) NOT NULL DEFAULT 'Unknown',
--    Owner VARCHAR(32) NOT NULL DEFAULT 'Unknown',
--    INDEX (Owner),
--    OwnerDN VARCHAR(255) NOT NULL DEFAULT 'Unknown',
--    INDEX (OwnerDN),
--    OwnerGroup varchar(128) NOT NULL DEFAULT 'lhcb_user',
--    INDEX (OwnerGroup),
--    SubmissionTime DATETIME,
--    RescheduleTime DATETIME,
--    LastUpdateTime DATETIME,
--    StartExecTime DATETIME,
--    HeartBeatTime DATETIME,
--    EndExecTime DATETIME,
--    Status VARCHAR(32) NOT NULL DEFAULT 'Received',
--    INDEX (Status),
--    INDEX (Status,Site),
--    MinorStatus VARCHAR(128) NOT NULL DEFAULT 'Initial insertion',
--    INDEX (MinorStatus),
--    ApplicationStatus VARCHAR(256) NOT NULL DEFAULT 'Unknown',
--    INDEX (ApplicationStatus),
--    ApplicationNumStatus INTEGER NOT NULL DEFAULT 0,
--    CPUTime FLOAT NOT NULL DEFAULT 0.0,
--    UserPriority INTEGER NOT NULL DEFAULT 0,
--    SystemPriority INTEGER NOT NULL DEFAULT 0,
--    RescheduleCounter INTEGER NOT NULL DEFAULT 0,
--    VerifiedFlag  ENUM ('True','False') NOT NULL DEFAULT 'False',
--    DeletedFlag  ENUM ('True','False') NOT NULL DEFAULT 'False',
--    KilledFlag ENUM ('True','False') NOT NULL DEFAULT 'False',
--    FailedFlag ENUM ('True','False') NOT NULL DEFAULT 'False',
--    ISandboxReadyFlag ENUM ('True','False') NOT NULL DEFAULT 'False',
--    OSandboxReadyFlag ENUM ('True','False') NOT NULL DEFAULT 'False',
--    RetrievedFlag ENUM ('True','False') NOT NULL DEFAULT 'False',
--    AccountedFlag ENUM ('True','False','Failed') NOT NULL DEFAULT 'False',
--    PRIMARY KEY (JobID)
--) ENGINE = InnoDB;

-- ------------------------------------------------------------------------------
--DROP TABLE IF EXISTS JobJDLs;
--CREATE TABLE JobJDLs (
--    JobID INTEGER NOT NULL AUTO_INCREMENT,
--    JDL BLOB NOT NULL DEFAULT '',
--    JobRequirements BLOB NOT NULL DEFAULT '',
--    OriginalJDL BLOB NOT NULL DEFAULT '',
--    PRIMARY KEY (JobID)
--);

-- ------------------------------------------------------------------------------
--DROP TABLE IF EXISTS SubJobs;
--CREATE TABLE SubJobs (
--    JobID INTEGER NOT NULL,
--    SubJobID INTEGER NOT NULL
--);

-- ------------------------------------------------------------------------------
--DROP TABLE IF EXISTS PrecursorJobs;
--CREATE TABLE PrecursorJobs (
--    JobID INTEGER NOT NULL,
--    PreJobID INTEGER NOT NULL
--);

-- ------------------------------------------------------------------------------
--DROP TABLE IF EXISTS InputData;
--CREATE TABLE InputData (
--    JobID INTEGER NOT NULL,
--    Status VARCHAR(32) NOT NULL DEFAULT 'AprioriGood',
--    LFN VARCHAR(255),
--    PRIMARY KEY(JobID, LFN)
--);

-- ------------------------------------------------------------------------------
--DROP TABLE IF EXISTS JobParameters;
--CREATE TABLE JobParameters (
--    JobID INTEGER NOT NULL,
--    Name VARCHAR(100) NOT NULL,
--    Value BLOB NOT NULL,
--    PRIMARY KEY(JobID, Name)
--) ENGINE = InnoDB;

-- ------------------------------------------------------------------------------
--DROP TABLE IF EXISTS OptimizerParameters;
--CREATE TABLE OptimizerParameters (
--    JobID INTEGER NOT NULL,
--    Name VARCHAR(100) NOT NULL,
--    Value MEDIUMBLOB NOT NULL,
--    PRIMARY KEY(JobID, Name)
--) ENGINE = InnoDB;

-- ------------------------------------------------------------------------------
--DROP TABLE IF EXISTS AtticJobParameters;
--CREATE TABLE AtticJobParameters (
--    JobID INTEGER NOT NULL,
--    RescheduleCycle INTEGER NOT NULL,
--    Name VARCHAR(100) NOT NULL,
--    Value BLOB NOT NULL,
--    PRIMARY KEY(JobID, Name, RescheduleCycle)
--);

-- ------------------------------------------------------------------------------
--DROP TABLE IF EXISTS TaskQueues;
--CREATE TABLE TaskQueues (
--    TaskQueueID INTEGER NOT NULL AUTO_INCREMENT,
--    Priority INTEGER NOT NULL DEFAULT 0,
--    Requirements BLOB NOT NULL,
--    NumberOfJobs INTEGER NOT NULL DEFAULT 0,
--    PRIMARY KEY (TaskQueueID)
--);

-- ------------------------------------------------------------------------------
--DROP TABLE IF EXISTS TaskQueue;
--CREATE TABLE TaskQueue (
--    TaskQueueID INTEGER NOT NULL,
--    JobID       INTEGER NOT NULL,
--    Rank        INTEGER NOT NULL DEFAULT 0,
--    PRIMARY KEY (JobID, TaskQueueID)
--);

-- ------------------------------------------------------------------------------
--DROP TABLE IF EXISTS SiteMask;
--CREATE TABLE SiteMask (
--    Site   VARCHAR(64) NOT NULL,
--    Status VARCHAR(64) NOT NULL,
--    LastUpdateTime DATETIME NOT NULL,
--    Author VARCHAR(255) NOT NULL,
--    Comment BLOB NOT NULL,
--    PRIMARY KEY (Site)
--);

--DROP TABLE IF EXISTS SiteMaskLogging;
--CREATE TABLE SiteMaskLogging (
--    Site   VARCHAR(64) NOT NULL,
--    Status VARCHAR(64) NOT NULL,
--    UpdateTime DATETIME NOT NULL,
--    Author VARCHAR(255) NOT NULL,
--    Comment BLOB NOT NULL
--);

-- ------------------------------------------------------------------------------
--DROP TABLE IF EXISTS HeartBeatLoggingInfo;
--CREATE TABLE HeartBeatLoggingInfo (
--    JobID INTEGER NOT NULL,
--    Name VARCHAR(100) NOT NULL,
--    Value BLOB NOT NULL,
--    HeartBeatTime DATETIME NOT NULL,
--    INDEX (JobID)
--)ENGINE = InnoDB;

-- ------------------------------------------------------------------------------
--DROP TABLE IF EXISTS JobCommands;
--CREATE TABLE JobCommands (
--    JobID INTEGER NOT NULL,
--    Command VARCHAR(100) NOT NULL,
--    Arguments VARCHAR(100) NOT NULL,
--    Status VARCHAR(64) NOT NULL DEFAULT 'Received',
--    ReceptionTime DATETIME NOT NULL,
--    ExecutionTime DATETIME,
--    INDEX (JobID)
--);
