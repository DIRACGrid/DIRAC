-- -------------------------------------------------------------------------------
--  Schema definition for the TransformationDB database a generic
--  engine to define input data streams and support dynamic data 
--  grouping per unit of execution.

DROP DATABASE IF EXISTS TransformationDB;
CREATE DATABASE TransformationDB;
-- ------------------------------------------------------------------------------

-- Database owner definition
USE mysql;

-- Must set passwords for database user by replacing "must_be_set".
GRANT SELECT,INSERT,LOCK TABLES,UPDATE,DELETE,CREATE,DROP,ALTER ON TransformationDB.* TO Dirac@'%' IDENTIFIED BY 'must_be_set';
FLUSH PRIVILEGES;

-- -----------------------------------------------------------------------------
USE TransformationDB;

-- -------------------------------------------------------------------------------
DROP TABLE IF EXISTS Transformations;
CREATE TABLE Transformations (
    TransformationID INTEGER NOT NULL AUTO_INCREMENT,
    TransformationName VARCHAR(255) NOT NULL,
    Description VARCHAR(255),
    LongDescription  BLOB,
    CreationDate DATETIME,
    LastUpdate DATETIME,
    AuthorDN VARCHAR(255) NOT NULL,
    AuthorGroup VARCHAR(255) NOT NULL,
    Type CHAR(32) DEFAULT 'Simulation',
    Plugin CHAR(32) DEFAULT 'None',
    AgentType CHAR(32) DEFAULT 'Manual',
    Status  CHAR(32) DEFAULT 'New',
    FileMask VARCHAR(255),
    TransformationGroup varchar(64) NOT NULL default 'General',
    TransformationFamily varchar(64) default '0',
    GroupSize INT NOT NULL DEFAULT 1,
    InheritedFrom INTEGER DEFAULT 0,
    Body LONGBLOB,
    MaxNumberOfTasks INT NOT NULL DEFAULT 0,
    EventsPerTask INT NOT NULL DEFAULT 0,
    PRIMARY KEY(TransformationID),
    INDEX(TransformationName)
) ENGINE=InnoDB;

-- -------------------------------------------------------------------------------
DROP TABLE IF EXISTS AdditionalParameters;
CREATE TABLE AdditionalParameters (
    TransformationID INTEGER NOT NULL,
    ParameterName VARCHAR(32) NOT NULL,
    ParameterValue LONGBLOB NOT NULL,
    ParameterType VARCHAR(32) DEFAULT 'StringType', 
    PRIMARY KEY(TransformationID,ParameterName)
);

-- -------------------------------------------------------------------------------
DROP TABLE IF EXISTS TransformationLog;
CREATE TABLE TransformationLog (
    TransformationID INTEGER NOT NULL,
    Message VARCHAR(255) NOT NULL,
    Author VARCHAR(255) NOT NULL DEFAULT "Unknown",
    MessageDate DATETIME NOT NULL,
    INDEX (TransformationID),
    INDEX (MessageDate)
);

-- -------------------------------------------------------------------------------
DROP TABLE IF EXISTS TransformationFiles;
CREATE TABLE TransformationFiles(
    TransformationID INTEGER NOT NULL,
    INDEX (TransformationID),
    FileID INTEGER NOT NULL,
    Status VARCHAR(32) DEFAULT "Unused",
    INDEX (Status),
    ErrorCount INT(4) NOT NULL DEFAULT 0,
    TaskID VARCHAR(32),
    TargetSE VARCHAR(255) DEFAULT "Unknown",
    UsedSE VARCHAR(255) DEFAULT "Unknown",
    LastUpdate DATETIME,
    InsertedTime  DATETIME,
    PRIMARY KEY (TransformationID,FileID)
);

-- -------------------------------------------------------------------------------
DROP TABLE IF EXISTS TransformationTasks;
CREATE TABLE TransformationTasks (
    TaskID INTEGER NOT NULL AUTO_INCREMENT,
    TransformationID INTEGER NOT NULL,
    ExternalStatus char(16) DEFAULT 'Created',
    INDEX(ExternalStatus),
    ExternalID char(16) DEFAULT '',
    TargetSE char(255) DEFAULT 'Unknown',
    CreationTime DATETIME NOT NULL,
    LastUpdateTime DATETIME NOT NULL,
    PRIMARY KEY(TransformationID,TaskID)
);

-- -------------------------------------------------------------------------------
DROP TABLE IF EXISTS TransformationFileTasks;
CREATE TABLE TransformationFileTasks (
  TransformationID INTEGER NOT NULL,
  FileID INTEGER NOT NULL,
  TaskID INTEGER NOT NULL,
  PRIMARY KEY(TransformationID,FileID,TaskID)
);

-- -------------------------------------------------------------------------------
DROP TABLE IF EXISTS TaskInputs;
CREATE TABLE TaskInputs (
TransformationID INTEGER NOT NULL,
TaskID INTEGER NOT NULL,
InputVector BLOB,
PRIMARY KEY(TransformationID,TaskID)
);

-- -------------------------------------------------------------------------------
DROP TABLE IF EXISTS TransformationInputDataQuery;
CREATE TABLE TransformationInputDataQuery(
TransformationID INTEGER NOT NULL,
ParameterName VARCHAR(512) NOT NULL,
ParameterValue BLOB NOT NULL,
ParameterType VARCHAR(8) NOT NULL,
PRIMARY KEY(TransformationID,ParameterName)
);

-- -------------------------------------------------------------------------------
DROP TABLE IF EXISTS DataFiles;
CREATE TABLE DataFiles (
   FileID INTEGER NOT NULL AUTO_INCREMENT,
   LFN VARCHAR(255) UNIQUE,
   Status varchar(32) DEFAULT 'AprioriGood',
   INDEX (Status),
   PRIMARY KEY (FileID)
);

