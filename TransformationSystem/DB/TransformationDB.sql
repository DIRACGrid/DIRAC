-- -------------------------------------------------------------------------------
--  Schema definition for the TransformationDB database a generic
--  engine to define input data streams and support dynamic data 
--  grouping per unit of execution.

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
    GroupSize INT NOT NULL DEFAULT 1,
    InheritedFrom INTEGER DEFAULT 0,
    Body LONGBLOB,
    MaxNumberOfJobs INT NOT NULL DEFAULT 0,
    EventsPerJob INT NOT NULL DEFAULT 0,
    PRIMARY KEY(TransformationID),
    INDEX(TransformationName)
) ENGINE=InnoDB;

-- MaxNumberOfJobs => MaxNumberOfTasks
-- EventsPerJob => EventsPerTask

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
    JobID VARCHAR(32),
    TargetSE VARCHAR(255) DEFAULT "Unknown",
    UsedSE VARCHAR(255) DEFAULT "Unknown",
    LastUpdate DATETIME,
    InsertedTime  DATETIME,
    PRIMARY KEY (TransformationID,FileID)
);

-- JobID => TaskID

-- -------------------------------------------------------------------------------
DROP TABLE IF EXISTS Jobs;
CREATE TABLE Jobs (
  JobID INTEGER NOT NULL AUTO_INCREMENT,
  TransformationID INTEGER NOT NULL,
  WmsStatus char(16) DEFAULT 'Created',
  JobWmsID char(16) DEFAULT '',
  TargetSE char(255) DEFAULT 'Unknown',
  CreationTime DATETIME NOT NULL,
  LastUpdateTime DATETIME NOT NULL,
  PRIMARY KEY(TransformationID,JobID),
INDEX(WmsStatus)
);

-- Jobs => TransformationTasks
-- JobID => TaskID
-- WmsStatus => ExternalStatus
-- JobWmsID => ExternalID

-- -------------------------------------------------------------------------------
DROP TABLE IF EXISTS JobInputs;
CREATE TABLE JobInputs (
TransformationID INTEGER NOT NULL,
JobID INTEGER NOT NULL,
InputVector BLOB,
PRIMARY KEY(TransformationID,JobID)
);

-- JobInputs => TransformationInputs
-- JobID => TaskID

-- -------------------------------------------------------------------------------
DROP TABLE IF EXISTS DataFiles;
CREATE TABLE DataFiles (
   FileID INTEGER NOT NULL AUTO_INCREMENT,
   LFN VARCHAR(255) UNIQUE,
   Status varchar(32) DEFAULT 'AprioriGood',
   INDEX (Status),
   PRIMARY KEY (FileID, LFN)
);

-- -------------------------------------------------------------------------------
DROP TABLE IF EXISTS Replicas;
CREATE TABLE Replicas (
  FileID INTEGER NOT NULL,
  PFN VARCHAR(255),
  SE VARCHAR(32),
  Status VARCHAR(32) DEFAULT 'AprioriGood',
  INDEX (Status),
  PRIMARY KEY (FileID, SE)
);
