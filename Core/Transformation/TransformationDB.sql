-- $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/Transformation/TransformationDB.sql,v 1.2 2008/01/25 16:36:51 gkuznets Exp $
--------------------------------------------------------------------------------
--
--  Schema definition for the TransformationDB database -
--  a generic engine to define input data streams and support dynamic
--  data grouping per unit of execution,e.g. jobs or data transfer requests
--
--  This schema is supposed to be included b specific data processing databases,
--  ProductionDB or AutoTransferDB
--
--------------------------------------------------------------------------------

DROP TABLE IF EXISTS Transformations;

--------------------------------------------------------------------------------
-- This table store Transformation definitions
-- TransformationID - Transformation internal incremental ID
-- Name - name of the Transformation
-- Description - short description of the workflow to fit one line
-- LongDescription - description of the workflow in a free form
-- CreationTime - time stamp
-- AuthorDN - persone published Production
-- AuthorGroup - group used to publish
-- Type - type of the workflow.
--   SIMULATION - Montecarlo production, no input data required
--   PROCESSING - Processing production, input files required
--   REPLICATION - data replication production, no body required
-- Mode - submission mode of the jobs for the submission agent
--   MANUAL
--   AUTOMATIC
-- Status - information about current status of the production
--   NEW - newly created, equivalent to STOPED
--   ACTIVE - can submit
--   STOPPED - stopped by manager
--   DONE - job limits reached, extension is possible
--   ERROR - Production with error, equivalent to STOPPED
--   TERMINATED - stopped, extension impossible
-- FileMask - filter mask
-- Body - XML body of the Transformation if required.
-- In the Table PArAmeters
--     PRParent - name of the parent Workflow used to create the current one.
--            taken from the XML field "type"
--     GroupSize - number of files per Transformation
--------------------------------------------------------------------------------

CREATE TABLE Transformations (
    TransformationID INTEGER NOT NULL AUTO_INCREMENT,
    TransformationName VARCHAR(255) NOT NULL,
    Description VARCHAR(255),
    LongDescription  BLOB,
    CreationDate DATETIME,
    AuthorDN VARCHAR(255) NOT NULL,
    AuthorGroup VARCHAR(255) NOT NULL,
    Type CHAR(16) DEFAULT 'Simulation',
    Mode CHAR(16) DEFAULT 'Manual',
    AgentType CHAR(16) DEFAULT 'Unknown',
    Status  CHAR(16) DEFAULT 'New',
    FileMask VARCHAR(255),
    FileGroupSize INT NOT NULL DEFAULT 0,
    Body BLOB,
    PRIMARY KEY(TransformationID,TransformationName)
) ENGINE=InnoDB;

--------------------------------------------------------------------------------
DROP TABLE IF EXISTS TransformationParameters;
--------------------------------------------------------------------------------
--
--  TransformationParameters table is a container for arbitrary parameters needed by specific
--  transformations
--
--------------------------------------------------------------------------------

CREATE TABLE TransformationParameters (
    TransformationID INTEGER NOT NULL,
    ParameterName VARCHAR(32) NOT NULL,
    ParameterValue VARCHAR(255) NOT NULL,
    PRIMARY KEY(TransformationID,ParameterName)
);

--------------------------------------------------------------------------------
DROP TABLE IF EXISTS TransformationLog;
--------------------------------------------------------------------------------
--
--  TransformationLog table keeps looging messages about status changes of the
--  transformations
--
--------------------------------------------------------------------------------

CREATE TABLE TransformationLog (
    TransformationID INTEGER NOT NULL,
    Message VARCHAR(255) NOT NULL,
    Author VARCHAR(255) NOT NULL DEFAULT "Unknown",
    MessageDate DATETIME NOT NULL,
    INDEX (TransformationID)
);

--------------------------------------------------------------------------------
DROP TABLE IF EXISTS DataFiles;
--------------------------------------------------------------------------------
CREATE TABLE DataFiles (
   FileID INTEGER NOT NULL AUTO_INCREMENT,
   LFN VARCHAR(255) UNIQUE,
   Status varchar(32) DEFAULT 'AprioriGood',
   PRIMARY KEY (FileID, LFN)
);

DROP TABLE IF EXISTS Replicas;
CREATE TABLE Replicas (
  FileID INTEGER NOT NULL,
  PFN VARCHAR(255),
  SE VARCHAR(32),
  Status VARCHAR(32) DEFAULT 'AprioriGood',
  PRIMARY KEY (FileID, SE)
);
