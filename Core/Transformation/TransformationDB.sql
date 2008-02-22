-- $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/Transformation/TransformationDB.sql,v 1.10 2008/02/22 15:25:22 gkuznets Exp $
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
-- Plugin - the plugin used to group files into jobs
--   NONE
--   BROADCAST
--   LOADBALANCE
-- AgentType - the agent that will process the transformation
--   Manual
--   Automatic
--   ReplicationPlacement ???
--   ProductionAgent ???
-- Status - information about current status of the production
--   New - newly created, equivalent to STOPED
--   Active - can submit
--   Flush - final stage, ignoring GroupSize
--   Stopped - stopped by manager
--   Error - Production with error, equivalent to STOPPED
--   Terminated - stopped, extension impossible
-- FileMask - filter mask
------- Explanation about status field ------
We have execute three types of action for each transformation
1 - Publish files in the Transformation table
2 - Create jobs
3 - Submit jobs
     STATUS | Avalible actions
New           1
Stopped       1
Active        1 2 3
Flush           2 3
Error         x
Terminated    x
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
    Plugin CHAR(16) DEFAULT 'None',
    AgentType CHAR(16) DEFAULT 'Manual',
    Status  CHAR(16) DEFAULT 'New',
    FileMask VARCHAR(255),
    PRIMARY KEY(TransformationID),
    INDEX(TransformationName)
) ENGINE=InnoDB;

----------------------------------------------------------------------------------
-- Once a transformation in entered in the the database a table is created to contain its associated files
--
-- CREATE TABLE T_$TransformationID(
--   FileID INTEGER NOT NULL,
--   Status VARCHAR(32) DEFAULT "Unused",
--   ErrorCount INT(4) NOT NULL DEFAULT 0,
--   JobID VARCHAR(32),
--   UsedSE VARCHAR(32) DEFAULT "Unknown",
--   PRIMARY KEY (FileID,Status)
--
-------------------------------------------------------------------------------------

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

--------------------------------------------------------------------------------
DROP TABLE IF EXISTS Replicas;
--------------------------------------------------------------------------------
CREATE TABLE Replicas (
  FileID INTEGER NOT NULL,
  PFN VARCHAR(255),
  SE VARCHAR(32),
  Status VARCHAR(32) DEFAULT 'AprioriGood',
  PRIMARY KEY (FileID, SE)
);

--------------------------------------------------------------------------------
DROP TABLE IF EXISTS FileTransformations;
--------------------------------------------------------------------------------
CREATE TABLE FileTransformations(
   FileID INTEGER NOT NULL,
   TransformationID INTEGER NOT NULL,
   TransformationType VARCHAR(32),
   PRIMARY KEY (FileID, TransformationID)
);


