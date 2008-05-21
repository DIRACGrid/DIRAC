DROP DATABASE IF EXISTS TransferDB;

CREATE DATABASE TransferDB;


--Create user DIRAC
--use mysql;
--GRANT SELECT,INSERT,LOCK TABLES,UPDATE,DELETE,CREATE,DROP,ALTER ON TransferDB.* TO 'Dirac'@'localhost' IDENTIFIED BY 'lhcbMySQL';
--GRANT SELECT,INSERT,LOCK TABLES,UPDATE,DELETE,CREATE,DROP,ALTER ON TransferDB.* TO 'Dirac'@'%' IDENTIFIED BY 'lhcbMySQL';
--FLUSH PRIVILEGES;

-------------------------------------------------------------
-- Create TransferDB tables

use TransferDB;


-- THESE ARE THE TABLES FOR THE REQUEST DB
-- Requests, SubRequests,Files,Datasets

DROP TABLE IF EXISTS Requests;
CREATE TABLE Requests (
   RequestID INTEGER NOT NULL AUTO_INCREMENT,
   Status varchar(32) DEFAULT 'Receiving',
   RequestName varchar(128) NOT NULL,
   JobID int(11) DEFAULT NULL,
   OwnerDN varchar(255) DEFAULT NULL,
   DIRACInstance varchar(32) DEFAULT NULL,
   CreationTime DATETIME,
   SubmissionTime DATETIME,
   LastUpdate DATETIME, 
   PRIMARY KEY (RequestID,RequestName)
);

DROP TABLE IF EXISTS SubRequests;
CREATE TABLE SubRequests (
   RequestID INTEGER NOT NULL,
   SubRequestID INTEGER NOT NULL AUTO_INCREMENT,
   Status varchar(32) DEFAULT 'Receiving',
   RequestType  varchar(32) NOT NULL,
   Operation varchar(32),
   SourceSE varchar(32),
   TargetSE varchar(255),
   Catalogue varchar(32),
   SubmissionTime datetime,
   LastUpdate datetime,
   PRIMARY KEY (RequestID,SubRequestID) 
);

DROP TABLE IF EXISTS Files;
CREATE TABLE Files (
   SubRequestID INTEGER NOT NULL,
   FileID INTEGER NOT NULL AUTO_INCREMENT,
   Status varchar(32) DEFAULT 'New',
   LFN  varchar(255),
   Size INTEGER,
   PFN varchar(255),
   GUID varchar(64),
   Md5 varchar(32),
   Addler varchar(32),
   Attempt varchar(32),
   PRIMARY KEY (SubRequestID, FileID)  
);

DROP TABLE IF EXISTS Datasets;
CREATE TABLE Datasets(
   SubRequestID INTEGER NOT NULL,
   Dataset varchar(255) NOT NULL,
   Status varchar(32) DEFAULT 'NEW',
   PRIMARY KEY (SubRequestID,Dataset)
);

-- THESE ARE THE TABLES FOR THE TRANSFER DB
-- Channels,Channel,FTSReq,FileToFTS,FTSReqLogging,FileToCat,ReplicationTree

DROP TABLE IF EXISTS Channels;
CREATE TABLE Channels (
   ChannelID INTEGER NOT NULL AUTO_INCREMENT,
   SourceSite  varchar(32)  NOT NULL,
   DestinationSite varchar(32) NOT NULL,
   Status varchar(32) NOT NULL,   
   ChannelName  varchar(32),  
   PRIMARY KEY (ChannelID,SourceSite,DestinationSite)
);

DROP TABLE IF EXISTS Channel;
CREATE TABLE Channel (
  ChannelID INTEGER NOT NULL,
  FileID INTEGER NOT NULL,
  Status VARCHAR(32) NOT NULL,
  SourceSURL varchar(256)  NOT NULL,
  TargetSURL varchar(256)  NOT NULL,
  SpaceToken varchar(32)  NOT NULL,
  FileSize INTEGER NOT NULL,
  SubmitTime DATETIME NOT NULL,
  ExecutionTime DATETIME,
  PRIMARY KEY (ChannelID,FileID)  
); 

DROP TABLE IF EXISTS FTSReq;
CREATE TABLE FTSReq (
  FTSReqID INTEGER NOT NULL AUTO_INCREMENT,
  ChannelID INTEGER NOT NULL,
  Status varchar(32) DEFAULT 'Submitted',
  FTSGUID varchar(64) NOT NULL,
  FTSServer varchar(255) NOT NULL,
  NumberOfFiles INTEGER DEFAULT 0,
  TotalSize bigint(20) DEFAULT 0,
  SubmitTime datetime NOT NULL,
  LastMonitor datetime,
  PercentageComplete float default 0.0
  PRIMARY KEY (FTSReqID,ChannelID)
);

DROP TABLE IF EXISTS FileToFTS;
CREATE TABLE FileToFTS (
  FileID INTEGER NOT NULL,
  FTSReqID varchar(64) NOT NULL,
  ChannelID INTEGER NOT NULL,
  Status varchar(32) DEFAULT 'Submitted'
  Duration int(8) DEFAULT 0,
  Reason varchar(511),
  Retries int(8) DEFAULT 0,
  FileSize int(11) DEFAULT 0,
  SubmissionTime datetime,
  TerminalTime datetime,
  PRIMARY KEY (FileID,FTSReqID)
);

DROP TABLE IF EXISTS FTSReqLogging;
CREATE TABLE FTSReqLogging (
  FTSReqID INTEGER NOT NULL,
  Event varchar(100),
  EventDateTime datetime
);

DROP TABLE IF EXISTS FileToCat;
CREATE TABLE FileToCat (
  FileID INTEGER NOT NULL,
  ChannelID INTEGER NOT NULL,
  LFN varchar(255) NOT NULL,
  PFN varchar(255) NOT NULL,
  SE  varchar(255) NOT NULL,
  Status varchar(255) NOT NULL DEFAULT 'Executing',
  SubmitTime  datetime NOT NULL,
  CompleteTime datetime,
  PRIMARY KEY (FileID,ChannelID,Status)
);

DROP TABLE IF EXISTS ReplicationTree;
CREATE TABLE ReplicationTree (
  FileID INTEGER NOT NULL,
  ChannelID INTEGER NOT NULL,
  AncestorChannel varchar(8) NOT NULL,
  Strategy varchar(32),
  CreationTime datetime NOT NULL
);
