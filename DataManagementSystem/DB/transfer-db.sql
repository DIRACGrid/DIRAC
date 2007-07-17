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

DROP TABLE IF EXISTS Requests;
CREATE TABLE Requests (
   RequestID INTEGER NOT NULL AUTO_INCREMENT,
   Status varchar(32) DEFAULT 'New',
   Operation varchar(32),
   Source varchar(32),
   Destination varchar(32),
   SubmissionTime DATETIME,
   PRIMARY KEY (RequestID)
);

DROP TABLE IF EXISTS Files;
CREATE TABLE Files (
  RequestID INTEGER NOT NULL,
  FileID INTEGER NOT NULL,
  LFN VARCHAR(255),
  FileSize INTEGER NOT NULL,  
  Status VARCHAR(32) DEFAULT 'New',
  PRIMARY KEY (RequestID, FileID)
);

DROP TABLE IF EXISTS FileToCat;
CREATE TABLE FileToCat (
  FileID INTEGER NOT NULL,
  Catalog varchar(32) NOT NULL,
  Status varchar(32) DEFAULT 'Waiting',
  PRIMARY KEY(FileID)
);

DROP TABLE IF EXISTS FileToFTS; 
CREATE TABLE FileToFTS (
  FileID INTEGER NOT NULL,
  FTSReqID varchar(64) NOT NULL,
  Status varchar(32) DEFAULT 'Waiting',
  Duration INTEGER default 0,
  Reason VARCHAR(255),
  PRIMARY KEY(FileID)
);

DROP TABLE IF EXISTS FTSReq;
CREATE TABLE FTSReq (
  ReqID INTEGER NOT NULL,
  FTSReqID varchar(64) NOT NULL,
  Status varchar(32) DEFAULT 'Waiting',
  PRIMARY KEY(ReqID,FTSReqID)
);

DROP TABLE IF EXISTS Channels;
CREATE TABLE Channels (
  ChannelID INTEGER NOT NULL,
  SourceSite varchar(32) NOT NULL,
  DestinationSite varchar(32) NOT NULL,
  ActiveJobs INTEGER DEFAULT 0,
  LatestThroughPut FLOAT DEFAULT 0.0, 
  Status varchar(32) NOT NULL,
  PRIMARY KEY(ChannelID)
);

DROP TABLE IF EXISTS Channel;
CREATE TABLE Channel (
  ChannelID INTEGER NOT NULL,
  FileID INTEGER NOT NULL,
  SubmitTime DATETIME NOT NULL, 
  Status varchar(32) NOT NULL,
  PRIMARY KEY(ChannelID,FileID)
);

