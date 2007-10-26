DROP DATABASE IF EXISTS TransferDB;

CREATE DATABASE TransferDB;


--Create user DIRACuse mysql;
GRANT SELECT,INSERT,LOCK TABLES,UPDATE,DELETE,CREATE,DROP,ALTER ON TransferDB.* TO 'Dirac'@'localhost' IDENTIFIED BY 'lhcbMySQL';
GRANT SELECT,INSERT,LOCK TABLES,UPDATE,DELETE,CREATE,DROP,ALTER ON TransferDB.* TO 'Dirac'@'%' IDENTIFIED BY 'lhcbMySQL';
FLUSH PRIVILEGES;

-------------------------------------------------------------
-- Create TransferDB tables

use TransferDB;

DROP TABLE IF EXISTS Requests;
CREATE TABLE Requests (
   RequestID INTEGER NOT NULL AUTO_INCREMENT,
   RequestName varchar(128) UNIQUE NOT NULL,
   JobID INTEGER,
   OwnerDN VARCHAR(255),
   DIRACInstance varchar(32),
   Status varchar(32) DEFAULT 'Receiving',
   CreationTime DATETIME,
   SubmissionTime DATETIME,
   PRIMARY KEY (RequestID,RequestName)
);

DROP TABLE IF EXISTS SubRequests;
CREATE TABLE SubRequests (
   RequestID INTEGER NOT NULL,
   SubRequestID INTEGER NOT NULL AUTO_INCREMENT,
   RequestType varchar(32) NOT NULL,
   Status varchar(32) DEFAULT 'Receiving',
   SpaceToken varchar(32),
   Operation varchar(32),
   SourceSE varchar(32),
   TargetSE varchar(32),
   Catalogue varchar(32),
   SubmissionTime DATETIME,
   PRIMARY KEY (SubRequestID)
);

DROP TABLE IF EXISTS Files;
CREATE TABLE Files (
  SubRequestID INTEGER NOT NULL,
  FileID INTEGER NOT NULL AUTO_INCREMENT,
  LFN VARCHAR(255),
  Size INTEGER,
  PFN VARCHAR(255),
  GUID VARCHAR(64),
  Md5 VARCHAR(32),
  Addler VARCHAR(32),
  Attempt VARCHAR(32),
  Status VARCHAR(32) DEFAULT 'New',
  PRIMARY KEY (FileID)
);

DROP TABLE IF EXISTS Datasets;
CREATE TABLE Datasets (
  SubRequestID INTEGER NOT NULL,
  Dataset VARCHAR(255),
  Status VARCHAR(32) DEFAULT 'New',
  PRIMARY KEY (SubRequestID,Dataset)
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
  Status varchar(32) DEFAULT 'Submitted',
  Duration INTEGER DEFAULT 0,
  Retries INTEGER DEFAULT 0,
  Reason VARCHAR(255),
  PRIMARY KEY(FileID,FTSReqID)
);

DROP TABLE IF EXISTS FTSReq;
CREATE TABLE FTSReq (
  FTSReqID INTEGER NOT NULL AUTO_INCREMENT,
  FTSGUID varchar(64) NOT NULL,
  FTSServer varchar(255) NOT NULL,
  ChannelID INTEGER NOT NULL,
  SubmitTime DATETIME NOT NULL,
  LastMonitor  DATETIME,
  PercentageComplete FLOAT DEFAULT 0.0,
  Status varchar(32) DEFAULT 'Submitted',
  PRIMARY KEY(FTSReqID,FTSGUID)
);

DROP TABLE IF EXISTS Channels;
CREATE TABLE Channels (
  ChannelID INTEGER NOT NULL AUTO_INCREMENT,
  SourceSite varchar(32) NOT NULL,
  DestinationSite varchar(32) NOT NULL,
  ActiveJobs INTEGER DEFAULT 0,
  LatestThroughPut FLOAT DEFAULT 0.0,
  Status varchar(32) NOT NULL,
  PRIMARY KEY(ChannelID,SourceSite,DestinationSite)
);

DROP TABLE IF EXISTS Channel;
CREATE TABLE Channel (
  ChannelID INTEGER NOT NULL,
  FileID INTEGER NOT NULL,
  SourceSURL varchar(256) NOT NULL,
  TargetSURL varchar(256) NOT NULL,
  SpaceToken varchar(32) NOT NULL,
  SubmitTime DATETIME NOT NULL,
  Status varchar(32) NOT NULL,
  PRIMARY KEY(ChannelID,FileID)
);