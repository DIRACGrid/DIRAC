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
   Status varchar(32) DEFAULT 'New',
   SubmissionTime DATETIME,
   PRIMARY KEY (RequestID)
);

DROP TABLE IF EXISTS SubRequests;
CREATE TABLE Requests (
   RequestID INTEGER NOT NULL,
   SubRequestID INTEGER NOT NULL AUTO_INCREMENT,
   Status varchar(32) DEFAULT 'New',
   Operation varchar(32),
   Source varchar(32),
   Destination varchar(32),
   Catalogues varchar(32),
   SubmissionTime DATETIME,
   PRIMARY KEY (RequestID,SubRequestID)
);

DROP TABLE IF EXISTS Files;
CREATE TABLE Files (
  SubRequestID INTEGER NOT NULL,
  FileID INTEGER NOT NULL AUTO_INCREMENT,
  LFN VARCHAR(255),
  Size INTEGER,
  PFN VARCHAR(255),
  GUID VARCHAR(32),
  Md5 VARCHAR(32),
  Addler VARCHAR(32),
  Attempt VARCHAR(32),
  Dataset VARCHAR(255),
  Status VARCHAR(32) DEFAULT 'New',
  PRIMARY KEY (SubRequestID, FileID)
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
  Duration INTEGER DEFAULT 0,
  Reason VARCHAR(255),
  PRIMARY KEY(FileID,FTSReqID)
);

DROP TABLE IF EXISTS FTSReq;
CREATE TABLE FTSReq (
  FTSReqID INTEGER NOT NULL AUTO_INCREMENT,
  FTSGUID varchar(64) NOT NULL,
  FTSServer varchar(255),
  Status varchar(32) DEFAULT 'Waiting',
  PRIMARY KEY(FTSReqID,FTSGUID)
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

