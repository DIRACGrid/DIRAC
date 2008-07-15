DROP DATABASE IF EXISTS RequestDB;
CREATE DATABASE RequestDB;

--
-- Must set passwords for database user by replacing "must_be_set".
--
use mysql;
GRANT SELECT,INSERT,LOCK TABLES,UPDATE,DELETE,CREATE,DROP,ALTER ON RequestDB.* TO 'Dirac'@'localhost' IDENTIFIED BY 'must_be_set';
GRANT SELECT,INSERT,LOCK TABLES,UPDATE,DELETE,CREATE,DROP,ALTER ON RequestDB.* TO 'Dirac'@'%' IDENTIFIED BY 'must_be_set';

use RequestDB;

-- THESE ARE THE TABLES FOR THE REQUEST DB
-- Requests, SubRequests,Files,Datasets

DROP TABLE IF EXISTS Requests;
CREATE TABLE Requests (
   RequestID INTEGER NOT NULL AUTO_INCREMENT,
   Status varchar(32) DEFAULT 'Receiving',
   RequestName varchar(128) NOT NULL,
   JobID int(11) DEFAULT NULL,
   OwnerDN varchar(255) DEFAULT NULL,
   OwnerGroup varchar(32) DEFAULT NULL,
   DIRACSetup varchar(32) DEFAULT NULL,
   SourceComponent varchar(32) DEFAULT NULL,
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
   CreationTime DATETIME,
   SubmissionTime DATETIME,
   LastUpdate DATETIME,
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
