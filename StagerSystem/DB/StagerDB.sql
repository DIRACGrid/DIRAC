-------------------------------------------------------------
-- author: A. Smith
-- StagerDB definition
-------------------------------------------------------------

DROP DATABASE IF EXISTS StagerDB;

CREATE DATABASE StagerDB;
--
--
-- Create user DIRAC
use mysql;
GRANT SELECT,INSERT,LOCK TABLES,UPDATE,DELETE,CREATE,DROP,ALTER ON StagerDB.* TO 'Dirac'@'localhost' IDENTIFIED BY 'must_be_set';
#GRANT SELECT,INSERT,LOCK TABLES,UPDATE,DELETE,CREATE,DROP,ALTER ON StagerDB.* TO 'Dirac'@'%' IDENTIFIED BY 'must_be_set';

FLUSH PRIVILEGES;

use StagerDB;

DROP TABLE IF EXISTS Files;
CREATE TABLE Files(
  FileID INTEGER AUTO_INCREMENT,
  Status VARCHAR(32) DEFAULT 'New',
  StorageElement VARCHAR(32) NOT NULL,
  LFN VARCHAR(255) NOT NULL,
  FileSize INTEGER(32) DEFAULT 0,
  PFN VARCHAR(255),
  PRIMARY KEY (FileID,LFN,StorageElement),
  INDEX(StorageElement,Status)
);

DROP TABLE IF EXISTS Tasks;
CREATE TABLE Tasks(
  FileID INTEGER NOT NULL,
  Status VARCHAR(32) DEFAULT 'New',
  TaskID INTEGER(8) NOT NULL,
  Source VARCHAR(32) NOT NULL,
  SubmitTime DATETIME NOT NULL,
  CallBackMethod VARCHAR(255),
  PRIMARY KEY(FileID,Source,TaskID)
);

DROP TABLE IF EXISTS StageRequests;
CREATE TABLE StageRequests(
  FileID INTEGER NOT NULL,
  StageStatus VARCHAR(32) DEFAULT 'Submitted',
  SRMRequestID INTEGER(32),
  StageRequestSubmitTime DATETIME NOT NULL,
  INDEX (StageStatus)
);

DROP TABLE IF EXISTS Pins;
CREATE TABLE Pins(
  FileID INTEGER NOT NULL,
  PinStatus VARCHAR(32) DEFAULT 'Created',
  SRMRequestID INTEGER(32),
  PinCreationTime DATETIME NOT NULL,
  PinExpiryTime DATETIME NOT NULL,
  INDEX(PinStatus)
);
