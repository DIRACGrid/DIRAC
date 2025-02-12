
-- When installing via dirac tools, the following is not needed (still here for reference)
--
-- DROP DATABASE IF EXISTS StorageManagementDB;
-- CREATE DATABASE StorageManagementDB;
-- use mysql;
-- GRANT SELECT,INSERT,LOCK TABLES,UPDATE,DELETE,CREATE,DROP,ALTER,REFERENCES ON StorageManagementDB.* TO 'Dirac'@'%' IDENTIFIED BY 'must_be_set';
-- FLUSH PRIVILEGES;

use StorageManagementDB;

DROP TABLE IF EXISTS TaskReplicas;
DROP TABLE IF EXISTS StageRequests;
DROP TABLE IF EXISTS CacheReplicas;
DROP TABLE IF EXISTS Tasks;

CREATE TABLE Tasks(
  `TaskID` INTEGER AUTO_INCREMENT,
  `Status` VARCHAR(32) DEFAULT 'New',
  `Source` VARCHAR(32) NOT NULL,
  `SubmitTime` DATETIME NOT NULL,
  `LastUpdate` DATETIME,
  `CompleteTime` DATETIME,
  `CallBackMethod` VARCHAR(255),
  `SourceTaskID` VARCHAR(32),
  PRIMARY KEY(`TaskID`),
  INDEX(`TaskID`,`Status`)
)ENGINE=INNODB;

CREATE TABLE CacheReplicas(
  `ReplicaID` INTEGER AUTO_INCREMENT,
  `Type` VARCHAR(32) NOT NULL,
  `Status` VARCHAR(32) DEFAULT 'New',
  `SE` VARCHAR(32) NOT NULL,
  `LFN` VARCHAR(255) NOT NULL,
  `PFN` VARCHAR(255),
  `Size` BIGINT(60) DEFAULT 0,
  `FileChecksum` VARCHAR(255) NOT NULL,
  `GUID` VARCHAR(255) NOT NULL,
  `SubmitTime` DATETIME NOT NULL,
  `LastUpdate` DATETIME,
  `Reason` VARCHAR(255),
  `Links` INTEGER DEFAULT 0,
  PRIMARY KEY (`ReplicaID`),
  INDEX(`ReplicaID`,`Status`,`SE`)
)ENGINE=INNODB;


CREATE TABLE TaskReplicas(
  `TaskID` INTEGER(8) NOT NULL,
  `ReplicaID` INTEGER(8) NOT NULL,
  PRIMARY KEY (`TaskID`, `ReplicaID`),
  FOREIGN KEY (`TaskID`) REFERENCES `Tasks`(`TaskID`),
  FOREIGN KEY (`ReplicaID`) REFERENCES `CacheReplicas`(`ReplicaID`),
  INDEX(`TaskID`, `ReplicaID`)
)ENGINE=INNODB;
CREATE TRIGGER taskreplicasAfterInsert AFTER INSERT ON TaskReplicas FOR EACH ROW UPDATE CacheReplicas SET CacheReplicas.Links=CacheReplicas.Links+1 WHERE CacheReplicas.ReplicaID=NEW.ReplicaID;
CREATE TRIGGER taskreplicasAfterDelete AFTER DELETE ON TaskReplicas FOR EACH ROW UPDATE CacheReplicas SET CacheReplicas.Links=CacheReplicas.Links-1 WHERE CacheReplicas.ReplicaID=OLD.ReplicaID;


CREATE TABLE StageRequests(
  `ReplicaID` INTEGER(8) NOT NULL,
  `StageStatus` VARCHAR(32) DEFAULT 'StageSubmitted',
  `RequestID` VARCHAR(64) DEFAULT '',
  `StageRequestSubmitTime` DATETIME NOT NULL,
  `StageRequestCompletedTime` DATETIME,
  `PinLength` INTEGER(8),
  `PinExpiryTime` DATETIME,
  INDEX (StageStatus),
  FOREIGN KEY (`ReplicaID`) REFERENCES CacheReplicas(`ReplicaID`)
)ENGINE=INNODB;
