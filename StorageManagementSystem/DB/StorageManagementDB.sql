########################################################################
# $Header: /tmp/libdirac/tmp.FKduyw2449/dirac/DIRAC3/DIRAC/StorageManagementSystem/DB/StorageManagementDB.sql,v 1.4 2009/11/04 09:25:05 acsmith Exp $
########################################################################

DROP DATABASE IF EXISTS StorageManagementDB;

CREATE DATABASE StorageManagementDB;

use mysql;
GRANT SELECT,INSERT,LOCK TABLES,UPDATE,DELETE,CREATE,DROP,ALTER ON StorageManagementDB.* TO 'Dirac'@'localhost' IDENTIFIED BY 'must_be_set';
FLUSH PRIVILEGES;

use StorageManagementDB;

DROP TABLE IF EXISTS CacheReplicas;
CREATE TABLE CacheReplicas(
  ReplicaID INTEGER AUTO_INCREMENT,
  Type VARCHAR(32) NOT NULL,
  Status VARCHAR(32) DEFAULT 'New',
  SE VARCHAR(32) NOT NULL,
  LFN VARCHAR(255) NOT NULL,
  PFN VARCHAR(255),
  Size INTEGER(32) DEFAULT 0,
  FileChecksum VARCHAR(255) NOT NULL,
  GUID VARCHAR(255) NOT NULL,
  SubmitTime DATETIME NOT NULL,
  LastUpdate DATETIME,
  Reason VARCHAR(255),
  Links INTEGER DEFAULT 0,
  PRIMARY KEY (ReplicaID,LFN,SE),
  INDEX(ReplicaID,Status,SE)
)ENGINE=INNODB;
delimiter //
CREATE TRIGGER replicasAfterUpdate AFTER UPDATE ON CacheReplicas
FOR EACH ROW
BEGIN
  IF NEW.Status = 'Failed' THEN
    UPDATE Tasks SET Status='Failed' WHERE TaskID IN (SELECT TaskID from TaskReplicas WHERE ReplicaID=NEW.ReplicaID);
  END IF;
  IF NEW.Status = 'Waiting' THEN
    UPDATE Tasks SET Status = 'Waiting' WHERE Status = 'New' AND TaskID IN (SELECT TaskID from TaskReplicas WHERE ReplicaID=NEW.ReplicaID);
  END IF;
  IF NEW.Status = 'StageSubmitted' THEN
    UPDATE Tasks SET Status = 'StageSubmitted' WHERE Status = 'Waiting' AND TaskID IN (SELECT TaskID from TaskReplicas WHERE ReplicaID=NEW.ReplicaID);
  END IF;
  IF NEW.Status = 'Staged' THEN
    UPDATE Tasks SET Status = 'StageCompleting' WHERE Status = 'StageSubmitted' AND TaskID IN (SELECT TaskID from TaskReplicas WHERE ReplicaID=NEW.ReplicaID);
  END IF;
END;//
delimiter ;

DROP TABLE IF EXISTS Tasks;
CREATE TABLE Tasks(
  TaskID INTEGER AUTO_INCREMENT,
  Status VARCHAR(32) DEFAULT 'New',
  Source VARCHAR(32) NOT NULL,
  SubmitTime DATETIME NOT NULL,
  CompleteTime DATETIME,
  CallBackMethod VARCHAR(255),
  SourceTaskID VARCHAR(32),
  PRIMARY KEY(TaskID,Status),
  INDEX(TaskID,Status)
)ENGINE=INNODB;

DROP TABLE IF EXISTS TaskReplicas;
CREATE TABLE TaskReplicas(
  TaskID INTEGER(8) NOT NULL REFERENCES Tasks(TaskID),
  ReplicaID INTEGER(8) NOT NULL REFERENCES CacheReplicas(ReplicaID),
  PRIMARY KEY (TaskID,ReplicaID),
  INDEX(TaskID,ReplicaID)
)ENGINE=INNODB;
CREATE TRIGGER taskreplicasAfterInsert AFTER INSERT ON TaskReplicas FOR EACH ROW UPDATE CacheReplicas SET CacheReplicas.Links=CacheReplicas.Links+1 WHERE CacheReplicas.ReplicaID=NEW.ReplicaID;
CREATE TRIGGER taskreplicasAfterDelete AFTER DELETE ON TaskReplicas FOR EACH ROW UPDATE CacheReplicas SET CacheReplicas.Links=CacheReplicas.Links-1 WHERE CacheReplicas.ReplicaID=OLD.ReplicaID;

DROP TABLE IF EXISTS StageRequests;
CREATE TABLE StageRequests(
  ReplicaID INTEGER(8) NOT NULL REFERENCES CacheReplicas(ReplicaID),
  StageStatus VARCHAR(32) DEFAULT 'StageSubmitted',
  RequestID INTEGER(32),
  StageRequestSubmitTime DATETIME NOT NULL,
  StageRequestCompletedTime DATETIME,
  PinLength INTEGER(8),
  PinExpiryTime DATETIME,
  INDEX (StageStatus),
  FOREIGN KEY (ReplicaID) REFERENCES CacheReplicas(ReplicaID)
)ENGINE=INNODB;
CREATE TRIGGER stageAfterInsert AFTER INSERT ON StageRequests FOR EACH ROW UPDATE CacheReplicas SET CacheReplicas.Status = NEW.StageStatus WHERE NEW.ReplicaID=CacheReplicas.ReplicaID;
CREATE TRIGGER stageAfterUpdate AFTER UPDATE ON StageRequests FOR EACH ROW UPDATE CacheReplicas SET CacheReplicas.Status = NEW.StageStatus WHERE NEW.ReplicaID=CacheReplicas.ReplicaID;
