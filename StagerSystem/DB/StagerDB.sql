########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/StagerSystem/DB/StagerDB.sql,v 1.6 2009/06/19 14:47:46 acsmith Exp $
########################################################################
   
-------------------------------------------------------------
-- author: A. Smith
-- StagerDB definition
-------------------------------------------------------------

DROP DATABASE IF EXISTS StagerDB;

CREATE DATABASE StagerDB;

use mysql;
GRANT SELECT,INSERT,LOCK TABLES,UPDATE,DELETE,CREATE,DROP,ALTER ON StagerDB.* TO 'Dirac'@'localhost' IDENTIFIED BY 'must_be_set';
FLUSH PRIVILEGES;

use StagerDB;

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

DROP TABLE IF EXISTS Replicas;
CREATE TABLE Replicas(
  ReplicaID INTEGER AUTO_INCREMENT,
  Status VARCHAR(32) DEFAULT 'New',
  StorageElement VARCHAR(32) NOT NULL,
  LFN VARCHAR(255) NOT NULL,
  PFN VARCHAR(255),
  FileSize INTEGER(32) DEFAULT 0,
  Reason VARCHAR(255),
  Links INTEGER DEFAULT 0,
  PRIMARY KEY (ReplicaID,LFN,StorageElement),
  INDEX(ReplicaID,Status,StorageElement)
)ENGINE=INNODB;
delimiter //
CREATE TRIGGER replicasAfterUpdate AFTER UPDATE ON Replicas
FOR EACH ROW
BEGIN
  IF NEW.Status = 'Failed' THEN
    UPDATE Tasks SET Status='Failed' WHERE TaskID IN (SELECT TaskID from TaskReplicas WHERE ReplicaID=NEW.ReplicaID);
  END IF;
END;//
delimiter ;

DROP TABLE IF EXISTS TaskReplicas;
CREATE TABLE TaskReplicas(
  TaskID INTEGER(8) NOT NULL REFERENCES Tasks(TaskID),
  ReplicaID INTEGER(8) NOT NULL REFERENCES Replicas(ReplicaID),
  PRIMARY KEY (TaskID,ReplicaID),
  INDEX(TaskID,ReplicaID)
)ENGINE=INNODB;
CREATE TRIGGER taskreplicasAfterInsert AFTER INSERT ON TaskReplicas FOR EACH ROW UPDATE Replicas SET Replicas.Links=Replicas.Links+1 WHERE Replicas.ReplicaID=NEW.ReplicaID;
CREATE TRIGGER taskreplicasAfterDelete AFTER DELETE ON TaskReplicas FOR EACH ROW UPDATE Replicas SET Replicas.Links=Replicas.Links-1 WHERE Replicas.ReplicaID=OLD.ReplicaID;

DROP TABLE IF EXISTS StageRequests;
CREATE TABLE StageRequests(
  ReplicaID INTEGER(8) NOT NULL REFERENCES Replicas(ReplicaID),
  StageStatus VARCHAR(32) DEFAULT 'StageSubmitted',
  RequestID INTEGER(32),
  StageRequestSubmitTime DATETIME NOT NULL,
  INDEX (StageStatus),
  FOREIGN KEY (ReplicaID) REFERENCES Replicas(ReplicaID)
)ENGINE=INNODB;
CREATE TRIGGER stageAfterInsert AFTER INSERT ON StageRequests FOR EACH ROW UPDATE Replicas SET Replicas.Status = NEW.StageStatus WHERE NEW.ReplicaID=Replicas.ReplicaID;
CREATE TRIGGER stageAfterUpdate AFTER UPDATE ON StageRequests FOR EACH ROW UPDATE Replicas SET Replicas.Status = NEW.StageStatus WHERE NEW.ReplicaID=Replicas.ReplicaID;

DROP TABLE IF EXISTS Pins;
CREATE TABLE Pins(
  ReplicaID INTEGER(8) NOT NULL REFERENCES Replicas(ReplicaID),
  PinStatus VARCHAR(32) DEFAULT 'PinCreated',
  RequestID INTEGER(32),
  PinCreationTime DATETIME NOT NULL,
  PinExpiryTime DATETIME NOT NULL,
  INDEX(PinStatus),
  FOREIGN KEY (ReplicaID) REFERENCES Replicas(ReplicaID)
)ENGINE=INNODB;
CREATE TRIGGER pinsAfterInsert AFTER INSERT ON Pins FOR EACH ROW UPDATE Replicas SET Replicas.Status = NEW.PinStatus WHERE NEW.ReplicaID=Replicas.ReplicaID;
CREATE TRIGGER pinsAfterUpdate AFTER UPDATE ON Pins FOR EACH ROW UPDATE Replicas SET Replicas.Status = NEW.PinStatus WHERE NEW.ReplicaID=Replicas.ReplicaID;
