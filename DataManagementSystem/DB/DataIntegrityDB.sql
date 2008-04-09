-------------------------------------------------------------
-- Integrity database  definition
-------------------------------------------------------------

DROP DATABASE IF EXISTS DataIntegrityDB;

CREATE DATABASE DataIntegrityDB;

-- Create user DIRAC
use mysql;
GRANT SELECT,INSERT,LOCK TABLES,UPDATE,DELETE,CREATE,DROP,ALTER ON DataIntegrityDB.* TO 'Dirac'@'localhost' IDENTIFIED BY 'lhcbMySQL';
GRANT SELECT,INSERT,LOCK TABLES,UPDATE,DELETE,CREATE,DROP,ALTER ON DataIntegrityDB.* TO 'Dirac'@'%' IDENTIFIED BY 'lhcbMySQL';
#FLUSH PRIVILEGES;

-------------------------------------------------------------

use DataIntegrityDB;

DROP TABLE IF EXISTS Problematics;
CREATE TABLE Problematics(
  FileID INTEGER NOT NULL AUTO_INCREMENT,
  Prognosis VARCHAR(32) NOT NULL,
  LFN VARCHAR(255) NOT NULL,
  PFN VARCHAR(255),
  Size BIGINT(20),
  StorageElement VARCHAR(32),
  GUID VARCHAR(255),
  Status VARCHAR(32) DEFAULT 'New',
  Retries INTEGER DEFAULT 0,
  InsertDate DATETIME NOT NULL,
  CompleteDate DATETIME,
  Source VARCHAR(127) NOT NULL DEFAULT 'Unknown',
  PRIMARY KEY(FileID),
  INDEX (Prognosis,Status)
);
