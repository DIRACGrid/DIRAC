-------------------------------------------------------------
-- Integrity database  definition
-------------------------------------------------------------

DROP DATABASE IF EXISTS DataIntegrityDB;

CREATE DATABASE DataIntegrityDB;
--
-- Must set passwords for database user by replacing "must_be_set".
--
-- Create user DIRAC
use mysql;
GRANT SELECT,INSERT,LOCK TABLES,UPDATE,DELETE,CREATE,DROP,ALTER ON DataIntegrityDB.* TO 'Dirac'@'localhost' IDENTIFIED BY 'must_be_set';
GRANT SELECT,INSERT,LOCK TABLES,UPDATE,DELETE,CREATE,DROP,ALTER ON DataIntegrityDB.* TO 'Dirac'@'%' IDENTIFIED BY 'must_be_set';
FLUSH PRIVILEGES;

-------------------------------------------------------------

use DataIntegrityDB;

DROP TABLE IF EXISTS Problematics;
CREATE TABLE Problematics(
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
  INDEX (Prognosis,Status)
);