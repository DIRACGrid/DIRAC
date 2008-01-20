-------------------------------------------------------------
-- RAW Integrity DB definition
-------------------------------------------------------------

DROP DATABASE IF EXISTS RAWIntegrityDB;

CREATE DATABASE RAWIntegrityDB;

-- Create user DIRAC
use mysql;
GRANT SELECT,INSERT,LOCK TABLES,UPDATE,DELETE,CREATE,DROP,ALTER ON RAWIntegrityDB.* TO 'Dirac'@'localhost' IDENTIFIED BY 'lhcbMySQL';
GRANT SELECT,INSERT,LOCK TABLES,UPDATE,DELETE,CREATE,DROP,ALTER ON RAWIntegrityDB.* TO 'Dirac'@'%' IDENTIFIED BY 'lhcbMySQL';

FLUSH PRIVILEGES;

-------------------------------------------------------------

use RAWIntegrityDB;

DROP TABLE IF EXISTS Files;
CREATE TABLE Files(
   LFN VARCHAR(255) NOT NULL,
   PFN VARCHAR(255) NOT NULL,
   Size INTEGER NOT NULL,
   StorageElement VARCHAR(32) NOT NULL,
   GUID VARCHAR(255) NOT NULL,
   FileChecksum VARCHAR(255) NOT NULL,
   SubmitTime DATETIME NOT NULL,
   CompleteTime DATETIME,
   Status VARCHAR(255) DEFAULT 'Active',
   INDEX (Status)
);