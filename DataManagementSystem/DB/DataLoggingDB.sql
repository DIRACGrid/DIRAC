-------------------------------------------------------------
-- RAW Integrity DB definition
-------------------------------------------------------------

DROP DATABASE IF EXISTS DataLoggingDB;

CREATE DATABASE DataLoggingDB;

-- Create user DIRAC
use mysql;
GRANT SELECT,INSERT,LOCK TABLES,UPDATE,DELETE,CREATE,DROP,ALTER ON DataLoggingDB.* TO 'Dirac'@'localhost' IDENTIFIED BY 'lhcbMySQL';
GRANT SELECT,INSERT,LOCK TABLES,UPDATE,DELETE,CREATE,DROP,ALTER ON DataLoggingDB.* TO 'Dirac'@'%' IDENTIFIED BY 'lhcbMySQL';

FLUSH PRIVILEGES;

-------------------------------------------------------------

use DataLoggingDB;

DROP TABLE IF EXISTS DataLoggingInfo;
CREATE TABLE DataLoggingInfo(
   FileID INTEGER NOT NULL AUTO_INCREMENT,
   LFN VARCHAR(255) NOT NULL,
   Status VARCHAR(255) NOT NULL,
   StatusTime DATETIME,
   StatusTimeOrder DOUBLE(11,3) NOT NULL,  
   Source VARCHAR(127) NOT NULL DEFAULT 'Unknown',
   PRIMARY KEY (FileID),
   INDEX (LFN)
);
