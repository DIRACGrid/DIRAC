-- -----------------------------------------------------------
-- Migration Monitoring DB definition
-- -----------------------------------------------------------

DROP DATABASE IF EXISTS MigrationMonitoringDB;

CREATE DATABASE MigrationMonitoringDB;
--
-- Must set passwords for database user by replacing "must_be_set".
--
-- Create user DIRAC
use mysql;
GRANT SELECT,INSERT,LOCK TABLES,UPDATE,DELETE,CREATE,DROP,ALTER ON MigrationMonitoringDB.* TO 'Dirac'@'localhost' IDENTIFIED BY 'must_be_set';
GRANT SELECT,INSERT,LOCK TABLES,UPDATE,DELETE,CREATE,DROP,ALTER ON MigrationMonitoringDB.* TO 'Dirac'@'%' IDENTIFIED BY 'must_be_set';

FLUSH PRIVILEGES;

-- -----------------------------------------------------------

use MigrationMonitoringDB;

DROP TABLE IF EXISTS Files;
CREATE TABLE Files(
   FileID INTEGER NOT NULL AUTO_INCREMENT,
   Status VARCHAR(32) NOT NULL DEFAULT 'Migrating',
   SE VARCHAR(32) NOT NULL,
   LFN VARCHAR(255) NOT NULL,
   PFN VARCHAR(255) NOT NULL,
   Size INTEGER NOT NULL,
   GUID VARCHAR(255) NOT NULL,
   FileChecksum VARCHAR(255) NOT NULL,
   SubmitTime DATETIME NOT NULL,
   LastUpdate DATETIME,
   INDEX (Status,SE),
);