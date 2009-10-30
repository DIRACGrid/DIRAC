-------------------------------------------------------------
-- Stager Service DB definition
-- author: A. Smith
-- date: 17.03.2007
-------------------------------------------------------------

DROP DATABASE IF EXISTS StagerDB;

CREATE DATABASE StagerDB;
--
-- Must set passwords for database user by replacing "must_be_set".
--
-- Create user DIRAC
use mysql;
GRANT SELECT,INSERT,LOCK TABLES,UPDATE,DELETE,CREATE,DROP,ALTER ON StagerDB.* TO 'Dirac'@'localhost' IDENTIFIED BY 'must_be_set';
GRANT SELECT,INSERT,LOCK TABLES,UPDATE,DELETE,CREATE,DROP,ALTER ON StagerDB.* TO 'Dirac'@'%' IDENTIFIED BY 'must_be_set';

FLUSH PRIVILEGES;

-------------------------------------------------------------

use StagerDB;

DROP TABLE IF EXISTS SiteFiles;
CREATE TABLE SiteFiles(
   LFN VARCHAR(255) NOT NULL,
   Site varchar(32) NOT NULL,
   SURL varchar(255) NOT NULL,
   SE varchar(32) NOT NULL,
   StageSubmit DATETIME NOT NULL,
   StageComplete DATETIME NOT NULL,
   Status varchar(32) DEFAULT 'New',
   JobID varchar(32) NOT NULL,
   Retry integer DEFAULT 0,
   Source varchar(32) NOT NULL,
   PRIMARY KEY (LFN, Site,JobID),
   INDEX (Status,Source)
);

DROP TABLE IF EXISTS SiteTiming;
CREATE TABLE SiteTiming(
  Site varchar(32) NOT NULL,
  Command varchar(32) NOT NULL,
  CommTime FLOAT DEFAULT 0.0,
  Files integer NOT NULL,
  Time DATETIME NOT NULL
);

DROP TABLE IF EXISTS StageTimeRepository;
CREATE TABLE StageTimeRepository(
  Site varchar(32) NOT NULL,
  Time DATETIME NOT NULL,
  StageTime FLOAT NOT NULL
);
