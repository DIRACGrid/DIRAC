-- $Header $

--------------------------------------------------------------------------------
--
--  Schema definition for the SandboxDB database - containing the job status
--  history ( logging ) information
---
--------------------------------------------------------------------------------

DROP DATABASE IF EXISTS SandboxDB;

CREATE DATABASE SandboxDB;

--------------------------------------------------------------------------------
-- Database owner definition

USE mysql;
DELETE FROM user WHERE user='Dirac';

--
-- Must set passwords for database user by replacing "must_be_set".
--

GRANT SELECT,INSERT,LOCK TABLES,UPDATE,DELETE,CREATE,DROP,ALTER ON SandboxDB.* TO Dirac@localhost IDENTIFIED BY 'must_be_set';
GRANT SELECT,INSERT,LOCK TABLES,UPDATE,DELETE,CREATE,DROP,ALTER ON SandboxDB.* TO Dirac@'%' IDENTIFIED BY 'must_be_set';

FLUSH PRIVILEGES;

------------------------------------------------------------------------------- 
USE SandboxDB;

--------------------------------------------------------------------------------
DROP TABLE IF EXISTS InputSandbox;
CREATE TABLE InputSandbox (
    JobID INTEGER NOT NULL,
    FileName VARCHAR(255) NOT NULL,
    FileBody LONGBLOB NOT NULL,
    PRIMARY KEY (JobID,FileName)
) TYPE=MyISAM MAX_ROWS=150000 AVG_ROW_LENGTH=150000;

--------------------------------------------------------------------------------
DROP TABLE IF EXISTS OutputSandbox;
CREATE TABLE OutputSandbox (
    JobID INTEGER NOT NULL,
    FileName VARCHAR(255) NOT NULL,
    FileBody LONGBLOB NOT NULL,
    PRIMARY KEY (JobID,FileName)
) TYPE=MyISAM MAX_ROWS=150000 AVG_ROW_LENGTH=150000;
