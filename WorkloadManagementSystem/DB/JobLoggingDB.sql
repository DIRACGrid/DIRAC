-- $Header $

-- ------------------------------------------------------------------------------
--
--  Schema definition for the JobLoggingDB database - containing the job status
--  history ( logging ) information
-- -
-- ------------------------------------------------------------------------------

DROP DATABASE IF EXISTS JobLoggingDB;

CREATE DATABASE JobLoggingDB;

-- ------------------------------------------------------------------------------
-- Database owner definition

USE mysql;
DELETE FROM user WHERE user='Dirac';

--
-- Must set passwords for database user by replacing "must_be_set".
--

GRANT SELECT,INSERT,LOCK TABLES,UPDATE,DELETE,CREATE,DROP,ALTER ON JobLoggingDB.* TO Dirac@localhost IDENTIFIED BY 'must_be_set';
GRANT SELECT,INSERT,LOCK TABLES,UPDATE,DELETE,CREATE,DROP,ALTER ON JobLoggingDB.* TO Dirac@'%' IDENTIFIED BY 'must_be_set';

FLUSH PRIVILEGES;

-- ----------------------------------------------------------------------------- 
USE JobLoggingDB;

-- ------------------------------------------------------------------------------
DROP TABLE IF EXISTS LoggingInfo;
CREATE TABLE LoggingInfo (
    JobID INTEGER NOT NULL,
    Status VARCHAR(32) NOT NULL DEFAULT '',
    MinorStatus VARCHAR(128) NOT NULL DEFAULT '',
    ApplicationStatus varchar(256) NOT NULL DEFAULT '', 
    StatusTime DATETIME NOT NULL ,
    StatusTimeOrder DOUBLE(11,3) NOT NULL,  
    StatusSource VARCHAR(32) NOT NULL DEFAULT 'Unknown',
    INDEX (JobID)
) ENGINE = InnoDB;

