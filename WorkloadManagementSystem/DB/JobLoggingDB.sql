-- ------------------------------------------------------------------------------
--
--  Schema definition for the JobLoggingDB database - containing the job status
--  history ( logging ) information
-- -
-- ------------------------------------------------------------------------------

-- When installing via dirac tools, the following is not needed (still here for reference)
-- 
-- DROP DATABASE IF EXISTS JobLoggingDB;
-- CREATE DATABASE JobLoggingDB;
-- ------------------------------------------------------------------------------
-- Database owner definition
-- USE mysql;
--
-- Must set passwords for database user by replacing "must_be_set".
--
-- GRANT SELECT,INSERT,LOCK TABLES,UPDATE,DELETE,CREATE,DROP,ALTER ON JobLoggingDB.* TO Dirac@'%' IDENTIFIED BY 'must_be_set';
-- FLUSH PRIVILEGES;

-- ----------------------------------------------------------------------------- 
USE JobLoggingDB;

-- ------------------------------------------------------------------------------
DROP TABLE IF EXISTS LoggingInfo;
CREATE TABLE LoggingInfo (
    JobID INTEGER NOT NULL,
    SeqNum INTEGER NOT NULL DEFAULT 0,
    Status VARCHAR(32) NOT NULL DEFAULT '',
    MinorStatus VARCHAR(128) NOT NULL DEFAULT '',
    ApplicationStatus varchar(255) NOT NULL DEFAULT '', 
    StatusTime DATETIME NOT NULL ,
    StatusTimeOrder DOUBLE(12,3) NOT NULL,  
    StatusSource VARCHAR(32) NOT NULL DEFAULT 'Unknown',
    PRIMARY KEY (JobID, SeqNum)
) ENGINE = InnoDB;

-- -----------------------------------------------------------------------------
--
-- Trigger to manage the new LoggingInfo structure: SeqNum is a sequential number within the same JobID
--    the trigger generates a proper sequence for each JobID
--

CREATE TRIGGER SeqNumGenerator BEFORE INSERT ON LoggingInfo
FOR EACH ROW SET NEW.SeqNum= (SELECT IFNULL(MAX(SeqNum) + 1,1) FROM LoggingInfo WHERE JobID=NEW.JobID);
