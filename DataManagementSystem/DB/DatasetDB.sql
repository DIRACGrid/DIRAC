DROP DATABASE IF EXISTS DatasetDB;
CREATE DATABASE DatasetDB;
--
-- Must set passwords for database user by replacing "must_be_set".
--
use mysql;
GRANT SELECT,INSERT,LOCK TABLES,UPDATE,DELETE,CREATE,DROP,ALTER ON DatasetDB.* TO 'Dirac'@'localhost' IDENTIFIED BY 'must_be_set';
GRANT SELECT,INSERT,LOCK TABLES,UPDATE,DELETE,CREATE,DROP,ALTER ON DatasetDB.* TO 'Dirac'@'%' IDENTIFIED BY 'must_be_set';

USE DatasetDB;

DROP TABLE IF EXISTS Datasets;
CREATE TABLE Datasets(
    DatasetID INTEGER NOT NULL AUTO_INCREMENT,
    DatasetHandle VARCHAR(255) NOT NULL,
    Description VARCHAR(255),
    LongDescription  BLOB,
    CreationDate DATETIME,
    AuthorDN VARCHAR(255) NOT NULL,
    AuthorGroup VARCHAR(255) NOT NULL,
    Type CHAR(16) DEFAULT 'Production',
    Status  CHAR(16) DEFAULT 'New',
    PRIMARY KEY(DatasetID)
) ENGINE=InnoDB;

DROP TABLE IF EXISTS DatasetParameters;
CREATE TABLE DatasetParameters (
    DatasetID INTEGER NOT NULL,
    ParameterName VARCHAR(32) NOT NULL,
    ParameterValue VARCHAR(255) NOT NULL,
    PRIMARY KEY(DatasetID,ParameterName,ParameterValue)
);

DROP TABLE IF EXISTS DatasetLog;
CREATE TABLE DatasetLog (
    DatasetID INTEGER NOT NULL,
    Message VARCHAR(255) NOT NULL,
    Author VARCHAR(255) NOT NULL DEFAULT "Unknown",
    MessageDate DATETIME NOT NULL,
    INDEX (DatasetID)
);
