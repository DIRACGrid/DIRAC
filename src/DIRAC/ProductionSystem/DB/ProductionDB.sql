-- -------------------------------------------------------------------------------
-- Schema definition for the ProductionDB database a generic

-- When installing via dirac tools, the following is not needed(still here for reference)
--
-- DROP DATABASE IF EXISTS ProductionDB;
-- CREATE DATABASE ProductionDB;
-- ------------------------------------------------------------------------------
-- Database owner definition
-- USE mysql;
-- Must set passwords for database user by replacing "must_be_set".
-- GRANT SELECT, INSERT, LOCK TABLES, UPDATE, DELETE, CREATE, DROP, ALTER, REFERENCES ON ProductionDB.* TO Dirac@'%' IDENTIFIED BY 'must_be_set';
-- FLUSH PRIVILEGES;

-- -----------------------------------------------------------------------------
USE ProductionDB;

SET FOREIGN_KEY_CHECKS = 0;

-- -------------------------------------------------------------------------------
DROP TABLE IF EXISTS Productions;
CREATE TABLE Productions(
    ProductionID INTEGER NOT NULL AUTO_INCREMENT,
    ProductionName VARCHAR(255) NOT NULL,
    Description LONGTEXT,
    CreationDate DATETIME,
    LastUpdate DATETIME,
    Author VARCHAR(255) NOT NULL,
    AuthorDN VARCHAR(255) DEFAULT NULL,
    AuthorGroup VARCHAR(255) NOT NULL,
    Status  CHAR(32) DEFAULT 'New',
    PRIMARY KEY(ProductionID),
    INDEX(ProductionName)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;

-- -------------------------------------------------------------------------------
DROP TABLE IF EXISTS ProductionSteps;
CREATE TABLE ProductionSteps(
    StepID INTEGER NOT NULL AUTO_INCREMENT,
    Name VARCHAR(255),
    Description VARCHAR(255),
    LongDescription TEXT,
    Body LONGTEXT,
    Type CHAR(32) DEFAULT 'Simulation',
    Plugin CHAR(32) DEFAULT 'None',
    AgentType CHAR(32) DEFAULT 'Manual',
    GroupSize INT NOT NULL DEFAULT 1,
    InputQuery LONGTEXT,
    OutputQuery LONGTEXT,
    LastUpdate DATETIME,
    InsertedTime DATETIME,
    PRIMARY KEY(StepID),
    UNIQUE INDEX(StepID)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;

-- -------------------------------------------------------------------------------
DROP TABLE IF EXISTS ProductionTransformations;
CREATE TABLE ProductionTransformations(
    ProductionID INTEGER NOT NULL,
    TransformationID INTEGER NOT NULL,
    LastUpdate DATETIME,
    InsertedTime DATETIME,
    PRIMARY KEY(ProductionID, TransformationID),
    UNIQUE INDEX(TransformationID),
    FOREIGN KEY(ProductionID) REFERENCES Productions(ProductionID)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;

-- -------------------------------------------------------------------------------
DROP TABLE IF EXISTS ProductionTransformationLinks;
CREATE TABLE ProductionTransformationLinks(
    TransformationID INTEGER NOT NULL,
    ParentTransformationID INTEGER DEFAULT -1,
    ProductionID INTEGER NOT NULL,
    PRIMARY KEY(ProductionID, TransformationID, ParentTransformationID),
    INDEX(TransformationID),
    FOREIGN KEY(ProductionID) REFERENCES Productions(ProductionID),
    FOREIGN KEY(TransformationID) REFERENCES ProductionTransformations(TransformationID)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;

SET FOREIGN_KEY_CHECKS = 1;
