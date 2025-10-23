-- When installing via dirac tools, the following is not needed (still here for reference)
--
-- DROP DATABASE IF EXISTS BundleDB;
-- CREATE DATABASE BundleDB;
-- ------------------------------------------------------------------------------
-- Database owner definition
-- USE mysql;
--
-- Must set passwords for database user by replacing "must_be_set".
--
-- GRANT SELECT,INSERT,LOCK TABLES,UPDATE,DELETE,CREATE,DROP,ALTER,REFERENCES ON BundleDB.* TO Dirac@'%' IDENTIFIED BY 'must_be_set';
-- FLUSH PRIVILEGES;

USE BundleDB;

-- ------------------------------------------------------------------------------
DROP TABLE IF EXISTS `JobInputs`;
DROP TABLE IF EXISTS `JobToBundle`;
DROP TABLE IF EXISTS `BundlesInfo`;

CREATE TABLE `BundlesInfo` (
    `BundleID`          VARCHAR(32) NOT NULL,
    `ProcessorSum`      INT(5) UNSIGNED NOT NULL DEFAULT 0,
    `MaxProcessors`     INT(5) UNSIGNED NOT NULL,
    `Site`              VARCHAR(128) NOT NULL,
    `CE`                VARCHAR(128) NOT NULL,
    `Queue`             VARCHAR(128) NOT NULL,
    `CEDict`            TEXT NOT NULL,
    `ExecTemplate`      VARCHAR(25) NOT NULL,
    `TaskID`            VARCHAR(255),
    `Status`            ENUM('Waiting', 'Running', 'Done', 'Failed') NOT NULL DEFAULT 'Waiting',
    `ProxyPath`         VARCHAR(255),
    `Flags`             SET('Cleaned', 'Purged') NOT NULL DEFAULT '',
    `FirstTimestamp`    DATETIME,
    `LastTimestamp`     DATETIME,
    PRIMARY KEY (`BundleID`),
    INDEX (`Site`,`CE`,`Queue`),
    INDEX (`Status`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ------------------------------------------------------------------------------
CREATE TABLE `JobToBundle` (
    `JobID`             VARCHAR(255) NOT NULL,
    `BundleID`          VARCHAR(32) NOT NULL,
    `DiracID`           INTEGER,
    `ExecutablePath`    VARCHAR(255) NOT NULL,
    `Outputs`           VARCHAR(255) NOT NULL,
    `Processors`        INT(5) UNSIGNED NOT NULL DEFAULT 1,
    PRIMARY KEY (`JobID`),
    FOREIGN KEY (`BundleID`) REFERENCES `BundlesInfo`(`BundleID`),
    INDEX (`DiracID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ------------------------------------------------------------------------------
CREATE TABLE `JobInputs` (
    `InputID`           INTEGER NOT NULL AUTO_INCREMENT,
    `JobID`             VARCHAR(255) NOT NULL,
    `InputPath`         VARCHAR(255) NOT NULL,
    PRIMARY KEY (`InputID`),
    FOREIGN KEY (`JobID`) REFERENCES `JobToBundle`(`JobID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
