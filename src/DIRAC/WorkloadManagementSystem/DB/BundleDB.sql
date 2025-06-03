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
DROP TABLE IF EXISTS `JobToBundle`;
DROP TABLE IF EXISTS `BundlesInfo`;

CREATE TABLE `BundlesInfo` (
    `BundleID`      INT(11) UNSIGNED NOT NULL AUTO_INCREMENT,
    `ProcessorSum`  INT(5) UNSIGNED NOT NULL DEFAULT 0,
    `MaxProcessors` INT(5) UNSIGNED NOT NULL,
    `Site`          VARCHAR(128) NOT NULL,
    `CE`            VARCHAR(128) NOT NULL,
    `Queue`         VARCHAR(128) NOT NULL,
    `CEDict`        TEXT NOT NULL,
    `ExecTemplate`  VARCHAR(25) NOT NULL,
    `TaskID`        INTEGER(11) UNSIGNED,
    `Status`        ENUM('Storing', 'Sent', 'Finalized', 'Failed') NOT NULL DEFAULT 'Storing',
    PRIMARY KEY (BundleID)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ------------------------------------------------------------------------------
CREATE TABLE `JobToBundle` (
    `JobID`             VARCHAR(255) NOT NULL,
    `BundleID`          INTEGER(11) UNSIGNED NOT NULL,
    `ExecutablePath`    VARCHAR(255) NOT NULL,
    `Inputs`            TEXT NOT NULL,
    PRIMARY KEY (`JobID`),
    FOREIGN KEY (`BundleID`) REFERENCES `BundlesInfo`(`BundleID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
