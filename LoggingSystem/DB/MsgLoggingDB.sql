-- $Header $

--------------------------------------------------------------------------------
--
--  Schema definition for the LoggingDB database - containing log messages
--  from DIRAC services and processes
---
--------------------------------------------------------------------------------

DROP DATABASE IF EXISTS MsgLoggingDB;

CREATE DATABASE MsgLoggingDB;

--------------------------------------------------------------------------------
-- Database owner definition

USE mysql;
DELETE FROM user WHERE user='Dirac';

--
-- Must set passwords for database user by replacing "must_be_set".
--

GRANT SELECT,INSERT,LOCK TABLES,UPDATE,DELETE,CREATE,DROP,ALTER ON MsgLoggingDB.* TO Dirac@localhost IDENTIFIED BY 'lhcbMySQL';
GRANT SELECT,INSERT,LOCK TABLES,UPDATE,DELETE,CREATE,DROP,ALTER ON MsgLoggingDB.* TO Dirac@loggerhost IDENTIFIED BY 'lhcbMySQL';

FLUSH PRIVILEGES;

------------------------------------------------------------------------------- 
USE MsgLoggingDB;

--------------------------------------------------------------------------------
DROP TABLE IF EXISTS DateStamps;
CREATE TABLE DateStamps (
--    TimeID INTEGER NOT NULL AUTO_INCREMENT,
    MsgTime DATETIME NOT NULL,
    VartxtString VARCHAR(255) NOT NULL,
    UserDNID INTEGER NOT NULL,
    ClientIPNumberID INTEGER NOT NULL,
--    ClientIPNumber INTEGER NOT NULL DEFAULT 'FFFFFFFF',
    LogLevel INTEGER NOT NULL,
    FixtxtID INTEGER NOT NULL,
    SystemID INTEGER NOT NULL,
    SubSystemID INTEGER NOT NULL,
    FrameID INTEGER NOT NULL,
    FOREIGN KEY ( UserDNID ) REFERENCES UserDNs( UserDNID ),
    FOREIGN KEY ( ClientIPNumberID ) REFERENCES ClientIPs( ClientIPNumberID ),
    FOREIGN KEY ( LogLevel ) REFERENCES LogLevels( LogLevel ),
    FOREIGN KEY ( FixtxtID ) REFERENCES FixtxtmsgTable( FixtxtID ),
    FOREIGN KEY ( SystemID ) REFERENCES System( SystemID ),
    FOREIGN KEY ( SubSystemID ) REFERENCES SubSystem( SubSystemID ),
    FOREIGN KEY ( FrameID ) REFERENCES Frame( FrameID ),
    PRIMARY KEY ( MsgTime, UserDNID, ClientIPNumberID, LogLevel, FixtxtID, SystemID, SubSystemID, FrameID )
);

--------------------------------------------------------------------------------
DROP TABLE IF EXISTS UserDNs;
CREATE TABLE UserDNs (
    UserDNID INTEGER NOT NULL AUTO_INCREMENT,
--    Owner VARCHAR(32) NOT NULL DEFAULT 'noname',
    OwnerDN VARCHAR(255) NOT NULL DEFAULT 'unknown',
    OwnerGroup VARCHAR(128) NOT NULL DEFAULT 'nogroup',
    PRIMARY KEY ( UserDNID )
);

--------------------------------------------------------------------------------
DROP TABLE IF EXISTS ClientIPs;
CREATE TABLE ClientIPs (
    ClientIPNumberID INTEGER NOT NULL AUTO_INCREMENT,
    ClientIPNumberString VARCHAR(15) NOT NULL DEFAULT '0.0.0.0',
--    DiracSite VARCHAR(25) NOT NULL DEFAULT 'None'
    PRIMARY KEY ( ClientIPNumberID )
);

--------------------------------------------------------------------------------
DROP TABLE IF EXISTS LogLevels;
CREATE TABLE LogLevels (
--    LogLevel ENUM( -30, -20, -10, 0, 10, 20, 30) NOT NULL DEFAULT 30,
    LogLevel INTEGER NOT NULL DEFAULT 30,
    LogLevelName VARCHAR(6) NOT NULL DEFAULT 'ALWAYS',
    PRIMARY KEY ( LogLevel )
);

--------------------------------------------------------------------------------
DROP TABLE IF EXISTS FixtxtmsgTable;
CREATE TABLE FixtxtmsgTable (
    FixtxtID INTEGER NOT NULL AUTO_INCREMENT,
    FixtxtString VARCHAR(255) NOT NULL DEFAULT 'No text',
    PRIMARY KEY ( FixtxtID )
);

--------------------------------------------------------------------------------
DROP TABLE IF EXISTS System;
CREATE TABLE System (
    SystemID INTEGER NOT NULL AUTO_INCREMENT,
    SystemName VARCHAR(128) NOT NULL DEFAULT 'No system',
    PRIMARY KEY ( SystemID )
);

--------------------------------------------------------------------------------
DROP TABLE IF EXISTS SubSystem;
CREATE TABLE SubSystem (
    SubSystemID INTEGER NOT NULL AUTO_INCREMENT,
    SubSystemName VARCHAR(128) NOT NULL DEFAULT 'No subsystem',
    PRIMARY KEY ( SubSystemID )
);

--------------------------------------------------------------------------------
DROP TABLE IF EXISTS Frame;
CREATE TABLE Frame (
    FrameID INTEGER NOT NULL AUTO_INCREMENT,
    FrameName VARCHAR(128) NOT NULL DEFAULT 'No frame',
    PRIMARY KEY ( FrameID )
);

INSERT INTO LogLevels VALUES (30,'ALWAYS');
INSERT INTO LogLevels VALUES (20,'INFO');
INSERT INTO LogLevels VALUES (10,'VERB');
INSERT INTO LogLevels VALUES (0,'DEBUG');
INSERT INTO LogLevels VALUES (-10,'WARN');
INSERT INTO LogLevels VALUES (-20,'ERROR');
--INSERT INTO LogLevels VALUES (-20,'EXCEPTION');
INSERT INTO LogLevels VALUES (-30,'FATAL');
