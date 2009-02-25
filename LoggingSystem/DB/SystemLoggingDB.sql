# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/LoggingSystem/DB/SystemLoggingDB.sql,v 1.10 2009/02/25 16:05:56 mseco Exp $
__RCSID__ = "$Id: SystemLoggingDB.sql,v 1.10 2009/02/25 16:05:56 mseco Exp $"

--------------------------------------------------------------------------------
--
--  Schema definition for the SystemLoggingDB database - containing log messages
--  from DIRAC services and processes
---
--------------------------------------------------------------------------------

DROP DATABASE IF EXISTS SystemLoggingDB;

CREATE DATABASE SystemLoggingDB;

--------------------------------------------------------------------------------
-- Database owner definition

USE mysql;
DELETE FROM user WHERE user='Dirac';

--
-- Must set passwords for database user by replacing "must_be_set".
--

GRANT SELECT,INSERT,LOCK TABLES,UPDATE,DELETE,CREATE,DROP,ALTER ON SystemLoggingDB.* TO Dirac@localhost IDENTIFIED BY 'must_be_set';
GRANT SELECT,INSERT,LOCK TABLES,UPDATE,DELETE,CREATE,DROP,ALTER ON SystemLoggingDB.* TO Dirac@'%' IDENTIFIED BY 'must_be_set';

FLUSH PRIVILEGES;

-------------------------------------------------------------------------------
USE SystemLoggingDB;

--------------------------------------------------------------------------------
DROP TABLE IF EXISTS MessageRepository;
CREATE TABLE MessageRepository (
    MessageID INTEGER NOT NULL AUTO_INCREMENT,
    MessageTime DATETIME NOT NULL,
    VariableText VARCHAR(255) NOT NULL,
    UserDNID INTEGER NOT NULL,
    ClientIPNumberID INTEGER NOT NULL,
    SiteID INTEGER NOT NULL,
    LogLevel VARCHAR(6) NOT NULL,
    FixedTextID INTEGER NOT NULL,
    SystemID INTEGER NOT NULL,
    SubSystemID INTEGER NOT NULL,
    FOREIGN KEY ( UserDNID ) REFERENCES UserDNs( UserDNID ),
    FOREIGN KEY ( ClientIPNumberID ) REFERENCES ClientIPs( ClientIPNumberID ),
    FOREIGN KEY ( FixedTextID ) REFERENCES FixedTextMessages( FixedTextID ),
    FOREIGN KEY ( SystemID ) REFERENCES System( SystemID ),
    FOREIGN KEY ( SubSystemID ) REFERENCES SubSystem( SubSystemID ),
    FOREIGN KEY ( SiteID ) REFERENCES Site( SiteID ),
    PRIMARY KEY ( MessageID ),
    INDEX (SystemID),
    INDEX (SubSystemID),
    INDEX (FixedTextID),
    INDEX (UserDNID),
    INDEX (ClientIPNumberID),
    INDEX (SiteID)
);

--------------------------------------------------------------------------------
DROP TABLE IF EXISTS UserDNs;
CREATE TABLE UserDNs (
    UserDNID INTEGER NOT NULL AUTO_INCREMENT,
    OwnerDN VARCHAR(255) NOT NULL DEFAULT 'unknown',
    OwnerGroup VARCHAR(128) NOT NULL DEFAULT 'nogroup',
    PRIMARY KEY ( UserDNID )
);

--------------------------------------------------------------------------------
DROP TABLE IF EXISTS ClientIPs;
CREATE TABLE ClientIPs (
    ClientIPNumberID INTEGER NOT NULL AUTO_INCREMENT,
    ClientIPNumberString VARCHAR(15) NOT NULL DEFAULT '0.0.0.0',
    ClientFQDN VARCHAR(128) NOT NULL DEFAULT 'unknown',
    PRIMARY KEY ( ClientIPNumberID )
);

--------------------------------------------------------------------------------
DROP TABLE IF EXISTS FixedTextMessages;
CREATE TABLE FixedTextMessages (
    FixedTextID INTEGER NOT NULL AUTO_INCREMENT,
    FixedTextString VARCHAR(255) NOT NULL DEFAULT 'Unknown',
    ReviewedMessage BOOLEAN NOT NULL DEFAULT FALSE,
    PRIMARY KEY ( FixedTextID )
);

--------------------------------------------------------------------------------
DROP TABLE IF EXISTS Systems;
CREATE TABLE Systems (
    SystemID INTEGER NOT NULL AUTO_INCREMENT,
    SystemName VARCHAR(128) NOT NULL DEFAULT 'Unknown',
    PRIMARY KEY ( SystemID )
);

--------------------------------------------------------------------------------
DROP TABLE IF EXISTS SubSystems;
CREATE TABLE SubSystems (
    SubSystemID INTEGER NOT NULL AUTO_INCREMENT,
    SubSystemName VARCHAR(128) NOT NULL DEFAULT 'Unknown',
    PRIMARY KEY ( SubSystemID )
);

--------------------------------------------------------------------------------

DROP TABLE IF EXISTS Sites;
CREATE TABLE Sites (
    SiteID INTEGER NOT NULL AUTO_INCREMENT,
    SiteName VARCHAR(64) NOT NULL DEFAULT 'Unknown',
    PRIMARY KEY ( SiteID )
);

--------------------------------------------------------------------------------

DROP TABLE IF EXISTS AgentPersistentData;
CREATE TABLE AgentPersistentData (
  AgentID INTEGER NOT NULL AUTO_INCREMENT,
  AgentName VARCHAR(64) NOT NULL DEFAULT 'unkwown',
  AgentData VARCHAR(512),
  PRIMARY KEY ( AgentID )
);