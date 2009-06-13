-- $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/FrameworkSystem/DB/NotificationDB.sql,v 1.1 2009/06/13 23:21:11 atsareg Exp $

--------------------------------------------------------------------------------
--
--  Schema definition for the NotificationDB database - containing the alarms
--  data
---
--------------------------------------------------------------------------------

DROP DATABASE IF EXISTS NotificationDB;

CREATE DATABASE NotificationDB;

--------------------------------------------------------------------------------
-- Database owner definition

USE mysql;
DELETE FROM user WHERE user='Dirac';

--
-- Must set passwords for database user by replacing "must_be_set".
--

GRANT SELECT,INSERT,LOCK TABLES,UPDATE,DELETE,CREATE,DROP,ALTER ON NotificationDB.* TO Dirac@localhost IDENTIFIED BY 'must_be_set';
GRANT SELECT,INSERT,LOCK TABLES,UPDATE,DELETE,CREATE,DROP,ALTER ON NotificationDB.* TO Dirac@'%' IDENTIFIED BY 'must_be_set';

FLUSH PRIVILEGES;

------------------------------------------------------------------------------- 
USE NotificationDB;

--------------------------------------------------------------------------------
DROP TABLE IF EXISTS Alarms;
CREATE TABLE Alarms (
    AlarmID INTEGER NOT NULL AUTO_INCREMENT,
    AlarmName VARCHAR(32) NOT NULL DEFAULT '',
    AlarmType VARCHAR(32) NOT NULL DEFAULT 'Action',
    AlarmBody BLOB,
    Comment BLOB,
    DestinationGroup VARCHAR(32) NOT NULL DEFAULT '',
    DestinationView VARCHAR(64) NOT NULL DEFAULT '',
    Status VARCHAR(128) NOT NULL DEFAULT '',
    Source VARCHAR(256) NOT NULL DEFAULT '',
    Author VARCHAR(256) NOT NULL DEFAULT 'System', 
    CreationDate DATETIME NOT NULL ,
    StatusDate DATETIME NOT NULL ,
    Action VARCHAR(128) NOT NULL DEFAULT '',
    Primary Key (AlarmID),
    Index (Status),
    Index (DestinationGroup)
);

CREATE TABLE AlarmLogging (
    AlarmID INTEGER NOT NULL,
    Status VARCHAR(128) NOT NULL DEFAULT '',
    Author VARCHAR(256) NOT NULL DEFAULT 'System',
    Action VARCHAR(128) NOT NULL DEFAULT '', 
    Primary Key (AlarmID)
);

