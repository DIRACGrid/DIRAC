# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/LoggingSystem/DB/SystemLoggingDB.sql,v 1.14 2009/10/14 07:40:33 rgracian Exp $
__RCSID__ = "$Id: SystemLoggingDB.sql,v 1.14 2009/10/14 07:40:33 rgracian Exp $"

-- ------------------------------------------------------------------------------
--
--  Schema definition for the SystemLoggingDB_n database - containing log messages
--  from DIRAC services and processes
-- -
-- ------------------------------------------------------------------------------
 
DROP DATABASE IF EXISTS SystemLoggingDB;

CREATE DATABASE SystemLoggingDB;

-- ------------------------------------------------------------------------------
-- Database owner definition

USE mysql;

--
-- Must set passwords for database user by replacing "must_be_set".
--

GRANT SELECT,INSERT,LOCK TABLES,UPDATE,DELETE,CREATE,DROP,ALTER ON SystemLoggingDB.* TO Dirac@localhost IDENTIFIED BY 'must_be_set';
GRANT SELECT,INSERT,LOCK TABLES,UPDATE,DELETE,CREATE,DROP,ALTER ON SystemLoggingDB.* TO Dirac@'%' IDENTIFIED BY 'must_be_set';

FLUSH PRIVILEGES;

USE SystemLoggingDB;

-- -----------------------------------------------------
-- Table `UserDNs`
-- -----------------------------------------------------


-- -----------------------------------------------------
-- Table `Sites`
-- -----------------------------------------------------

-- -----------------------------------------------------
-- Table `ClientIPs`
-- -----------------------------------------------------

-- -----------------------------------------------------
-- Table `SubSystems`
-- -----------------------------------------------------

-- -----------------------------------------------------
-- Table `Systems`
-- -----------------------------------------------------


-- -----------------------------------------------------
-- Table `FixedTextMessages`
-- -----------------------------------------------------

-- -----------------------------------------------------
-- Table `MessageRepository`
-- -----------------------------------------------------

-- -----------------------------------------------------
-- Table `AgentPersistentData`ON UPDATE CASCADE ON DELETE CASCADE
-- -----------------------------------------------------
