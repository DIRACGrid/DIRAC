
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/LoggingSystem/DB/SystemLoggingDB.sql,v 1.13 2009/10/05 09:41:55 vfernand Exp $
__RCSID__ = "$Id: SystemLoggingDB.sql,v 1.13 2009/10/05 09:41:55 vfernand Exp $"

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
DELETE FROM user WHERE user='Dirac';

--
-- Must set passwords for database user by replacing "must_be_set".
--

GRANT SELECT,INSERT,LOCK TABLES,UPDATE,DELETE,CREATE,DROP,ALTER ON SystemLoggingDB.* TO Dirac@localhost IDENTIFIED BY 'must_be_set';
GRANT SELECT,INSERT,LOCK TABLES,UPDATE,DELETE,CREATE,DROP,ALTER ON SystemLoggingDB.* TO Dirac@'%' IDENTIFIED BY 'must_be_set';

FLUSH PRIVILEGES;

-- -----------------------------------------------------
-- Table `SystemLoggingDB_n`.`UserDNs`
-- -----------------------------------------------------
CREATE  TABLE IF NOT EXISTS `UserDNs` (
  `UserDNID` INT NOT NULL AUTO_INCREMENT ,
  `OwnerDN` VARCHAR(255) NOT NULL DEFAULT 'unknown' ,
  `OwnerGroup` VARCHAR(128) NOT NULL DEFAULT 'nogroup' ,
  PRIMARY KEY (`UserDNID`) ) ENGINE=InnoDB;


-- -----------------------------------------------------
-- Table `Sites`
-- -----------------------------------------------------
CREATE  TABLE IF NOT EXISTS `Sites` (
  `SiteID` INT NOT NULL AUTO_INCREMENT ,
  `SiteName` VARCHAR(64) NOT NULL DEFAULT 'Unknown' ,
  PRIMARY KEY (`SiteID`) ) ENGINE=InnoDB;


-- -----------------------------------------------------
-- Table `ClientIPs`
-- -----------------------------------------------------
CREATE  TABLE IF NOT EXISTS `ClientIPs` (
  `ClientIPNumberID` INT NOT NULL AUTO_INCREMENT ,
  `ClientIPNumberString` VARCHAR(15) NOT NULL DEFAULT '0.0.0.0' ,
  `ClientFQDN` VARCHAR(128) NOT NULL DEFAULT 'unknown' ,
  `SiteID` INT NOT NULL ,
  PRIMARY KEY (`ClientIPNumberID`, `SiteID`) ,
  INDEX `SiteID` (`SiteID` ASC) ,
    FOREIGN KEY (`SiteID` ) REFERENCES Sites(SiteID) ON UPDATE CASCADE ON DELETE CASCADE ) ENGINE=InnoDB;


-- -----------------------------------------------------
-- Table `SubSystems`
-- -----------------------------------------------------
CREATE  TABLE IF NOT EXISTS `SubSystems` (
  `SubSystemID` INT NOT NULL AUTO_INCREMENT ,
  `SubSystemName` VARCHAR(128) NOT NULL DEFAULT 'Unknown' ,
  PRIMARY KEY (`SubSystemID`) ) ENGINE=InnoDB;


-- -----------------------------------------------------
-- Table `Systems`
-- -----------------------------------------------------
CREATE  TABLE IF NOT EXISTS `Systems` (
  `SystemID` INT NOT NULL AUTO_INCREMENT ,
  `SystemName` VARCHAR(128) NOT NULL DEFAULT 'Unknown' ,
  `SubSystemID` INT NOT NULL ,
  PRIMARY KEY (`SystemID`, `SubSystemID`) ,
  INDEX `SubSystemID` (`SubSystemID` ASC) ,
    FOREIGN KEY (`SubSystemID`) REFERENCES SubSystems(SubSystemID) ON UPDATE CASCADE ON DELETE CASCADE) 
    ENGINE=InnoDB;


-- -----------------------------------------------------
-- Table `FixedTextMessages`
-- -----------------------------------------------------
CREATE  TABLE IF NOT EXISTS `FixedTextMessages` (
  `FixedTextID` INT NOT NULL AUTO_INCREMENT ,
  `FixedTextString` VARCHAR(255) NOT NULL DEFAULT 'Unknown' ,
  `ReviewedMessage` TINYINT(1) NOT NULL DEFAULT FALSE ,
  `SystemID` INT NOT NULL ,
  PRIMARY KEY (`FixedTextID`, `SystemID`) ,
  INDEX `SystemID` (`SystemID` ASC) ,
    FOREIGN KEY (`SystemID` ) REFERENCES Systems(`SystemID`) ON UPDATE CASCADE ON DELETE CASCADE) ENGINE=InnoDB;


-- -----------------------------------------------------
-- Table `MessageRepository`
-- -----------------------------------------------------
CREATE  TABLE IF NOT EXISTS `MessageRepository` (
  `MessageID` INT NOT NULL AUTO_INCREMENT ,
  `MessageTime` DATETIME NOT NULL ,
  `VariableText` VARCHAR(255) NOT NULL ,
  `UserDNID` INT NOT NULL ,
  `ClientIPNumberID` INT NOT NULL ,
  `LogLevel` VARCHAR(6) NOT NULL ,
  `FixedTextID` INT NOT NULL ,
  PRIMARY KEY (`MessageID`) ,
  INDEX `TimeStampsIDX` (`MessageTime` ASC) ,
  INDEX `FixTextIDX` (`FixedTextID` ASC) ,
  INDEX `UserIDX` (`UserDNID` ASC) ,
  INDEX `IPsIDX` (`ClientIPNumberID` ASC) ,
    FOREIGN KEY (`UserDNID` ) REFERENCES UserDNs(`UserDNID` ) ON UPDATE CASCADE ON DELETE CASCADE,
    FOREIGN KEY (`ClientIPNumberID` ) REFERENCES ClientIPs(`ClientIPNumberID` ) ON UPDATE CASCADE ON DELETE CASCADE, 
    FOREIGN KEY (`FixedTextID` ) REFERENCES FixedTextMessages (`FixedTextID` ) ON UPDATE CASCADE ON DELETE CASCADE )
    ENGINE=InnoDB;


-- -----------------------------------------------------
-- Table `AgentPersistentData`ON UPDATE CASCADE ON DELETE CASCADE
-- -----------------------------------------------------
CREATE  TABLE IF NOT EXISTS `AgentPersistentData` (
  `AgentID` INT NOT NULL AUTO_INCREMENT ,
  `AgentName` VARCHAR(64) NOT NULL DEFAULT 'unkwown' ,
  `AgentData` VARCHAR(512) NULL DEFAULT NULL ,
  PRIMARY KEY (`AgentID`) ) ENGINE=InnoDB;
