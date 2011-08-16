-- -----------------------------------------------------------
-- Resource Status database definition
-- -----------------------------------------------------------

DROP DATABASE IF EXISTS ResourceStatusDB;

CREATE DATABASE ResourceStatusDB;
--
-- Must set passwords for database user by replacing "must_be_set".
--
-- Create user DIRAC
USE mysql;
GRANT SELECT,INSERT,LOCK TABLES,UPDATE,DELETE,CREATE,DROP,ALTER ON ResourceStatusDB.* TO 'Dirac'@'localhost' IDENTIFIED BY 'must_be_set';
GRANT SELECT,INSERT,LOCK TABLES,UPDATE,DELETE,CREATE,DROP,ALTER ON ResourceStatusDB.* TO 'Dirac'@'%' IDENTIFIED BY 'must_be_set';

FLUSH PRIVILEGES;

-- -----------------------------------------------------------

USE ResourceStatusDB;

DROP TABLE IF EXISTS SiteTypes;
CREATE TABLE SiteTypes(
  SiteType VARCHAR(4) NOT NULL,
  Description BLOB,
  PRIMARY KEY(SiteType)
) Engine=InnoDB;

DROP TABLE IF EXISTS ServiceTypes;
CREATE TABLE ServiceTypes(
  ServiceType VARCHAR(32) NOT NULL,
  Description BLOB,
  PRIMARY KEY(ServiceType)
) Engine=InnoDB;

DROP TABLE IF EXISTS ResourceTypes;
CREATE TABLE ResourceTypes(
  ResourceType VARCHAR(8) NOT NULL,
  Description BLOB,
  PRIMARY KEY(ResourceType)
) Engine=InnoDB;

DROP TABLE IF EXISTS Status;
CREATE TABLE Status(
  Status VARCHAR(8) NOT NULL,
  Description BLOB,
  PRIMARY KEY(Status)
) Engine=InnoDB;

DROP TABLE IF EXISTS Sites;
CREATE TABLE Sites(
  SiteID INT UNSIGNED NOT NULL AUTO_INCREMENT,
  SiteName VARCHAR(64) NOT NULL,
  INDEX (SiteName),
  SiteType VARCHAR(8) NOT NULL,
  GridSiteName VARCHAR(64) NOT NULL,
  INDEX (GridSiteName),
  Status VARCHAR(8) NOT NULL,
  Index(Status),
  Reason VARCHAR(255) NOT NULL DEFAULT 'Unspecified',
  DateCreated DATETIME NOT NULL,
  DateEffective DATETIME NOT NULL,
  DateEnd DATETIME,
  LastCheckTime DATETIME NOT NULL,
  TokenOwner VARCHAR(8) NOT NULL Default 'RS_SVC',
  TokenExpiration DATETIME NOT NULL,
  FOREIGN KEY(SiteType) REFERENCES SiteTypes(SiteType),
  FOREIGN KEY (Status) REFERENCES Status(Status),
  PRIMARY KEY(SiteID)
) Engine=InnoDB;

DROP TABLE IF EXISTS Services;
CREATE TABLE Services(
  ServiceID INT UNSIGNED NOT NULL AUTO_INCREMENT,
  ServiceName VARCHAR(64) NOT NULL,
  INDEX (ServiceName),
  ServiceType VARCHAR(32) NOT NULL,
  INDEX (ServiceType),
  SiteName VARCHAR(64) NOT NULL,
  INDEX (SiteName),
  Status VARCHAR(8) NOT NULL,
  Index(Status),
  Reason VARCHAR(255) NOT NULL DEFAULT 'Unspecified',
  DateCreated DATETIME NOT NULL,
  DateEffective DATETIME NOT NULL,
  DateEnd DATETIME,
  LastCheckTime DATETIME NOT NULL,
  TokenOwner VARCHAR(8) NOT NULL Default 'RS_SVC',
  TokenExpiration DATETIME NOT NULL,
  FOREIGN KEY (ServiceType) REFERENCES ServiceTypes(ServiceType),
  FOREIGN KEY (Status) REFERENCES Status(Status),
  PRIMARY KEY(ServiceID)
) Engine=InnoDB;

DROP TABLE IF EXISTS Resources;
CREATE TABLE Resources(
  ResourceID INT UNSIGNED NOT NULL AUTO_INCREMENT,
  ResourceName VARCHAR(64) NOT NULL,
  INDEX (ResourceName),
  ResourceType VARCHAR(8) NOT NULL,
  ServiceType VARCHAR(32) NOT NULL,
  INDEX (ServiceType),
  SiteName VARCHAR(64),
  INDEX (SiteName),
  GridSiteName VARCHAR(64),
  INDEX (GridSiteName),
  Status VARCHAR(8) NOT NULL,
  INDEX (Status),
  Reason VARCHAR(255) NOT NULL DEFAULT 'Unspecified',
  DateCreated DATETIME NOT NULL,
  DateEffective DATETIME NOT NULL,
  INDEX (DateEffective),
  DateEnd DATETIME,
  LastCheckTime DATETIME NOT NULL,
  TokenOwner VARCHAR(8) NOT NULL Default 'RS_SVC',
  TokenExpiration DATETIME NOT NULL,
  FOREIGN KEY (ServiceType) REFERENCES ServiceTypes(ServiceType),
  FOREIGN KEY (ResourceType) REFERENCES ResourceTypes(ResourceType),
  FOREIGN KEY (Status) REFERENCES Status(Status),
  PRIMARY KEY (ResourceID)
) Engine = InnoDB ;

DROP TABLE IF EXISTS StorageElementsRead;
CREATE TABLE StorageElementsRead(
  StorageElementID INT UNSIGNED NOT NULL AUTO_INCREMENT,
  StorageElementName VARCHAR(64) NOT NULL,
  INDEX (StorageElementName),
  ResourceName VARCHAR(64) NOT NULL,
  INDEX (ResourceName),
  GridSiteName VARCHAR(64),
  INDEX (GridSiteName),
  Status VARCHAR(8) NOT NULL,
  INDEX (Status),
  Reason VARCHAR(255) NOT NULL DEFAULT 'Unspecified',
  DateCreated DATETIME NOT NULL,
  DateEffective DATETIME NOT NULL,
  INDEX (DateEffective),
  DateEnd DATETIME,
  LastCheckTime DATETIME NOT NULL,
  TokenOwner VARCHAR(8) NOT NULL Default 'RS_SVC',
  TokenExpiration DATETIME NOT NULL,
  FOREIGN KEY (Status) REFERENCES Status(Status),
  PRIMARY KEY (StorageElementID)
) Engine = InnoDB ;

DROP TABLE IF EXISTS StorageElementsWrite;
CREATE TABLE StorageElementsWrite(
  StorageElementID INT UNSIGNED NOT NULL AUTO_INCREMENT,
  StorageElementName VARCHAR(64) NOT NULL,
  INDEX (StorageElementName),
  ResourceName VARCHAR(64) NOT NULL,
  INDEX (ResourceName),
  GridSiteName VARCHAR(64),
  INDEX (GridSiteName),
  Status VARCHAR(8) NOT NULL,
  INDEX (Status),
  Reason VARCHAR(255) NOT NULL DEFAULT 'Unspecified',
  DateCreated DATETIME NOT NULL,
  DateEffective DATETIME NOT NULL,
  INDEX (DateEffective),
  DateEnd DATETIME,
  LastCheckTime DATETIME NOT NULL,
  TokenOwner VARCHAR(8) NOT NULL Default 'RS_SVC',
  TokenExpiration DATETIME NOT NULL,
  FOREIGN KEY (Status) REFERENCES Status(Status),
  PRIMARY KEY (StorageElementID)
) Engine = InnoDB ;

DROP TABLE IF EXISTS SitesHistory;
CREATE TABLE SitesHistory(
  SitesHistoryID INT UNSIGNED NOT NULL AUTO_INCREMENT,
  SiteName VARCHAR(64) NOT NULL,
  INDEX (SiteName),
  Status VARCHAR(8) NOT NULL,
  Reason VARCHAR(255) NOT NULL,
  DateCreated DATETIME NOT NULL,
  DateEffective DATETIME NOT NULL,
  DateEnd DATETIME NOT NULL,
  TokenOwner VARCHAR(64),
  PRIMARY KEY(SitesHistoryID)
) Engine = InnoDB ;

DROP TABLE IF EXISTS ServicesHistory;
CREATE TABLE ServicesHistory(
  ServicesHistoryID INT UNSIGNED NOT NULL AUTO_INCREMENT,
  ServiceName VARCHAR(64) NOT NULL,
  INDEX (ServiceName),
  Status VARCHAR(8) NOT NULL,
  Reason VARCHAR(255) NOT NULL,
  DateCreated DATETIME NOT NULL,
  DateEffective DATETIME NOT NULL,
  DateEnd DATETIME,
  TokenOwner VARCHAR(64),
  PRIMARY KEY(ServicesHistoryID)
) Engine=InnoDB;

DROP TABLE IF EXISTS ResourcesHistory;
CREATE TABLE ResourcesHistory(
  ResourcesHistoryID INT UNSIGNED NOT NULL AUTO_INCREMENT,
  ResourceName VARCHAR(64) NOT NULL,
  INDEX (ResourceName),
  Status VARCHAR(8) NOT NULL,
  Reason VARCHAR(255) NOT NULL,
  DateCreated DATETIME NOT NULL,
  DateEffective DATETIME NOT NULL,
  DateEnd DATETIME NOT NULL,
  TokenOwner VARCHAR(64) NOT NULL,
  PRIMARY KEY (ResourcesHistoryID)
) Engine=InnoDB;

DROP TABLE IF EXISTS StorageElementsReadHistory;
CREATE TABLE StorageElementsReadHistory(
  StorageElementsHistoryID INT UNSIGNED NOT NULL AUTO_INCREMENT,
  StorageElementName VARCHAR(64) NOT NULL,
  INDEX (StorageElementName),
  Status VARCHAR(8) NOT NULL,
  Reason VARCHAR(255) NOT NULL,
  DateCreated DATETIME NOT NULL,
  DateEffective DATETIME NOT NULL,
  DateEnd DATETIME NOT NULL,
  TokenOwner VARCHAR(64) NOT NULL,
  PRIMARY KEY (StorageElementsHistoryID)
) Engine=InnoDB;

DROP TABLE IF EXISTS StorageElementsWriteHistory;
CREATE TABLE StorageElementsWriteHistory(
  StorageElementsHistoryID INT UNSIGNED NOT NULL AUTO_INCREMENT,
  StorageElementName VARCHAR(64) NOT NULL,
  INDEX (StorageElementName),
  Status VARCHAR(8) NOT NULL,
  Reason VARCHAR(255) NOT NULL,
  DateCreated DATETIME NOT NULL,
  DateEffective DATETIME NOT NULL,
  DateEnd DATETIME NOT NULL,
  TokenOwner VARCHAR(64) NOT NULL,
  PRIMARY KEY (StorageElementsHistoryID)
) Engine=InnoDB;

DROP TABLE IF EXISTS GridSites;
CREATE TABLE GridSites(
  gsID INT UNSIGNED NOT NULL AUTO_INCREMENT,
  GridSiteName VARCHAR(64) NOT NULL,
  INDEX (GridSiteName),
  GridTier VARCHAR(4) NOT NULL,
  PRIMARY KEY(gsID)
) Engine=InnoDB;

DROP VIEW IF EXISTS PresentStorageElementsRead;
CREATE VIEW PresentStorageElementsRead AS SELECT 
  StorageElementsRead.StorageElementName, 
  StorageElementsRead.ResourceName,
  StorageElementsRead.GridSiteName, 
  GridSites.GridTier AS SiteType,
  StorageElementsRead.Status,
  StorageElementsRead.DateEffective, 
  StorageElementsReadHistory.Status AS FormerStatus,
  StorageElementsRead.Reason,
  StorageElementsRead.LastCheckTime,
  StorageElementsRead.TokenOwner,
  StorageElementsRead.TokenExpiration
FROM ( 
  (StorageElementsRead INNER JOIN GridSites ON 
   StorageElementsRead.GridSiteName = GridSites.GridSiteName)
    INNER JOIN StorageElementsReadHistory ON 
      StorageElementsRead.StorageElementName = StorageElementsReadHistory.StorageElementName AND 
      StorageElementsRead.DateEffective = StorageElementsReadHistory.DateEnd 
) WHERE StorageElementsRead.DateEffective < UTC_TIMESTAMP()
ORDER BY StorageElementName;

DROP VIEW IF EXISTS PresentStorageElementsWrite;
CREATE VIEW PresentStorageElementsWrite AS SELECT 
  StorageElementsWrite.StorageElementName, 
  StorageElementsWrite.ResourceName,
  StorageElementsWrite.GridSiteName, 
  GridSites.GridTier AS SiteType,
  StorageElementsWrite.Status,
  StorageElementsWrite.DateEffective, 
  StorageElementsWriteHistory.Status AS FormerStatus,
  StorageElementsWrite.Reason,
  StorageElementsWrite.LastCheckTime,
  StorageElementsWrite.TokenOwner,
  StorageElementsWrite.TokenExpiration
FROM ( 
  (StorageElementsWrite INNER JOIN GridSites ON 
   StorageElementsWrite.GridSiteName = GridSites.GridSiteName)
    INNER JOIN StorageElementsWriteHistory ON 
      StorageElementsWrite.StorageElementName = StorageElementsWriteHistory.StorageElementName AND 
      StorageElementsWrite.DateEffective = StorageElementsWriteHistory.DateEnd 
) WHERE StorageElementsWrite.DateEffective < UTC_TIMESTAMP()
ORDER BY StorageElementName;

DROP VIEW IF EXISTS PresentResources;
CREATE VIEW PresentResources AS SELECT 
  Resources.ResourceName, 
  Resources.SiteName, 
  Resources.ServiceType,
  Resources.GridSiteName, 
  GridSites.GridTier AS SiteType, 
  Resources.ResourceType,
  Resources.Status,
  Resources.DateEffective, 
  ResourcesHistory.Status AS FormerStatus,
  Resources.Reason,
  Resources.LastCheckTime,
  Resources.TokenOwner, 
  Resources.TokenExpiration
FROM (
  (Resources INNER JOIN GridSites ON 
   Resources.GridSiteName = GridSites.GridSiteName) 
    INNER JOIN ResourcesHistory ON 
      Resources.ResourceName = ResourcesHistory.ResourceName AND 
      Resources.DateEffective = ResourcesHistory.DateEnd
) WHERE Resources.DateEffective < UTC_TIMESTAMP()
ORDER BY ResourceName;

DROP VIEW IF EXISTS PresentSites;
CREATE VIEW PresentSites AS SELECT 
  Sites.SiteName, 
  Sites.SiteType,
  Sites.GridSiteName,
  GridSites.GridTier,
  Sites.Status,
  Sites.DateEffective, 
  SitesHistory.Status AS FormerStatus,
  Sites.Reason,
  Sites.LastCheckTime,
  Sites.TokenOwner,
  Sites.TokenExpiration
FROM (
  	(Sites INNER JOIN GridSites ON
  	Sites.GridSiteName = GridSites.GridSiteName)
  	INNER JOIN SitesHistory ON 
  	 Sites.SiteName = SitesHistory.SiteName AND 
  	 Sites.DateEffective = SitesHistory.DateEnd 
) WHERE Sites.DateEffective < UTC_TIMESTAMP()
ORDER BY SiteName;

DROP VIEW IF EXISTS PresentServices;
CREATE VIEW PresentServices AS SELECT 
  Services.ServiceName,
  Services.SiteName, 
  Sites.SiteType,
  Services.ServiceType, 
  Services.Status,
  Services.DateEffective, 
  ServicesHistory.Status AS FormerStatus,
  Services.Reason,
  Services.LastCheckTime,
  Services.TokenOwner, 
  Services.TokenExpiration
FROM (
	(Services INNER JOIN Sites ON
	 Services.Sitename = Sites.SiteName)
	 INNER JOIN ServicesHistory ON 
	  Services.ServiceName = ServicesHistory.ServiceName AND 
  	  Services.DateEffective = ServicesHistory.DateEnd 
) WHERE Services.DateEffective < UTC_TIMESTAMP()
ORDER BY ServiceName;
