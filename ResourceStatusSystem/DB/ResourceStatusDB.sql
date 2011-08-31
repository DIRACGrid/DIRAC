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

--
-- GRID TABLES
--

DROP TABLE IF EXISTS GridSites;
CREATE TABLE GridSites(
  gsID INT UNSIGNED NOT NULL AUTO_INCREMENT,
  GridSiteName VARCHAR(64) NOT NULL,
  INDEX (GridSiteName),
  GridTier VARCHAR(4) NOT NULL,
  PRIMARY KEY(gsID)
) Engine=InnoDB;

--
-- SITES TABLES
--

DROP TABLE IF EXISTS Sites;
CREATE TABLE Sites(
  SiteID INT UNSIGNED NOT NULL AUTO_INCREMENT,
  SiteName VARCHAR(64) NOT NULL,
  INDEX (SiteName),
  SiteType VARCHAR(8) NOT NULL,
  GridSiteName VARCHAR(64) NOT NULL,
  INDEX (GridSiteName),
  PRIMARY KEY(SiteID)
) Engine=InnoDB;

DROP TABLE IF EXISTS SitesStatus;
CREATE TABLE SitesStatus(
  SiteStatusID INT UNSIGNED NOT NULL AUTO_INCREMENT,
  SiteName VARCHAR(64) NOT NULL,
  INDEX (SiteName),
  StatusType VARCHAR(16) NOT NULL DEFAULT '',
  Index(StatusType), 
  Status VARCHAR(8) NOT NULL,
  INDEX (Status),
  Reason VARCHAR(255) NOT NULL DEFAULT 'Unspecified',
  DateCreated DATETIME NOT NULL,
  DateEffective DATETIME NOT NULL,
  INDEX (DateEffective),
  DateEnd DATETIME NOT NULL,
  LastCheckTime DATETIME NOT NULL,
  TokenOwner VARCHAR(8) NOT NULL DEFAULT 'RS_SVC',
  TokenExpiration DATETIME NOT NULL,
  FOREIGN KEY (SiteName) REFERENCES Sites(SiteName),
  PRIMARY KEY (SiteStatusID)
) Engine = InnoDB;

DROP TABLE IF EXISTS SitesHistory;
CREATE TABLE SitesHistory(
  SitesHistoryID INT UNSIGNED NOT NULL AUTO_INCREMENT,
  SiteName VARCHAR(64) NOT NULL,
  INDEX (SiteName),
  StatusType VARCHAR(16) NOT NULL DEFAULT '',
  INDEX (StatusType),
  Status VARCHAR(8) NOT NULL,
  Reason VARCHAR(255) NOT NULL DEFAULT 'Unspecified',
  DateCreated DATETIME NOT NULL,
  DateEffective DATETIME NOT NULL,
  DateEnd DATETIME NOT NULL,
  TokenOwner VARCHAR(64) NOT NULL DEFAULT 'RS_SVC',
  FOREIGN KEY (SiteName) REFERENCES Sites(SiteName),
  PRIMARY KEY(SitesHistoryID)
) Engine = InnoDB;

DROP VIEW IF EXISTS PresentSites;
CREATE VIEW PresentSites AS SELECT 
  Sites.SiteName, 
  Sites.SiteType,
  Sites.GridSiteName,
  GridSites.GridTier,
  SitesStatus.StatusType,
  SitesStatus.Status,
  SitesStatus.DateEffective, 
  SitesStatus.Reason,
  SitesStatus.LastCheckTime,
  SitesStatus.TokenOwner,
  SitesStatus.TokenExpiration,
  SitesHistory.Status AS FormerStatus
FROM ( 
  (
    ( Sites 
        INNER JOIN GridSites ON 
          Sites.GridSiteName = GridSites.GridSiteName
    )
    INNER JOIN SitesHistory ON 
      Sites.SiteName = SitesHistory.SiteName
  )
  INNER JOIN SitesStatus ON
    Sites.SiteName = SitesStatus.SiteName AND
    SitesHistory.DateEnd = SitesStatus.DateEffective AND
    SitesHistory.StatusType = SitesStatus.StatusType 
) WHERE SitesStatus.DateEffective < UTC_TIMESTAMP()
ORDER BY SiteName, DateEffective;

--
-- SERVICES TABLES
--

DROP TABLE IF EXISTS Services;
CREATE TABLE Services(
  ServiceID INT UNSIGNED NOT NULL AUTO_INCREMENT,
  ServiceName VARCHAR(64) NOT NULL,
  INDEX (ServiceName),
  ServiceType VARCHAR(32) NOT NULL,
  INDEX (ServiceType),
  SiteName VARCHAR(64) NOT NULL,
  INDEX (SiteName),
  PRIMARY KEY(ServiceID)
) Engine=InnoDB;

DROP TABLE IF EXISTS ServicesStatus;
CREATE TABLE ServicesStatus(
  ServiceStatusID INT UNSIGNED NOT NULL AUTO_INCREMENT,
  ServiceName VARCHAR(64) NOT NULL,
  INDEX (ServiceName),
  StatusType VARCHAR(16) NOT NULL DEFAULT '',
  Index(StatusType), 
  Status VARCHAR(8) NOT NULL,
  INDEX (Status),
  Reason VARCHAR(255) NOT NULL DEFAULT 'Unspecified',
  DateCreated DATETIME NOT NULL,
  DateEffective DATETIME NOT NULL,
  INDEX (DateEffective),
  DateEnd DATETIME NOT NULL,
  LastCheckTime DATETIME NOT NULL,
  TokenOwner VARCHAR(8) NOT NULL DEFAULT 'RS_SVC',
  TokenExpiration DATETIME NOT NULL,
  FOREIGN KEY (ServiceName) REFERENCES Services(ServiceName),
  PRIMARY KEY (ServiceStatusID)
) Engine = InnoDB;

DROP TABLE IF EXISTS ServicesHistory;
CREATE TABLE ServicesHistory(
  ServicesHistoryID INT UNSIGNED NOT NULL AUTO_INCREMENT,
  ServiceName VARCHAR(64) NOT NULL,
  INDEX (ServiceName),
  StatusType VARCHAR(16) NOT NULL DEFAULT '',
  INDEX (StatusType),
  Status VARCHAR(8) NOT NULL,
  Reason VARCHAR(255) NOT NULL DEFAULT 'Unspecified',
  DateCreated DATETIME NOT NULL,
  DateEffective DATETIME NOT NULL,
  DateEnd DATETIME NOT NULL,
  TokenOwner VARCHAR(64) NOT NULL DEFAULT 'RS_SVC',
  FOREIGN KEY (ServiceName) REFERENCES Services(ServiceName),
  PRIMARY KEY(ServicesHistoryID)
) Engine=InnoDB;

DROP VIEW IF EXISTS PresentServices;
CREATE VIEW PresentServices AS SELECT 
  Services.ServiceName,
  Services.SiteName, 
  Sites.SiteType,
  Services.ServiceType, 
  ServicesStatus.StatusType,
  ServicesStatus.Status,
  ServicesStatus.DateEffective, 
  ServicesStatus.Reason,
  ServicesStatus.LastCheckTime,
  ServicesStatus.TokenOwner, 
  ServicesStatus.TokenExpiration,
  ServicesHistory.Status AS FormerStatus
  FROM ( 
  (
    ( Services 
        INNER JOIN Sites ON 
          Services.SiteName = Sites.SiteName
    )
    INNER JOIN ServicesHistory ON 
      Services.ServiceName = ServicesHistory.ServiceName
  )
  INNER JOIN ServicesStatus ON
    Services.ServiceName = ServicesStatus.ServiceName AND
    ServicesHistory.DateEnd = ServicesStatus.DateEffective AND
    ServicesHistory.StatusType = ServicesStatus.StatusType 
) WHERE ServicesStatus.DateEffective < UTC_TIMESTAMP()
ORDER BY ServiceName, DateEffective;

--
-- RESOURCES TABLES
--

DROP TABLE IF EXISTS Resources;
CREATE TABLE Resources(
  ResourceID INT UNSIGNED NOT NULL AUTO_INCREMENT,
  ResourceName VARCHAR(64) NOT NULL,
  INDEX (ResourceName),
  ResourceType VARCHAR(8) NOT NULL,
  ServiceType VARCHAR(32) NOT NULL,
  INDEX (ServiceType),
  SiteName VARCHAR(64) NOT NULL,
  INDEX (SiteName),
  GridSiteName VARCHAR(64) NOT NULL,
  INDEX (GridSiteName),
  PRIMARY KEY (ResourceID)
) Engine = InnoDB;

DROP TABLE IF EXISTS ResourcesStatus;
CREATE TABLE ResourcesStatus(
  ResourceStatusID INT UNSIGNED NOT NULL AUTO_INCREMENT,
  ResourceName VARCHAR(64) NOT NULL,
  INDEX (ResourceName),
  StatusType VARCHAR(16) NOT NULL DEFAULT '',
  Index(StatusType), 
  Status VARCHAR(8) NOT NULL,
  INDEX (Status),
  Reason VARCHAR(255) NOT NULL DEFAULT 'Unspecified',
  DateCreated DATETIME NOT NULL,
  DateEffective DATETIME NOT NULL,
  INDEX (DateEffective),
  DateEnd DATETIME NOT NULL,
  LastCheckTime DATETIME NOT NULL,
  TokenOwner VARCHAR(8) NOT NULL DEFAULT 'RS_SVC',
  TokenExpiration DATETIME NOT NULL,
  FOREIGN KEY (ResourceName) REFERENCES Resources(ResourceName),
  PRIMARY KEY (ResourceStatusID)
) Engine = InnoDB;

DROP TABLE IF EXISTS ResourcesHistory;
CREATE TABLE ResourcesHistory(
  ResourcesHistoryID INT UNSIGNED NOT NULL AUTO_INCREMENT,
  ResourceName VARCHAR(64) NOT NULL,
  INDEX (ResourceName),
  StatusType VARCHAR(16) NOT NULL DEFAULT '',
  INDEX (StatusType),
  Status VARCHAR(8) NOT NULL,
  Reason VARCHAR(255) NOT NULL DEFAULT 'Unspecified',
  DateCreated DATETIME NOT NULL,
  DateEffective DATETIME NOT NULL,
  DateEnd DATETIME NOT NULL,
  TokenOwner VARCHAR(64) NOT NULL DEFAULT 'RS_SVC',
  FOREIGN KEY (ResourceName) REFERENCES Resources(ResourceName),
  PRIMARY KEY (ResourcesHistoryID)
) Engine=InnoDB;

DROP VIEW IF EXISTS PresentResources;
CREATE VIEW PresentResources AS SELECT 
  Resources.ResourceName, 
  Resources.SiteName, 
  Resources.ServiceType,
  Resources.GridSiteName, 
  GridSites.GridTier AS SiteType, 
  Resources.ResourceType,
  ResourcesStatus.StatusType,
  ResourcesStatus.Status,
  ResourcesStatus.DateEffective, 
  ResourcesStatus.Reason,
  ResourcesStatus.LastCheckTime,
  ResourcesStatus.TokenOwner, 
  ResourcesStatus.TokenExpiration,
  ResourcesHistory.Status AS FormerStatus
FROM (
  (
    ( Resources 
        INNER JOIN GridSites ON 
          Resources.GridSiteName = GridSites.GridSiteName
    )
    INNER JOIN ResourcesHistory ON 
      Resources.ResourceName = ResourcesHistory.ResourceName
  )
  INNER JOIN ResourcesStatus ON
    Resources.ResourceName = ResourcesStatus.ResourceName AND
    ResourcesHistory.DateEnd = ResourcesStatus.DateEffective AND
    ResourcesHistory.StatusType = ResourcesStatus.StatusType 
) WHERE ResourcesStatus.DateEffective < UTC_TIMESTAMP()
ORDER BY ResourceName, DateEffective;

--
-- STORAGE ELEMENTS TABLES
--

DROP TABLE IF EXISTS StorageElements;
CREATE TABLE StorageElements(
  StorageElementName VARCHAR(64) NOT NULL,
  INDEX (StorageElementName),
  ResourceName VARCHAR(64) NOT NULL,
  INDEX (ResourceName),
  GridSiteName VARCHAR(64) NOT NULL,
  INDEX (GridSiteName),
  PRIMARY KEY (StorageElementName)
) Engine = InnoDB;

DROP TABLE IF EXISTS StorageElementsStatus;
CREATE TABLE StorageElementsStatus(
  StorageElementStatusID INT UNSIGNED NOT NULL AUTO_INCREMENT,
  StorageElementName VARCHAR(64) NOT NULL,
  INDEX (StorageElementName),
  StatusType VARCHAR(16) NOT NULL DEFAULT '',
  Index(StatusType), 
  Status VARCHAR(8) NOT NULL,
  INDEX (Status),
  Reason VARCHAR(255) NOT NULL DEFAULT 'Unspecified',
  DateCreated DATETIME NOT NULL,
  DateEffective DATETIME NOT NULL,
  INDEX (DateEffective),
  DateEnd DATETIME NOT NULL,
  LastCheckTime DATETIME NOT NULL,
  TokenOwner VARCHAR(8) NOT NULL Default 'RS_SVC',
  TokenExpiration DATETIME NOT NULL,
  FOREIGN KEY (StorageElementName) REFERENCES StorageElements(StorageElementName),
  PRIMARY KEY (StorageElementStatusID)
) Engine = InnoDB;

DROP TABLE IF EXISTS StorageElementsHistory;
CREATE TABLE StorageElementsHistory(
  StorageElementsHistoryID INT UNSIGNED NOT NULL AUTO_INCREMENT,
  StorageElementName VARCHAR(64) NOT NULL,
  INDEX (StorageElementName),
  StatusType VARCHAR(16) NOT NULL DEFAULT '',
  INDEX (StatusType),
  Status VARCHAR(8) NOT NULL,
  Reason VARCHAR(255) NOT NULL DEFAULT 'Unspecified',
  DateCreated DATETIME NOT NULL,
  DateEffective DATETIME NOT NULL,
  DateEnd DATETIME NOT NULL,
  TokenOwner VARCHAR(64) NOT NULL DEFAULT 'RS_SVC',
  FOREIGN KEY (StorageElementName) REFERENCES StorageElements(StorageElementName),
  PRIMARY KEY (StorageElementsHistoryID)
) Engine=InnoDB;

DROP VIEW IF EXISTS PresentStorageElements;
CREATE VIEW PresentStorageElements AS SELECT 
  StorageElements.StorageElementName, 
  StorageElements.ResourceName,
  StorageElements.GridSiteName, 
  GridSites.GridTier AS SiteType,
  StorageElementsStatus.StatusType,
  StorageElementsStatus.Status,
  StorageElementsStatus.DateEffective, 
  StorageElementsStatus.Reason,
  StorageElementsStatus.LastCheckTime,
  StorageElementsStatus.TokenOwner,
  StorageElementsStatus.TokenExpiration,
  StorageElementsHistory.Status AS FormerStatus
FROM ( 
  (
    ( StorageElements 
        INNER JOIN GridSites ON 
          StorageElements.GridSiteName = GridSites.GridSiteName
    )
    INNER JOIN StorageElementsHistory ON 
      StorageElements.StorageElementName = StorageElementsHistory.StorageElementName
  )
  INNER JOIN StorageElementsStatus ON
    StorageElements.StorageElementName = StorageElementsStatus.StorageElementName AND
    StorageElementsHistory.DateEnd = StorageElementsStatus.DateEffective AND
    StorageElementsHistory.StatusType = StorageElementsStatus.StatusType 
) WHERE StorageElementsStatus.DateEffective < UTC_TIMESTAMP()
ORDER BY StorageElementName, DateEffective;