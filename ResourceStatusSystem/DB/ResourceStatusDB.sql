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

DROP TABLE IF EXISTS GridSite;
CREATE TABLE GridSite(
  gsID INT UNSIGNED NOT NULL AUTO_INCREMENT,
  GridSiteName VARCHAR(64) NOT NULL,
  INDEX (GridSiteName),
  GridTier VARCHAR(4) NOT NULL,
  MTime DATETIME NOT NULL,
  PRIMARY KEY(gsID)
) Engine=InnoDB;

--
-- SITES TABLES
--

DROP TABLE IF EXISTS Site;
CREATE TABLE Site(
  SiteName VARCHAR(64) NOT NULL,
  SiteType VARCHAR(8) NOT NULL,
  GridSiteName VARCHAR(64) NOT NULL,
  INDEX (GridSiteName),
  MTime DATETIME NOT NULL,
  PRIMARY KEY(SiteName)
) Engine=InnoDB;

DROP TABLE IF EXISTS SiteStatus;
CREATE TABLE SiteStatus(
  SiteStatusID INT UNSIGNED NOT NULL AUTO_INCREMENT,
  SiteName VARCHAR(64) NOT NULL,
  INDEX (SiteName),
  StatusType VARCHAR(16) NOT NULL DEFAULT '',
  INDEX (StatusType),
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
  FOREIGN KEY (SiteName) REFERENCES Site(SiteName),
  UNIQUE KEY(SiteName,StatusType),
  PRIMARY KEY (SiteStatusID)
) Engine = InnoDB;

DROP TABLE IF EXISTS SiteScheduledStatus;
CREATE TABLE SiteScheduledStatus(
  SiteScheduledStatusID INT UNSIGNED NOT NULL AUTO_INCREMENT,
  SiteName VARCHAR(64) NOT NULL,
  INDEX (SiteName),
  StatusType VARCHAR(16) NOT NULL DEFAULT '',
  INDEX (StatusType),
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
  FOREIGN KEY (SiteName) REFERENCES Site(SiteName),
  UNIQUE KEY(SiteName,StatusType),
  PRIMARY KEY (SiteScheduledStatusID)
) Engine = InnoDB;

DROP TABLE IF EXISTS SiteHistory;
CREATE TABLE SiteHistory(
  SiteHistoryID INT UNSIGNED NOT NULL AUTO_INCREMENT,
  SiteName VARCHAR(64) NOT NULL,
  INDEX (SiteName),
  StatusType VARCHAR(16) NOT NULL DEFAULT '',
  INDEX (StatusType),
  Status VARCHAR(8) NOT NULL,
  Reason VARCHAR(255) NOT NULL DEFAULT 'Unspecified',
  DateCreated DATETIME NOT NULL,
  DateEffective DATETIME NOT NULL,
  DateEnd DATETIME NOT NULL,
  INDEX (DateEnd),
  TokenOwner VARCHAR(64) NOT NULL DEFAULT 'RS_SVC',
  FOREIGN KEY (SiteName) REFERENCES Site(SiteName),
  UNIQUE KEY(SiteName,StatusType,DateEnd),
  PRIMARY KEY(SiteHistoryID)
) Engine = InnoDB;

DROP VIEW IF EXISTS PresentSite;
CREATE VIEW PresentSite AS SELECT 
  Site.SiteName, 
  Site.SiteType,
  Site.GridSiteName,
  GridSite.GridTier,
  SiteStatus.StatusType,
  SiteStatus.Status,
  SiteStatus.DateEffective, 
  SiteStatus.Reason,
  SiteStatus.LastCheckTime,
  SiteStatus.TokenOwner,
  SiteStatus.TokenExpiration,
  SiteHistory.Status AS FormerStatus
FROM ( 
  (
    ( Site 
        INNER JOIN GridSite ON 
          Site.GridSiteName = GridSite.GridSiteName
    )
    INNER JOIN SiteHistory ON 
      Site.SiteName = SiteHistory.SiteName
  )
  INNER JOIN SiteStatus ON
    Site.SiteName = SiteStatus.SiteName AND
    SiteHistory.DateEnd = SiteStatus.DateEffective AND
    SiteHistory.StatusType = SiteStatus.StatusType 
) WHERE SiteStatus.DateEffective < UTC_TIMESTAMP()
ORDER BY SiteName, DateEffective;

--
-- SERVICES TABLES
--

DROP TABLE IF EXISTS Service;
CREATE TABLE Service(
  ServiceName VARCHAR(64) NOT NULL,
  ServiceType VARCHAR(32) NOT NULL,
  INDEX (ServiceType),
  SiteName VARCHAR(64) NOT NULL,
  INDEX (SiteName),
  MTime DATETIME NOT NULL,
  PRIMARY KEY(ServiceName)
) Engine=InnoDB;

DROP TABLE IF EXISTS ServiceStatus;
CREATE TABLE ServiceStatus(
  ServiceStatusID INT UNSIGNED NOT NULL AUTO_INCREMENT,
  ServiceName VARCHAR(64) NOT NULL,
  INDEX (ServiceName),
  StatusType VARCHAR(16) NOT NULL DEFAULT '',
  INDEX (StatusType),
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
  FOREIGN KEY (ServiceName) REFERENCES Service(ServiceName),
  UNIQUE KEY (ServiceName,StatusType),
  PRIMARY KEY (ServiceStatusID)
) Engine = InnoDB;

DROP TABLE IF EXISTS ServiceScheduledStatus;
CREATE TABLE ServiceScheduledStatus(
  ServiceScheduledStatusID INT UNSIGNED NOT NULL AUTO_INCREMENT,
  ServiceName VARCHAR(64) NOT NULL,
  INDEX (ServiceName),
  StatusType VARCHAR(16) NOT NULL DEFAULT '',
  INDEX (StatusType),
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
  FOREIGN KEY (ServiceName) REFERENCES Service(ServiceName),
  UNIQUE KEY (ServiceName,StatusType),
  PRIMARY KEY (ServiceScheduledStatusID)
) Engine = InnoDB;

DROP TABLE IF EXISTS ServiceHistory;
CREATE TABLE ServiceHistory(
  ServiceHistoryID INT UNSIGNED NOT NULL AUTO_INCREMENT,
  ServiceName VARCHAR(64) NOT NULL,
  INDEX (ServiceName),
  StatusType VARCHAR(16) NOT NULL DEFAULT '',
  INDEX (StatusType),
  Status VARCHAR(8) NOT NULL,
  Reason VARCHAR(255) NOT NULL DEFAULT 'Unspecified',
  DateCreated DATETIME NOT NULL,
  DateEffective DATETIME NOT NULL,
  DateEnd DATETIME NOT NULL,
  INDEX (DateEnd),
  TokenOwner VARCHAR(64) NOT NULL DEFAULT 'RS_SVC',
  FOREIGN KEY (ServiceName) REFERENCES Service(ServiceName),
  UNIQUE KEY(ServiceName,StatusType,DateEnd),
  PRIMARY KEY(ServiceHistoryID)
) Engine=InnoDB;

DROP VIEW IF EXISTS PresentService;
CREATE VIEW PresentService AS SELECT 
  Service.ServiceName,
  Service.SiteName, 
  Site.SiteType,
  Service.ServiceType, 
  ServiceStatus.StatusType,
  ServiceStatus.Status,
  ServiceStatus.DateEffective, 
  ServiceStatus.Reason,
  ServiceStatus.LastCheckTime,
  ServiceStatus.TokenOwner, 
  ServiceStatus.TokenExpiration,
  ServiceHistory.Status AS FormerStatus
  FROM ( 
  (
    ( Service 
        INNER JOIN Site ON 
          Service.SiteName = Site.SiteName
    )
    INNER JOIN ServiceHistory ON 
      Service.ServiceName = ServiceHistory.ServiceName
  )
  INNER JOIN ServiceStatus ON
    Service.ServiceName = ServiceStatus.ServiceName AND
    ServiceHistory.DateEnd = ServiceStatus.DateEffective AND
    ServiceHistory.StatusType = ServiceStatus.StatusType 
) WHERE ServiceStatus.DateEffective < UTC_TIMESTAMP()
ORDER BY ServiceName, DateEffective;

--
-- RESOURCES TABLES
--

DROP TABLE IF EXISTS Resource;
CREATE TABLE Resource(
  ResourceName VARCHAR(64) NOT NULL,
  ResourceType VARCHAR(8) NOT NULL,
  ServiceType VARCHAR(32) NOT NULL,
  INDEX (ServiceType),
  SiteName VARCHAR(64) NOT NULL,
  INDEX (SiteName),
  GridSiteName VARCHAR(64) NOT NULL,
  INDEX (GridSiteName),
  MTime DATETIME NOT NULL,
  PRIMARY KEY (ResourceName)
) Engine = InnoDB;

DROP TABLE IF EXISTS ResourceStatus;
CREATE TABLE ResourceStatus(
  ResourceStatusID INT UNSIGNED NOT NULL AUTO_INCREMENT,
  ResourceName VARCHAR(64) NOT NULL,
  INDEX (ResourceName),
  StatusType VARCHAR(16) NOT NULL DEFAULT '',
  INDEX (StatusType),
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
  FOREIGN KEY (ResourceName) REFERENCES Resource(ResourceName),
  UNIQUE KEY (ResourceName,StatusType),
  PRIMARY KEY (ResourceStatusID)
) Engine = InnoDB;

DROP TABLE IF EXISTS ResourceScheduledStatus;
CREATE TABLE ResourceScheduledStatus(
  ResourceScheduledStatusID INT UNSIGNED NOT NULL AUTO_INCREMENT,
  ResourceName VARCHAR(64) NOT NULL,
  INDEX (ResourceName),
  StatusType VARCHAR(16) NOT NULL DEFAULT '',
  INDEX (StatusType),
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
  FOREIGN KEY (ResourceName) REFERENCES Resource(ResourceName),
  UNIQUE KEY (ResourceName,StatusType),
  PRIMARY KEY (ResourceScheduledStatusID)
) Engine = InnoDB;

DROP TABLE IF EXISTS ResourceHistory;
CREATE TABLE ResourceHistory(
  ResourceHistoryID INT UNSIGNED NOT NULL AUTO_INCREMENT,
  ResourceName VARCHAR(64) NOT NULL,
  INDEX (ResourceName),
  StatusType VARCHAR(16) NOT NULL DEFAULT '',
  INDEX (StatusType),
  Status VARCHAR(8) NOT NULL,
  Reason VARCHAR(255) NOT NULL DEFAULT 'Unspecified',
  DateCreated DATETIME NOT NULL,
  DateEffective DATETIME NOT NULL,
  DateEnd DATETIME NOT NULL,
  INDEX (DateEnd),
  TokenOwner VARCHAR(64) NOT NULL DEFAULT 'RS_SVC',
  FOREIGN KEY (ResourceName) REFERENCES Resource(ResourceName),
  UNIQUE KEY(ResourceName,StatusType,DateEnd),
  PRIMARY KEY (ResourceHistoryID)
) Engine=InnoDB;

DROP VIEW IF EXISTS PresentResource;
CREATE VIEW PresentResources AS SELECT 
  Resource.ResourceName, 
  Resource.SiteName, 
  Resource.ServiceType,
  Resource.GridSiteName, 
  GridSite.GridTier AS SiteType, 
  Resource.ResourceType,
  ResourceStatus.StatusType,
  ResourceStatus.Status,
  ResourceStatus.DateEffective, 
  ResourceStatus.Reason,
  ResourceStatus.LastCheckTime,
  ResourceStatus.TokenOwner, 
  ResourceStatus.TokenExpiration,
  ResourceHistory.Status AS FormerStatus
FROM (
  (
    ( Resource 
        INNER JOIN GridSite ON 
          Resource.GridSiteName = GridSite.GridSiteName
    )
    INNER JOIN ResourceHistory ON 
      Resource.ResourceName = ResourceHistory.ResourceName
  )
  INNER JOIN ResourceStatus ON
    Resource.ResourceName = ResourceStatus.ResourceName AND
    ResourceHistory.DateEnd = ResourceStatus.DateEffective AND
    ResourceHistory.StatusType = ResourceStatus.StatusType 
) WHERE ResourceStatus.DateEffective < UTC_TIMESTAMP()
ORDER BY ResourceName, DateEffective;

--
-- STORAGE ELEMENTS TABLES
--

DROP TABLE IF EXISTS StorageElement;
CREATE TABLE StorageElement(
  StorageElementName VARCHAR(64) NOT NULL,
  ResourceName VARCHAR(64) NOT NULL,
  INDEX (ResourceName),
  GridSiteName VARCHAR(64) NOT NULL,
  INDEX (GridSiteName),
  MTime DATETIME NOT NULL,
  PRIMARY KEY (StorageElementName)
) Engine = InnoDB;

DROP TABLE IF EXISTS StorageElementStatus;
CREATE TABLE StorageElementStatus(
  StorageElementStatusID INT UNSIGNED NOT NULL AUTO_INCREMENT,
  StorageElementName VARCHAR(64) NOT NULL,
  INDEX (StorageElementName),
  StatusType VARCHAR(16) NOT NULL DEFAULT '',
  INDEX (StatusType),
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
  FOREIGN KEY (StorageElementName) REFERENCES StorageElement(StorageElementName),
  UNIQUE KEY (StorageElementName,StatusType),
  PRIMARY KEY (StorageElementStatusID)
) Engine = InnoDB;

DROP TABLE IF EXISTS StorageElementScheduledStatus;
CREATE TABLE StorageElementScheduledStatus(
  StorageElementScheduledStatusID INT UNSIGNED NOT NULL AUTO_INCREMENT,
  StorageElementName VARCHAR(64) NOT NULL,
  INDEX (StorageElementName),
  StatusType VARCHAR(16) NOT NULL DEFAULT '',
  INDEX (StatusType),
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
  FOREIGN KEY (StorageElementName) REFERENCES StorageElement(StorageElementName),
  UNIQUE KEY (StorageElementName,StatusType),
  PRIMARY KEY (StorageElementScheduledStatusID)
) Engine = InnoDB;

DROP TABLE IF EXISTS StorageElementHistory;
CREATE TABLE StorageElementHistory(
  StorageElementHistoryID INT UNSIGNED NOT NULL AUTO_INCREMENT,
  StorageElementName VARCHAR(64) NOT NULL,
  INDEX (StorageElementName),
  StatusType VARCHAR(16) NOT NULL DEFAULT '',
  INDEX (StatusType),
  Status VARCHAR(8) NOT NULL,
  Reason VARCHAR(255) NOT NULL DEFAULT 'Unspecified',
  DateCreated DATETIME NOT NULL,
  DateEffective DATETIME NOT NULL,
  DateEnd DATETIME NOT NULL,
  INDEX (DateEnd),
  TokenOwner VARCHAR(64) NOT NULL DEFAULT 'RS_SVC',
  FOREIGN KEY (StorageElementName) REFERENCES StorageElement(StorageElementName),
  UNIQUE KEY(StorageElementName,StatusType,DateEnd),
  PRIMARY KEY (StorageElementHistoryID)
) Engine=InnoDB;

DROP VIEW IF EXISTS PresentStorageElement;
CREATE VIEW PresentStorageElement AS SELECT 
  StorageElement.StorageElementName, 
  StorageElement.ResourceName,
  StorageElement.GridSiteName, 
  GridSite.GridTier AS SiteType,
  StorageElementStatus.StatusType,
  StorageElementStatus.Status,
  StorageElementStatus.DateEffective, 
  StorageElementStatus.Reason,
  StorageElementStatus.LastCheckTime,
  StorageElementStatus.TokenOwner,
  StorageElementStatus.TokenExpiration,
  StorageElementHistory.Status AS FormerStatus
FROM ( 
  (
    ( StorageElement 
        INNER JOIN GridSite ON 
          StorageElement.GridSiteName = GridSite.GridSiteName
    )
    INNER JOIN StorageElementHistory ON 
      StorageElement.StorageElementName = StorageElementHistory.StorageElementName
  )
  INNER JOIN StorageElementStatus ON
    StorageElement.StorageElementName = StorageElementStatus.StorageElementName AND
    StorageElementHistory.DateEnd = StorageElementStatus.DateEffective AND
    StorageElementHistory.StatusType = StorageElementStatus.StatusType 
) WHERE StorageElementStatus.DateEffective < UTC_TIMESTAMP()
ORDER BY StorageElementName, DateEffective;