DROP DATABASE IF EXISTS StorageUsageDB;

CREATE DATABASE StorageUsageDB;

use mysql;
GRANT SELECT,INSERT,LOCK TABLES,UPDATE,DELETE,CREATE,DROP,ALTER ON StorageUsageDB.* TO 'Dirac'@'localhost' IDENTIFIED BY '';
GRANT SELECT,INSERT,LOCK TABLES,UPDATE,DELETE,CREATE,DROP,ALTER ON StorageUsageDB.* TO 'Dirac'@'%' IDENTIFIED BY '';

-------------------------------------------------------------

use StorageUsageDB;

DROP TABLE IF EXISTS Directory;
CREATE TABLE Directory(
  Directory VARCHAR(255) NOT NULL,
  DirectoryID INTEGER NOT NULL AUTO_INCREMENT,
  DirectoryFiles INTEGER NOT NULL,
  DirectorySize BIGINT NOT NULL,
  PRIMARY KEY(Directory,DirectoryID)
);

DROP TABLE IF EXISTS DirectoryUsage;
CREATE TABLE DirectoryUsage(
   DirectoryID INTEGER NOT NULL,
   StorageElement VARCHAR(32) NOT NULL,
   StorageElementSize BIGINT NOT NULL,
   StorageElementFiles INTEGER NOT NULL,
   Updated DATETIME NOT NULL,
   PRIMARY KEY (DirectoryID,StorageElement)
);

DROP TABLE IF EXISTS DirectoryParameters;
CREATE TABLE DirectoryParameters(
  DirectoryID INTEGER NOT NULL,
  Parameter VARCHAR(255) NOT NULL,
  PRIMARY KEY (DirectoryID,Parameter)
);
