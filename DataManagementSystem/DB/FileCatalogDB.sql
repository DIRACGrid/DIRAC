DROP DATABASE IF EXISTS FileCatalogDB;
-- ------------------------------------------------------------------------------
CREATE DATABASE FileCatalogDB;

--
-- Must set passwords for database user by replacing "must_be_set".
--
GRANT SELECT,INSERT,LOCK TABLES,CREATE TEMPORARY TABLES,UPDATE,DELETE,CREATE,DROP,ALTER ON FileCatalogDB.* TO Dirac@localhost IDENTIFIED BY 'must_be_set';
FLUSH PRIVILEGES;

USE FileCatalogDB;

