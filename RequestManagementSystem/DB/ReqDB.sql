use mysql;
--
-- Must set passwords for database user by replacing "must_be_set".
--


GRANT SELECT,INSERT,LOCK TABLES,CREATE TEMPORARY TABLES,UPDATE,DELETE,CREATE,DROP,ALTER ON ReqDB TO Dirac@'%' IDENTIFIED BY  'to_be_changed';
GRANT INDEX ON ReqDB.* TO  Dirac@'%' IDENTIFIED BY 'to_be_changed';

FLUSH PRIVILEGES;

USE ReqDB;
