#!/bin/bash
##################################################################################
#
# Script to install a DIRAC database to be used as part of the SystemAdministrator
# service
#
##################################################################################

DESTDIR=`mysqladmin variables | grep datadir | cut -d '|' -f 3 | sed 's/ //g' | sed 's:/mysql/db/::'` 

for dir in ${DESTDIR}/mysql/db ${DESTDIR}/mysql/log ; do
  [ ! -e $dir ] && echo "MySQL is not installed" && echo "Skip DB installation" && exit
done

[ -z "$MYSQL_ROOT_PWD" ] && echo "No root password defined" && exit 1
[ -z "$MYSQL_DIRAC_PWD" ] && echo "No Dirac password defined" && exit 1

cd ${DESTDIR}/pro

while [ $1 ]; do

  DB=$1
  shift
  file=`ls */*/DB/${DB}.sql`
  [ -z "$file" ] && echo "DB $DB not found" && continue
  echo $DB
  grep -qi "use $DB;" $file || echo ERROR $file
  grep -qi "use $DB;" $file || continue
  
  mysqladmin -u root -p$MYSQL_ROOT_PWD create $DB || continue
  
  echo "GRANT SELECT,INSERT,LOCK TABLES,UPDATE,DELETE,CREATE,DROP,ALTER ON $DB.* TO Dirac@localhost IDENTIFIED BY '"$MYSQL_DIRAC_PWD"'" | mysql -u root -p$MYSQL_ROOT_PWD
  echo "GRANT SELECT,INSERT,LOCK TABLES,UPDATE,DELETE,CREATE,DROP,ALTER ON $DB.* TO Dirac@$HOSTNAME IDENTIFIED BY '"$MYSQL_DIRAC_PWD"'" | mysql -u root -p$MYSQL_ROOT_PWD
  echo "GRANT SELECT,INSERT,LOCK TABLES,UPDATE,DELETE,CREATE,DROP,ALTER ON $DB.* TO Dirac@'%' IDENTIFIED BY '"$MYSQL_DIRAC_PWD"'" | mysql -u root -p$MYSQL_ROOT_PWD
  export DB
  awk 'BEGIN { N = 0 }
         { if ( tolower($0) ~ tolower("use "ENVIRON["DB"]";") ) N=1;
           if ( N == 1 ) print }' $file | mysql -u root -p$MYSQL_ROOT_PWD $DB

done

[ -z "$MYSQL_ROOT_PWD" ] || mysqladmin -u root -p$MYSQL_ROOT_PWD flush-privileges