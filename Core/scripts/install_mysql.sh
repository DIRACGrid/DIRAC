#!/bin/bash

[ -z "$DIRACINSTANCE" ] && DIRACINSTANCE=Development

source /opt/dirac/bashrc

for dir in /opt/dirac/mysql/db /opt/dirac/mysql/log ; do
  [ -e $dir ] && echo "Existing directory $dir" && echo "Skip MySQL installation" && exit
done

echo -n 'Enter root password:'
read -t 30 -s passwd || exit 1
echo

echo -n 'Enter Dirac password:'
read -t 30 -s diracpwd || exit 1
echo

mkdir -p /opt/dirac/mysql/db
mkdir -p /opt/dirac/mysql/log

mycnf=/opt/dirac/pro/mysql/etc/my.cnf
sed -i 's/\[mysqld\]/\[mysqld\]\ninnodb_file_per_table/g' $mycnf
sed -i 's/innodb_log_arch_dir.*$//' $mycnf

mysql_install_db --datadir=/opt/dirac/mysql/db/ 2>&1 > /opt/dirac/mysql/log/mysql_install_db.log

/opt/dirac/pro/mysql/share/mysql/mysql.server start

cd /opt/dirac/pro

for file in ` find DIRAC/*/DB -name "*DB.sql" ` ; do
		DB=`basename $file .sql`
        echo $DB
        grep -qi "use $DB;" $file || echo ERROR $file
        grep -qi "use $DB;" $file || continue
        mysqladmin -u root create $DB
        echo "GRANT SELECT,INSERT,LOCK TABLES,UPDATE,DELETE,CREATE,DROP,ALTER ON $DB.* TO Dirac@localhost IDENTIFIED BY '"$diracpwd"'" | mysql -u root
        echo "GRANT SELECT,INSERT,LOCK TABLES,UPDATE,DELETE,CREATE,DROP,ALTER ON $DB.* TO Dirac@'%' IDENTIFIED BY '"$diracpwd"'" | mysql -u root
        export DB
        awk 'BEGIN { N = 0 }
             { if ( tolower($0) ~ tolower("use "ENVIRON["DB"]";") ) N=1;
               if ( N == 1 ) print }' $file | mysql -u root $DB

SYSTEM=`dirname $file` ; SYSTEM=`dirname $SYSTEM` ;SYSTEM=`basename $SYSTEM System`
cat << EOF > /tmp/$DB.cfg
Systems
{
  $SYSTEM
  {
    $DIRACINSTANCE
    {
      Databases
      {
        $DB
        {
          User = Dirac
          Host = localhost
          Password = $diracpwd
          DBName = $DB
        }
      }
    }
  }
}
EOF

python << EOF
from DIRAC.Core.Utilities.CFG import CFG
mainCFG = CFG()
mainCFG.loadFromFile( "etc/dirac.cfg" )

db = CFG()

db.loadFromFile( "/tmp/$DB.cfg" )

open( "etc/dirac.cfg", "w" ).write( str(mainCFG.mergeWith( db )) )

EOF

done

mysqladmin -u root flush-privileges
mysqladmin -u root password "$passwd"
mysqladmin -u root -h $1 password "$passwd"

/opt/dirac/pro/mysql/share/mysql/mysql.server stop
