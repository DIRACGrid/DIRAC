#!/bin/bash

[ -z "$DIRACINSTANCE" ] && DIRACINSTANCE=Development

if [ ! $1 ]; then
  echo
  echo Usage: `basename $0` DBName [DBName] ...
  echo
  exit 1
fi

for dir in /opt/dirac/mysql/db /opt/dirac/mysql/log ; do
  [ ! -e $dir ] && echo "MySQL is not installed" && echo "Skip DB installation" && exit
done


cd /opt/dirac/pro

while [ $1 ]; do

  DB=$1
  shift
  file=`ls */*/DB/${DB}.sql`
  [ -z "$file" ] && echo "DB $DB not found" && continue
  echo $DB
  grep -qi "use $DB;" $file || echo ERROR $file
  grep -qi "use $DB;" $file || continue
  if [ -z "$MYSQL_ROOT_PWD" ] ; then
    echo -n 'Enter MySQL root password:'
    read -t 30 -s passwd || exit 1
    echo
    export MYSQL_ROOT_PWD="$passwd"
  fi

  mysqladmin -u root -p$MYSQL_ROOT_PWD create $DB || continue
  if [ -z "$MYSQL_DIRAC_PWD" ] ; then
    echo -n 'Enter Dirac password:'
    read -t 30 -s diracpwd || exit 1
    echo
    export MYSQL_DIRAC_PWD="$diracpwd"
  fi
  
  echo "GRANT SELECT,INSERT,LOCK TABLES,UPDATE,DELETE,CREATE,DROP,ALTER ON $DB.* TO Dirac@localhost IDENTIFIED BY '"$MYSQL_DIRAC_PWD"'" | mysql -u root -p$MYSQL_ROOT_PWD
  echo "GRANT SELECT,INSERT,LOCK TABLES,UPDATE,DELETE,CREATE,DROP,ALTER ON $DB.* TO Dirac@'%' IDENTIFIED BY '"$MYSQL_DIRAC_PWD"'" | mysql -u root -p$MYSQL_ROOT_PWD
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
          Host = $HOST
          Password = $MYSQL_DIRAC_PWD
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

touch etc/DBs.cfg
python << EOF
from DIRAC.Core.Utilities.CFG import CFG
mainCFG = CFG()
mainCFG.loadFromFile( "etc/DBs.cfg" )
db = CFG()
db.loadFromFile( "/tmp/$DB.cfg" )
open( "etc/DBs.cfg", "w" ).write( str(mainCFG.mergeWith( db )) )
EOF

done

[ -z "$MYSQL_ROOT_PWD" ] || mysqladmin -u root -p$MYSQL_ROOT_PWD flush-privileges
