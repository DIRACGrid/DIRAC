#!/bin/bash -x

DESTDIR=$1
if [ -z "$DESTDIR" ]
then
  DESTDIR=/opt/dirac
fi  

source $DESTDIR/bashrc

# Fix mysql.server to make it point to the actual db directory
#
sed -i "s:^datadir=.*:datadir=${DESTDIR}/mysql/db:" ${DESTDIR}/pro/mysql/share/mysql/mysql.server

for dir in $DESTDIR/mysql/db $DESTDIR/mysql/log ; do
  [ -e $dir ] && echo "Existing directory $dir" && echo "Skip MySQL installation" && exit
done

if [ -z "$MYSQL_ROOT_PWD" ] ; then
  echo -n 'Enter MySQL root password:'
  read -t 30 -s passwd || exit 1
  echo
  export MYSQL_ROOT_PWD="$passwd"
fi  

if [ -z "$MYSQL_DIRAC_PWD" ] ; then
  echo -n 'Enter Dirac password:'
  read -t 30 -s diracpwd || exit 1
  echo
  export MYSQL_DIRAC_PWD="$diracpwd"
fi

mkdir -p $DESTDIR/mysql/db
mkdir -p $DESTDIR/mysql/log

mycnf=$DESTDIR/pro/mysql/etc/my.cnf
sed -i 's/\[mysqld\]/\[mysqld\]\ninnodb_file_per_table/g' $mycnf
sed -i 's/innodb_log_arch_dir.*$//' $mycnf
sed -i "s:/opt/dirac:$DESTDIR:" $mycnf

mysql_install_db --datadir=$DESTDIR/mysql/db/ 2>&1 > $DESTDIR/mysql/log/mysql_install_db.log

#
# Set the root password
echo Setting the root password ...
$DESTDIR/pro/mysql/share/mysql/mysql.server start
mysqladmin -u root password "$MYSQL_ROOT_PWD"
mysqladmin -u root -h $1 password "$MYSQL_ROOT_PWD"
mysqladmin -u root -p$MYSQL_ROOT_PWD flush-privileges
