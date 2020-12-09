#!/bin/bash

set -euv

# this doesn't work on cern SL6 machines in openstack, there we still have `hostname`
# Sed the /etc/hosts file to append dirac-tuto
sed -i "s/\(127.*\)/\1 dirac-tuto/" /etc/hosts
sed -i "s/\(::1.*\)/\1 dirac-tuto/" /etc/hosts

# on cern SL6 image on openstack just use HOSTNAME
HOSTNAME=`hostname`


# avoid calling passwd, which might be the cern passwd in /usr/sue/bin
# START add_dirac
adduser -s /bin/bash -d /home/dirac dirac || echo "User dirac already exists."
echo password | /usr/bin/passwd --stdin dirac
mkdir -p /opt/dirac/sbin
chown -R dirac:dirac /opt/dirac/
# END add_dirac


#localinstall does not error when rpm is already installed
# START runit
yum localinstall -y http://diracproject.web.cern.ch/diracproject/rpm/runit-2.1.2-1.el7.cern.x86_64.rpm
# END runit

cat > /opt/dirac/sbin/runsvdir-start <<'EOF'
# START runsvdir-start
#!/bin/bash
cd /opt/dirac
RUNSVCTRL='/sbin/runsvctrl'
chpst -u dirac $RUNSVCTRL d /opt/dirac/startup/*
killall runsv svlogd
RUNSVDIR='/sbin/runsvdir'
exec chpst -u dirac $RUNSVDIR -P /opt/dirac/startup 'log:  DIRAC runsv'
# END runsvdir-start
EOF

# runsvdir-start can fail to start/restart if it does not contain the shebang at the top of the file
# we remove the first line of the script
sed -i '1d' /opt/dirac/sbin/runsvdir-start

cat > /lib/systemd/system/runsvdir-start.service <<EOF
# START systemd-runsvdir
[Unit]
Description=Runit Process Supervisor

[Service]
ExecStart=/opt/dirac/sbin/runsvdir-start
Restart=always
KillMode=process

[Install]
WantedBy=multi-user.target
# END systemd-runsvdir
EOF


# START restartrunsv
chown dirac:dirac /opt/dirac/sbin/runsvdir-start
chmod +x /opt/dirac/sbin/runsvdir-start
systemctl daemon-reload
systemctl start runsvdir-start
# END restartrunsv

## SETUP FOR MYSQL
# remove mysql
# START mysqlInstall
yum remove -y $(rpm -qa | grep -i -e mysql -e mariadb | paste -sd ' ') || echo  "MySQL is not yet installed"
rm -rf /var/lib/mysql/*
yum install -y \
    https://dev.mysql.com/get/Downloads/MySQL-5.7/mysql-community-devel-5.7.25-1.el7.x86_64.rpm \
    https://dev.mysql.com/get/Downloads/MySQL-5.7/mysql-community-server-5.7.25-1.el7.x86_64.rpm \
    https://dev.mysql.com/get/Downloads/MySQL-5.7/mysql-community-client-5.7.25-1.el7.x86_64.rpm \
    https://dev.mysql.com/get/Downloads/MySQL-5.7/mysql-community-libs-5.7.25-1.el7.x86_64.rpm \
    https://dev.mysql.com/get/Downloads/MySQL-5.7/mysql-community-common-5.7.25-1.el7.x86_64.rpm
# END mysqlInstall

# START mysqlStart
systemctl start mysqld || systemctl restart mysqld
# END mysqlStart

cat > mysqlSetup.sql <<EOF
# START mysqlSetup
ALTER USER 'root'@'localhost' IDENTIFIED BY 'MyStrongPass@4';
FLUSH PRIVILEGES;
uninstall plugin validate_password;
ALTER USER 'root'@'localhost' IDENTIFIED BY 'password';
FLUSH PRIVILEGES;
quit
# END mysqlSetup
EOF

# START mysqlInit
MY_PW=`grep "temporary password" /var/log/mysqld.log | tail -n1 | sed "s/.* //"`
mysql -u root --password=${MY_PW} --connect-expired-password < mysqlSetup.sql
# END mysqlInit

## SETUP FOR DIRAC

cat > install.cfg <<EOF
# START install.cfg
LocalInstallation
{
  #  DIRAC release version to install
  Release = v7r0p36
  #  Installation type
  InstallType = server
  #  Each DIRAC update will be installed in a separate directory, not overriding the previous ones
  UseVersionsDir = yes
  #  The directory of the DIRAC software installation
  TargetPath = /opt/dirac
  #  Install the WebApp extension
  Extensions = WebApp

  # Name of the VO we will use
  VirtualOrganization = tutoVO
  # Name of the site or host
  SiteName = dirac-tuto
  # Setup name
  Setup = MyDIRAC-Production
  #  Default name of system instances
  InstanceName = Production
  #  Flag to skip download of CAs
  SkipCADownload = yes
  #  Flag to use the server certificates
  UseServerCertificate = yes

  # Name of the Admin user (from the user certificate we created )
  AdminUserName = ciuser
  # DN of the Admin user certificate (from the user certificate we created)
  AdminUserDN = /C=ch/O=DIRAC/OU=DIRAC CI/CN=ciuser/emailAddress=lhcb-dirac-ci@cern.ch
  AdminUserEmail= adminUser@cern.ch
  # Name of the Admin group
  AdminGroupName = dirac_admin

  # DN of the host certificate (from the host certificate we created)
  HostDN = /C=ch/O=DIRAC/OU=DIRAC CI/CN=dirac-tuto/emailAddress=lhcb-dirac-ci@cern.ch
  # Define the Configuration Server as Master
  ConfigurationMaster = yes

  # List of DataBases to be installed (what's here is a list for a basic installation)
  Databases = InstalledComponentsDB
  Databases += ResourceStatusDB

  #  List of Services to be installed (what's here is a list for a basic installation)
  Services  = Configuration/Server
  Services += Framework/ComponentMonitoring
  Services += Framework/SystemAdministrator
  #  Flag determining whether the Web Portal will be installed
  WebPortal = yes
  WebApp = yes

  Database
  {
    #  User name used to connect the DB server
    User = Dirac
    #  Password for database user access
    Password = Dirac
    #  Password for root DB user
    RootPwd = password
    #  location of DB server
    Host = localhost
  }
}
# END install.cfg
EOF

sed -i "s/dirac-tuto/${HOSTNAME}/"g install.cfg

cat > setupCA <<EOF
#!/bin/bash
# START setupCA
mkdir -p ~/caUtilities/ && cd ~/caUtilities/
curl -O -L https://raw.githubusercontent.com/DIRACGrid/DIRAC/integration/tests/Jenkins/utilities.sh
curl -O -L https://raw.githubusercontent.com/DIRACGrid/DIRAC/integration/tests/Jenkins/config/ci/openssl_config_ca.cnf
curl -O -L https://raw.githubusercontent.com/DIRACGrid/DIRAC/integration/tests/Jenkins/config/ci/openssl_config_host.cnf
curl -O -L https://raw.githubusercontent.com/DIRACGrid/DIRAC/integration/tests/Jenkins/config/ci/openssl_config_user.cnf
export SERVERINSTALLDIR=/opt/dirac
export CI_CONFIG=~/caUtilities/
source utilities.sh
generateCA
generateCertificates 365
generateUserCredentials 365
# END setupCA
EOF

cat > installDirac <<EOF
#!/bin/bash
# START installDirac
mkdir -p ~/DiracInstallation && cd ~/DiracInstallation
curl -O -L https://github.com/DIRACGrid/DIRAC/raw/integration/Core/scripts/install_site.sh
chmod +x install_site.sh
cp ../install.cfg .
./install_site.sh --dirac-os install.cfg
# END installDirac
EOF

chmod +x setupCA
chmod +x installDirac

cp setupCA installDirac /home/dirac/
cp install.cfg /home/dirac/

chown dirac:dirac /home/dirac/installDirac
chown dirac:dirac /home/dirac/setupCA

sudo -u dirac /home/dirac/setupCA
sudo -u dirac /home/dirac/installDirac

# Installing the client

# add user to be used as a user

# START user_diracuser
adduser -s /bin/bash -d /home/diracuser diracuser || echo "User diracuser already exists."
echo password | /usr/bin/passwd --stdin diracuser
mkdir -p ~diracuser/.globus/
cp /opt/dirac/user/client.pem ~diracuser/.globus/usercert.pem
cp /opt/dirac/user/client.key ~diracuser/.globus/userkey.pem
chown -R diracuser:diracuser ~diracuser/.globus/
# END user_diracuser

cat > InstallDiracClient <<EOF
# START installClient1
mkdir -p ~/DiracInstallation && cd ~/DiracInstallation
curl -O -L https://github.com/DIRACGrid/DIRAC/raw/integration/src/DIRAC/Core/scripts/dirac-install.py
chmod +x dirac-install.py
./dirac-install.py -r v7r0p36 --dirac-os
# END installClient1
# START installClient2
mkdir -p ~/DiracInstallation/etc/grid-security/
ln -fs /opt/dirac/etc/grid-security/certificates/ ~/DiracInstallation/etc/grid-security/certificates
# END installClient2
EOF


cp InstallDiracClient /home/diracuser
chown -R diracuser:diracuser /home/diracuser/InstallDiracClient
chmod +x /home/diracuser/InstallDiracClient
sudo -u diracuser /home/diracuser/InstallDiracClient


mkdir -p ~diracuser/DiracInstallation/etc
cat > ~diracuser/DiracInstallation/etc/dirac.cfg <<EOF
# START dirac.cfg
DIRAC
{
  Setup = MyDIRAC-Production
  Configuration
  {
    Servers = dips://dirac-tuto:9135/Configuration/Server
  }
}
# END dirac.cfg
EOF

chown diracuser:diracuser  -R ~diracuser/DiracInstallation/etc/
