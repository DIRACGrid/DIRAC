cd ~
# hosts config
cp /etc/hosts /resources/hosts
line1=$(sed '1!d' /etc/hosts)
line2=$(sed '2!d' /etc/hosts)
hostname=$(hostname)
req_line1="$line1 localhost.localdomain localhost4 localhost4.localdomain4 $hostname"
req_line2="$line2 localhost.localdomain localhost6 localhost6.localdomain6 $hostname"
sed -i "s|${line1}|${req_line1}|g" /resources/hosts
sed -i "s|${line2}|${req_line2}|g" /resources/hosts
cp /resources/hosts /etc/hosts
yum localinstall -y http://diracproject.web.cern.ch/diracproject/rpm/runit-2.1.2-1.el6.x86_64.rpm
sleep 5s
yum install nano wget -y && status runsvdir
yes | cp /resources/runsvdir.conf /etc/init/runsvdir.conf
mkdir -p /opt/dirac/sbin
yes | cp /resources/runsvdir-start /opt/dirac/sbin/runsvdir-start
chmod +x /opt/dirac/sbin/runsvdir-start
restart runsvdir
yes | cp /resources/setupCA.sh ~/
chmod +x ~/setupCA.sh
./setupCA.sh
cd ~
yes | cp /resources/install.cfg ~/
mkdir -p ~/DiracInstallation && cd ~/DiracInstallation
curl -O -L https://github.com/DIRACGrid/DIRAC/raw/integration/Core/scripts/install_site.sh
chmod +x install_site.sh
cp ../install.cfg .
chmod +x /resources/setupDIRACClient.sh && cp /resources/setupDIRACClient.sh ~/DiracInstallation/
