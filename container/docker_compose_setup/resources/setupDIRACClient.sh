mkdir ~diracuser/DiracInstallation && cd ~diracuser/DiracInstallation
wget -O dirac-install.py https://github.com/DIRACGrid/DIRAC/raw/integration/Core/scripts/dirac-install.py
chmod +x dirac-install.py
./dirac-install.py -r v6r21p5
mkdir -p ~diracuser/DiracInstallation/etc/grid-security/
ln -s /opt/dirac/etc/grid-security/certificates/ ~diracuser/DiracInstallation/etc/grid-security/certificates
cp /resources/dirac.cfg ~diracuser/DiracInstallation/etc/dirac.cfg
source ~diracuser/DiracInstallation/bashrc
source ~diracuser/DiracInstallation/bashrc
dirac-proxy-init
cd ~diracuser/.globus/
openssl pkcs12 -export -out certificate.p12 -inkey userkey.pem -in usercert.pem
